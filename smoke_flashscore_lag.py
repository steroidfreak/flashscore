"""
Measure how fast/slow flashscore.mobi is vs Dafabet for live tennis scores.

Method
------
For ~BENCH_DURATION seconds, poll BOTH sources every POLL_INTERVAL seconds
and record every (set_games_tuple) observed per matched pair with a
monotonic timestamp.  When the SAME new score first appears on each side,
the difference between the two "first-seen" timestamps is the lag.

    lag_seconds = t_flashscore_first_saw(X) - t_dafabet_first_saw(X)

Positive → flashscore is slower (Dafabet is ahead).
Negative → Dafabet is slower.

No Telegram alerts, no AI, no duplicate detection. Read-only benchmark.
"""

import asyncio
import os
import statistics
import sys
import time
from collections import defaultdict

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright

from delay_detector import (
    extract_dafabet_scores,
    fetch_flashscore_live,
    match_dafabet_to_flashscore,
)
from monitor import TENNIS_URL, extract_matches

POLL_INTERVAL: float = 8.0      # seconds between snapshots
BENCH_DURATION: float = 150.0   # total seconds to run the benchmark


def _total_games(games: list[tuple]) -> int:
    """Sum of all games in all completed+current sets so every transition is unique."""
    return sum((g[0] or 0) + (g[1] or 0) for g in games)


async def snapshot(
    dafa_ctx, fs_page, dafabet_entries_cache: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Fetch one parallel (dafabet_scored, flashscore) snapshot."""
    # Dafabet scoreboards open in dafa_ctx (many tabs briefly); Flashscore is
    # a single LIVE listing page reload.
    dafa_scored, fs_live = await asyncio.gather(
        extract_dafabet_scores(dafa_ctx, dabet_live_entries_ref[0]),
        fetch_flashscore_live(fs_page),
    )
    return dafa_scored, fs_live


# Kept in a list so snapshot() can read the latest live-entry roster
# without us having to thread it through every call.
dabet_live_entries_ref: list[list[dict]] = [[]]


async def main() -> None:
    print("═" * 70)
    print("  FLASHSCORE.MOBI vs DAFABET — LAG BENCHMARK")
    print(f"  Poll interval: {POLL_INTERVAL}s · Duration: {BENCH_DURATION}s")
    print("═" * 70)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])

        dafa_ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        dafa_page = await dafa_ctx.new_page()
        fs_ctx = await browser.new_context(locale="en-US")
        fs_page = await fs_ctx.new_page()

        try:
            print("\n[1] Scraping Dafabet listing (once, for match inventory)…")
            entries = await extract_matches(dafa_page, TENNIS_URL)
            live_entries = [
                e for e in entries
                if not e.get("not_started")
                and "/" not in e.get("home", "")   # drop doubles — flashscore model is singles-oriented
                and "/" not in e.get("away", "")
            ]
            print(f"    → {len(live_entries)} live singles on Dafabet")
            if not live_entries:
                print("    [!] Nothing to benchmark.")
                return
            dabet_live_entries_ref[0] = live_entries

            # Per-match history:  url  →  {"dafa": {score_key: t0}, "fs": {score_key: t0}}
            # score_key = total_games count (monotonically increases across the match)
            history: dict[str, dict[str, dict[int, float]]] = defaultdict(
                lambda: {"dafa": {}, "fs": {}}
            )
            match_labels: dict[str, str] = {}

            start = time.monotonic()
            cycle = 0
            while time.monotonic() - start < BENCH_DURATION:
                cycle += 1
                t = time.monotonic() - start
                print(f"\n[cycle {cycle}  t={t:5.1f}s] snapshotting both sources…")

                try:
                    dafa_scored, fs_live = await asyncio.gather(
                        extract_dafabet_scores(dafa_ctx, live_entries),
                        fetch_flashscore_live(fs_page),
                    )
                except Exception as exc:
                    print(f"    [warn] snapshot failed: {exc}")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                now_dafa = time.monotonic() - start
                now_fs = now_dafa  # fetched in parallel via gather → same effective t

                matched_this_cycle = 0
                for d in dafa_scored:
                    fs = match_dafabet_to_flashscore(d, fs_live)
                    if not fs:
                        continue
                    matched_this_cycle += 1

                    url = d["url"]
                    label = f"{d['home']} vs {d['away']}"
                    match_labels[url] = label

                    dafa_key = _total_games(d.get("game_scores", []))
                    fs_key = _total_games(fs.get("game_scores", []))

                    # Record first observation of each score count on each side
                    if dafa_key not in history[url]["dafa"]:
                        history[url]["dafa"][dafa_key] = now_dafa
                    if fs_key not in history[url]["fs"]:
                        history[url]["fs"][fs_key] = now_fs

                print(f"    matched pairs: {matched_this_cycle} · "
                      f"(Dafabet live {len(dafa_scored)}, flashscore live {len(fs_live)})")

                # Stop a bit before duration so we don't overshoot
                remaining = BENCH_DURATION - (time.monotonic() - start)
                if remaining <= 0:
                    break
                await asyncio.sleep(min(POLL_INTERVAL, remaining))

            # ── Analyze lag per matched score ────────────────────────
            print("\n" + "═" * 70)
            print("  LAG ANALYSIS (per score value seen on both sides)")
            print("═" * 70)
            print(f"  {'Match':<42}  {'score':>7}  {'dafa_t':>7}  "
                  f"{'fs_t':>7}  {'lag':>7}")
            print("  " + "-" * 80)

            all_lags: list[float] = []         # positive → flashscore slower
            per_match_lag_median: dict[str, float] = {}

            for url, d in history.items():
                lags: list[float] = []
                common_keys = sorted(set(d["dafa"]) & set(d["fs"]))
                if not common_keys:
                    continue
                label = match_labels[url][:40]
                for k in common_keys:
                    td = d["dafa"][k]
                    tf = d["fs"][k]
                    lag = tf - td
                    lags.append(lag)
                    all_lags.append(lag)
                    print(f"  {label:<42}  {k:>7}  {td:>6.1f}s  {tf:>6.1f}s  "
                          f"{lag:>+6.1f}s")
                if lags:
                    per_match_lag_median[label] = statistics.median(lags)

            # ── Overall summary ─────────────────────────────────────
            print("\n" + "═" * 70)
            print("  SUMMARY")
            print("═" * 70)

            if not all_lags:
                print("  No score values seen on both sides — benchmark inconclusive.")
                print("  Try a longer BENCH_DURATION or run during higher-activity hours.")
                return

            pos = [x for x in all_lags if x > 0.5]
            neg = [x for x in all_lags if x < -0.5]
            sync = [x for x in all_lags if -0.5 <= x <= 0.5]

            print(f"  Score transitions compared : {len(all_lags)}")
            print(f"  Mean lag (fs - dafa)       : {statistics.mean(all_lags):+.1f}s")
            print(f"  Median lag                 : {statistics.median(all_lags):+.1f}s")
            if len(all_lags) > 1:
                print(f"  Stdev                      : {statistics.stdev(all_lags):.1f}s")
            print(f"  Min / Max                  : {min(all_lags):+.1f}s / {max(all_lags):+.1f}s")
            print()
            print(f"  Flashscore SLOWER (lag > +0.5s) : {len(pos)} transitions "
                  f"(mean {statistics.mean(pos):+.1f}s)" if pos else
                  f"  Flashscore SLOWER (lag > +0.5s) : 0 transitions")
            print(f"  In sync (|lag| ≤ 0.5s)          : {len(sync)} transitions")
            print(f"  Dafabet SLOWER (lag < -0.5s)    : {len(neg)} transitions "
                  f"(mean {statistics.mean(neg):+.1f}s)" if neg else
                  f"  Dafabet SLOWER (lag < -0.5s)    : 0 transitions")

            # Cycle resolution caveat
            print(f"\n  NOTE: resolution is {POLL_INTERVAL:.0f}s (one poll interval); "
                  f"sub-{POLL_INTERVAL:.0f}s differences appear as 0.")

        finally:
            await fs_page.close()
            await fs_ctx.close()
            await dafa_page.close()
            await dafa_ctx.close()
            await browser.close()
            print("\n[*] Cleanup done.")


if __name__ == "__main__":
    asyncio.run(main())
