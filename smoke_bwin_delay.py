"""End-to-end smoke test for the bwin reference delay pipeline.

Runs ONE full cycle minus the duplicate-detection / Telegram bits:

    1.  Scrape Dafabet live tennis listing.
    2.  extract_dafabet_scores() to enrich each entry with sets/games/points.
    3.  Open the persistent bwin page.
    4.  check_bwin_delays() — 1st pass: populates `bwin_delay_pending`,
        no alerts expected (two-cycle debounce).
    5.  Wait ~30s so bwin's WebSocket can push updates.
    6.  check_bwin_delays() — 2nd pass: any lag that persisted fires an alert.
    7.  Print cross-source matches, pending candidates, confirmed alerts.

Nothing is sent to Telegram; alerts are printed only. Safe to run any time.
"""

import asyncio
import os
import sys

# Force UTF-8 on Windows so ↔ / emoji in logs don't crash the console.
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from playwright.async_api import async_playwright

from delay_detector import (
    BWIN_LIVE_URL,
    build_bwin_heartbeat_section,
    check_bwin_delays,
    extract_dafabet_scores,
    fetch_bwin_live,
    match_dafabet_to_bwin,
)
from monitor import TENNIS_URL, extract_matches


async def main() -> None:
    print("═" * 70)
    print("  BWIN REFERENCE DELAY — END-TO-END SMOKE TEST")
    print("═" * 70)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])

        # ── Dafabet context ──
        dafa_ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        dafa_page = await dafa_ctx.new_page()

        # ── bwin context (separate to isolate cookies / geo) ──
        bwin_ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )
        bwin_page = await bwin_ctx.new_page()

        try:
            # ── Step 1: Dafabet listing ──
            print("\n[1] Scraping Dafabet live tennis listing…")
            entries = await extract_matches(dafa_page, TENNIS_URL)
            live_entries = [e for e in entries if not e.get("not_started")]
            print(f"    → {len(entries)} entries ({len(live_entries)} live)")
            for e in live_entries[:15]:
                print(f"      • {e['home']} vs {e['away']}")
            if len(live_entries) > 15:
                print(f"      … and {len(live_entries) - 15} more")
            if not live_entries:
                print("    [!] No live Dafabet matches — aborting smoke test.")
                return

            # ── Step 2: Dafabet scores ──
            print(f"\n[2] Extracting Dafabet scores for {len(live_entries)} match(es)…")
            scored = await extract_dafabet_scores(dafa_ctx, live_entries)

            # ── Step 3: bwin persistent page ──
            print(f"\n[3] Opening persistent bwin page: {BWIN_LIVE_URL}")
            bwin_first = await fetch_bwin_live(bwin_page)
            print(f"    → {len(bwin_first)} live match(es) parsed from bwin")
            for bw in bwin_first[:15]:
                games = bw["game_scores"][-1] if bw["game_scores"] else (0, 0)
                print(f"      • [{bw.get('country1','?')}/{bw.get('country2','?')}] "
                      f"{bw['player1']} vs {bw['player2']}  "
                      f"— Set {bw['current_set']} "
                      f"(Sets {bw['sets_p1']}-{bw['sets_p2']}, "
                      f"Game {games[0]}-{games[1]})")

            # ── Cross-source coverage diagnostic ──
            print(f"\n[4] Cross-source matches (Dafabet ↔ bwin):")
            coverage = 0
            for d in scored:
                bw = match_dafabet_to_bwin(d, bwin_first)
                if bw:
                    coverage += 1
                    print(f"      ✓ {d['home']} vs {d['away']}  "
                          f"→  {bw['player1']} vs {bw['player2']}  "
                          f"[Dafabet set {d.get('current_set', '?')} "
                          f"/ bwin set {bw['current_set']}]")
            print(f"    → {coverage}/{len(scored)} Dafabet matches have a bwin reference")

            # ── Step 5: 1st delay check ──
            print(f"\n[5] First check_bwin_delays pass "
                  f"(candidates land in pending; no alerts expected)…")
            alerted_bwin_delays: set = set()
            bwin_delay_pending: dict = {}
            alerts_1 = await check_bwin_delays(
                bwin_first, scored, alerted_bwin_delays, bwin_delay_pending
            )
            print(f"    → {len(alerts_1)} confirmed alerts, "
                  f"{len(bwin_delay_pending)} pending candidate(s)")
            for url, info in bwin_delay_pending.items():
                d = info["delay"]
                print(f"      ⏳ PENDING {d['type']}: {url}")

            # ── Step 6: wait for bwin WebSocket updates ──
            wait_s = 30
            print(f"\n[6] Waiting {wait_s}s for bwin WebSocket updates "
                  f"(no page reload)…")
            await asyncio.sleep(wait_s)

            # ── Step 7: 2nd delay check ──
            # Re-extract both Dafabet scores AND bwin state for the 2nd pass.
            print(f"\n[7] Re-extracting Dafabet + bwin state and running 2nd pass…")
            scored_2 = await extract_dafabet_scores(dafa_ctx, live_entries)
            bwin_second = await fetch_bwin_live(bwin_page)
            alerts_2 = await check_bwin_delays(
                bwin_second, scored_2, alerted_bwin_delays, bwin_delay_pending
            )

            # Render the heartbeat section the same way monitor.py will.
            print(f"\n[7b] Rendering heartbeat bwin section preview…")
            hb_section = build_bwin_heartbeat_section(
                bwin_second, scored_2, alerted_bwin_delays, bwin_delay_pending
            )
            print("─" * 70)
            print(hb_section.strip())
            print("─" * 70)

            print("\n" + "═" * 70)
            print("  RESULTS")
            print("═" * 70)
            print(f"  Dafabet live matches        : {len(live_entries)}")
            print(f"  bwin live matches           : {len(bwin_first)}")
            print(f"  Cross-source matched pairs  : {coverage}")
            print(f"  1st-pass confirmed alerts   : {len(alerts_1)}")
            print(f"  1st-pass pending candidates : {len(bwin_delay_pending)}")
            print(f"  2nd-pass confirmed alerts   : {len(alerts_2)}")

            if alerts_2:
                print("\n  🔔 CONFIRMED DELAY ALERTS (would be sent to Telegram):")
                for a in alerts_2:
                    print("\n" + "─" * 70)
                    print(a["alert_msg"])
                    print("─" * 70)
            else:
                print("\n  No confirmed delay alerts this run (all sources in sync).")

        finally:
            await bwin_page.close()
            await bwin_ctx.close()
            await dafa_page.close()
            await dafa_ctx.close()
            await browser.close()
            print("\n[*] Cleanup done.")


if __name__ == "__main__":
    asyncio.run(main())
