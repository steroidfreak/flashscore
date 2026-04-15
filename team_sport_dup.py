"""
Team-sport duplicate detection (basketball, volleyball, …)
==========================================================
Shared logic for any Dafabet team-sport listing page that uses the same
DOM as tennis (`bg-th-card-container` cards, `truncate text-th-primary-text`
team divs).  Tennis still has its own player-name model in monitor.py
because singles/doubles names with first initials need different scoring.

Design
------
1. `normalize_team` strips accents, punctuation, and a curated set of
   noise tokens (`fc`, `cd`, `bc`, `vc`, `vk`, `club`, `de`, `los`,
   `volleyball`, `basket`, …) so that "CD Espanol De Talca" and
   "Espanol Talca" collapse to the same token set.
2. `team_similarity` blends Jaccard token overlap (60%) with a fuzzy
   whole-string ratio (40%) on the noise-stripped form, with a floor
   when a distinctive (≥5-char) token is shared on both sides.
3. `team_match_similarity` compares two match entries on both normal
   AND reversed pairings, then applies a "shared-team boost": if one
   side is near-identical (≥STRONG_SIDE) and the other side has at
   least MIN_SIDE overlap, the pair is boosted to 0.80. This catches
   the real-world pattern from the user's screenshots:

       Match A: "Colegio Los Leones"  vs  "CD Espanol De Talca"
       Match B: "Leones de Quilpue"   vs  "CD Espanol De Talca"

   – the away side is identical and the home side shares the
   distinctive nickname "Leones".

`run_team_sport_loop` is a long-lived asyncio task that monitor.py
launches once per team sport, sharing a single browser context with
the tennis loop so everything runs in one process.
"""

import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Awaitable, Callable

from playwright.async_api import Page

# ── Team-name normalization ────────────────────────────────────────

# Tokens that carry no distinguishing information across the team-sport
# leagues we monitor. Stripped during normalization so name variants
# like "BC Partizan" / "Partizan Belgrade" / "Partizan" line up.
NOISE_TOKENS: set[str] = {
    # club prefixes / suffixes
    "fc", "cf", "sc", "ac", "bc", "kk", "sk", "bk", "cd", "cb", "club",
    "bbc", "bbk", "sp", "sport", "sports", "sporting",
    # sport names (basketball / volleyball, multilingual)
    "basket", "basketball", "baloncesto", "basketbol", "ball",
    "volley", "volleyball", "voleibol", "volei", "vc", "vk", "vbc",
    # articles / connectors (en, es, fr, it, pt, de)
    "de", "del", "la", "las", "los", "el", "le", "les", "du", "das",
    "dos", "da", "di", "the", "und", "and", "&", "y",
    # reserves / age-group / gender markers
    "ii", "iii", "iv", "u18", "u19", "u20", "u21", "u23", "jr",
    "junior", "reserves", "youth",
}


def _ascii_lower(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return nfkd.encode("ascii", "ignore").decode().lower()


def normalize_team(raw: str) -> dict:
    """
    Return a structured representation of a team name:
      raw_lower   – lowercase accent-stripped original
      normalized  – noise-token-free whitespace-collapsed string
      tokens      – set of surviving tokens (for Jaccard overlap)
    """
    raw_lower = _ascii_lower(raw)
    cleaned = re.sub(r"[^\w\s]", " ", raw_lower)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    tokens = [t for t in cleaned.split() if t and t not in NOISE_TOKENS]
    if not tokens:
        # Every token was noise – fall back so we never compare empty sets
        tokens = cleaned.split()

    return {
        "raw_lower":  raw_lower,
        "normalized": " ".join(tokens),
        "tokens":     set(tokens),
    }


def _fuzzy(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def team_similarity(team_a: str, team_b: str) -> float:
    """
    Return a 0.0–1.0 similarity score between two team names.

    Combines Jaccard token overlap with a whole-string fuzzy ratio so
    the comparison stays robust across:
      • word-order swaps         "Partizan Belgrade" vs "Belgrade Partizan"
      • extra/missing prefixes   "FC Barcelona"      vs "Barcelona"
      • partial nickname overlap "Colegio Los Leones" vs "Leones de Quilpue"
    """
    if not team_a or not team_b:
        return 0.0

    a = normalize_team(team_a)
    b = normalize_team(team_b)

    if a["raw_lower"] == b["raw_lower"]:
        return 1.0
    if a["normalized"] and a["normalized"] == b["normalized"]:
        return 0.98

    inter = a["tokens"] & b["tokens"]
    union = a["tokens"] | b["tokens"]
    jaccard = len(inter) / len(union) if union else 0.0

    fuzzy = _fuzzy(a["normalized"], b["normalized"])
    blended = jaccard * 0.6 + fuzzy * 0.4

    # Distinctive shared-token guard: any common token of ≥5 chars is
    # rare enough (e.g. "leones", "partizan") that we floor the blend
    # so the side-level score still surfaces for the shared-team boost.
    shared_distinctive = {t for t in inter if len(t) >= 5}
    if shared_distinctive:
        blended = max(blended, 0.55)

    return max(blended, fuzzy)


def team_match_similarity(
    entry_a:    dict,
    entry_b:    dict,
    min_side:   float,
    strong_side: float,
) -> tuple[float, str]:
    """
    Compare two team-sport match entries on both pairings (normal and
    reversed). Returns (overall_score, human-readable explanation).

    A "shared-team boost" promotes pairs where one side is near-identical
    (>=strong_side) and the other side still has min_side overlap –
    basketball/volleyball almost never have the same team playing two
    concurrent live games, so a near-identical side is itself strong
    evidence of a duplicate listing.
    """
    h_norm = team_similarity(entry_a["home"], entry_b["home"])
    a_norm = team_similarity(entry_a["away"], entry_b["away"])
    s_norm = (h_norm + a_norm) / 2

    h_rev = team_similarity(entry_a["home"], entry_b["away"])
    a_rev = team_similarity(entry_a["away"], entry_b["home"])
    s_rev = (h_rev + a_rev) / 2

    if s_norm >= s_rev:
        score = s_norm
        min_s = min(h_norm, a_norm)
        max_s = max(h_norm, a_norm)
        expl = (
            f"  Home: {entry_a['home']!r} <-> {entry_b['home']!r}  [{h_norm:.2f}]\n"
            f"  Away: {entry_a['away']!r} <-> {entry_b['away']!r}  [{a_norm:.2f}]"
        )
    else:
        score = s_rev
        min_s = min(h_rev, a_rev)
        max_s = max(h_rev, a_rev)
        expl = (
            f"  HomeA<->AwayB: {entry_a['home']!r} <-> {entry_b['away']!r}  [{h_rev:.2f}]\n"
            f"  AwayA<->HomeB: {entry_a['away']!r} <-> {entry_b['home']!r}  [{a_rev:.2f}]"
        )

    if max_s >= strong_side and min_s >= min_side:
        score = max(score, 0.80)
        expl += f"\n  Shared-team boost: max_side={max_s:.2f} min_side={min_s:.2f}"

    if min_s < min_side and max_s < strong_side:
        score = min(score, min_side - 0.01)

    return score, expl


def detect_team_duplicates(
    entries:     list[dict],
    threshold:   float,
    min_side:    float,
    strong_side: float,
) -> list[dict]:
    """Compare every pair and return those above `threshold`."""
    suspects = []
    for i in range(len(entries)):
        for j in range(i + 1, len(entries)):
            a, b = entries[i], entries[j]
            score, expl = team_match_similarity(a, b, min_side, strong_side)
            if score >= threshold:
                suspects.append({
                    "score":       score,
                    "match_a":     a,
                    "match_b":     b,
                    "explanation": expl,
                    "pair_key":    frozenset([a["url"], b["url"]]),
                })
    return suspects


def confidence_label(score: float) -> str:
    if score >= 0.92:
        return "Very high"
    if score >= 0.82:
        return "High"
    return "Moderate"


# ── Scraping ───────────────────────────────────────────────────────

async def expand_all_sections(page: Page) -> int:
    """Click every collapsed league header so all hidden matches load."""
    closed = await page.query_selector_all(
        'div[data-state="closed"][class*="bg-th-card-container"]'
    )
    if not closed:
        return 0
    for header in closed:
        try:
            await header.scroll_into_view_if_needed()
            await header.click()
            await page.wait_for_timeout(250)
        except Exception:
            pass
    await page.wait_for_timeout(800)
    return len(closed)


async def extract_team_matches(page: Page, url: str) -> list[dict]:
    """
    Reload `url`, expand every collapsed section, and return every match:
      [{"url","home","away","section","not_started"}, ...]

    DOM structure (identical to tennis listing):
      - Section headers: div[data-state="closed"|"open"][class*="bg-th-card-container"]
      - Match links:     a[href] matching /en/live/<id>-...-vs-...
      - Team names:      first two div.truncate[class*="text-th-primary-text"]
                         inside the link's card container.
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25_000)
    except Exception as exc:
        print(f"[warn] page load: {exc}")
    await page.wait_for_timeout(4_000)
    await expand_all_sections(page)

    return await page.evaluate(
        """
        () => {
            const matchRe = /\\/en\\/live\\/\\d+-.+-vs-/;
            const cls = e => (e.getAttribute('class') || '');
            const seen = new Set();
            const results = [];

            for (const link of document.querySelectorAll('a[href]')) {
                const href = link.href.split('?')[0];
                if (!matchRe.test(new URL(link.href).pathname)) continue;
                if (seen.has(href)) continue;
                seen.add(href);

                let section = "";
                let secEl = link.parentElement;
                for (let sd = 0; sd < 20 && secEl; sd++) {
                    const sc = secEl.getAttribute('class') || '';
                    if (sc.includes('bg-th-card-container') && secEl.hasAttribute('data-state')) {
                        for (const ch of secEl.children) {
                            const t = ch.innerText
                                ? ch.innerText.trim().split(String.fromCharCode(10))[0]
                                : '';
                            if (t.length > 2 && t.length < 120 && !t.includes(' vs ') && !t.includes('/')) {
                                section = t;
                                break;
                            }
                        }
                        break;
                    }
                    secEl = secEl.parentElement;
                }

                let container = link.parentElement;
                let found = false;
                for (let depth = 0; depth < 8 && container; depth++) {
                    const nameDivs = [...container.querySelectorAll('div')].filter(d => {
                        const c = cls(d);
                        return c.includes('truncate') && c.includes('text-th-primary-text');
                    });
                    if (nameDivs.length >= 2) {
                        const cardText  = (container.innerText || '').toLowerCase();
                        const notStarted = cardText.includes('not started');
                        results.push({
                            url:         href,
                            home:        nameDivs[0].innerText.trim(),
                            away:        nameDivs[1].innerText.trim(),
                            section:     section,
                            not_started: notStarted,
                        });
                        found = true;
                        break;
                    }
                    container = container.parentElement;
                }
                if (!found) {
                    const slug  = new URL(link.href).pathname.replace('/en/live/', '');
                    const vsIdx = slug.indexOf('-vs-');
                    if (vsIdx !== -1) {
                        const numEnd   = slug.indexOf('-');
                        const homePart = slug.slice(numEnd + 1, vsIdx).replace(/-/g, ' ');
                        const awayPart = slug.slice(vsIdx + 4).replace(/-/g, ' ');
                        results.push({
                            url: href, home: homePart, away: awayPart,
                            section: section, not_started: false,
                        });
                    }
                }
            }
            return results;
        }
        """
    )


# ── Persistence ────────────────────────────────────────────────────

def load_pairs(path: Path) -> set[frozenset]:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return {frozenset(p) for p in data}
        except Exception as exc:
            print(f"[warn] load_pairs({path}): {exc}")
    return set()


def save_pairs(path: Path, pairs: set[frozenset]) -> None:
    try:
        path.write_text(
            json.dumps([sorted(p) for p in pairs], indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[warn] save_pairs({path}): {exc}")


# ── Main loop (one task per sport) ────────────────────────────────

async def run_team_sport_loop(
    *,
    sport:         str,                              # "basketball" | "volleyball"
    emoji:         str,                              # for Telegram message header
    url:           str,
    page:          Page,
    interval:      int,
    threshold:     float,
    min_side:      float,
    strong_side:   float,
    pairs_file:    Path,
    send_telegram: Callable[[str], Awaitable[None]],
    counters:      dict | None = None,
) -> None:
    """
    Long-lived asyncio task. Polls the listing page every `interval`s,
    runs duplicate detection, and pushes alerts via `send_telegram`.

    `counters` (optional) is a shared dict mutated in place after each
    cycle so a heartbeat task can read live coverage:
        {
          "<sport>": {
              "live_count": int,
              "last_cycle_at": datetime | None,
              "alerts_since_heartbeat": int,
              "current_matches": list[dict],
          }
        }
    """
    alerted_pairs = load_pairs(pairs_file)
    if alerted_pairs:
        print(f"[{sport}] Loaded {len(alerted_pairs)} previously alerted pair(s).")

    print(f"[{sport}] Starting loop. URL: {url}")
    print(f"[{sport}] Threshold={threshold}  min_side={min_side}  strong_side={strong_side}")

    if counters is not None:
        counters[sport] = {
            "live_count":            0,
            "last_cycle_at":         None,
            "alerts_since_heartbeat": 0,
            "current_matches":       [],
        }

    while True:
        try:
            entries = await extract_team_matches(page, url)
            current_urls = {e["url"] for e in entries}

            # Drop alerts for matches that have ended
            expired = {pk for pk in alerted_pairs if not pk.issubset(current_urls)}
            if expired:
                alerted_pairs -= expired
                save_pairs(pairs_file, alerted_pairs)

            live_entries        = [e for e in entries if not e.get("not_started")]
            not_started_entries = [e for e in entries if e.get("not_started")]

            # Per-cycle inventory dump (mirrors the tennis loop's format
            # in monitor.py so all three sports look the same on stdout).
            print(
                f"\n[{sport}] {len(entries)} match(es) on listing page "
                f"({len(live_entries)} live, {len(not_started_entries)} not started):"
            )
            for e in live_entries:
                section = e.get("section") or "—"
                print(f"    [LIVE]        {e['home']} vs {e['away']}  ({section})")
            for e in not_started_entries:
                section = e.get("section") or "—"
                print(
                    f"    [NOT STARTED] {e['home']} vs {e['away']}  ({section})"
                    f"  ← excluded from checks"
                )

            if counters is not None:
                state = counters[sport]
                state["live_count"]      = len(live_entries)
                state["last_cycle_at"]   = datetime.now(timezone.utc)
                state["current_matches"] = live_entries

            suspects = detect_team_duplicates(
                live_entries, threshold, min_side, strong_side
            )
            new_suspects = [s for s in suspects if s["pair_key"] not in alerted_pairs]

            for s in new_suspects:
                a, b  = s["match_a"], s["match_b"]
                pct   = int(s["score"] * 100)
                label = confidence_label(s["score"])

                print(f"[{sport}] DUPLICATE [{label} {pct}%]")
                print(f"  A: {a['home']} vs {a['away']}  ({a.get('section','')})")
                print(f"  B: {b['home']} vs {b['away']}  ({b.get('section','')})")
                print(s["explanation"])

                msg = (
                    f"{emoji} <b>Possible duplicate {sport} match!</b>\n"
                    f"Confidence: <b>{label} ({pct}%)</b>\n\n"
                    f"<b>Match A:</b>  {a['home']}  vs  {a['away']}\n"
                    f"  Section: {a.get('section') or '—'}\n\n"
                    f"<b>Match B:</b>  {b['home']}  vs  {b['away']}\n"
                    f"  Section: {b.get('section') or '—'}\n\n"
                    f"Name comparison:\n{s['explanation']}\n\n"
                    f"<a href='{a['url']}'>Open Match A</a>\n"
                    f"<a href='{b['url']}'>Open Match B</a>"
                )
                await send_telegram(msg)
                alerted_pairs.add(s["pair_key"])
                if counters is not None:
                    counters[sport]["alerts_since_heartbeat"] += 1

            if new_suspects:
                save_pairs(pairs_file, alerted_pairs)

        except Exception as exc:
            print(f"[{sport}] cycle error: {exc}")

        await asyncio.sleep(interval)
