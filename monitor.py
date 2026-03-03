"""
Dafabet Tennis Duplicate Match Detector
========================================
Monitors https://sports.dafabet.com/en/live/sport/239-TENN every
CHECK_INTERVAL seconds.

Detects when two live match listings are likely the SAME real match
listed twice with slightly different player name formats, e.g.:

  Match A: "Butvilas, Edas vs Imamura, Masamichi"
  Match B: "Butvilas, E vs Imamura, M"

Name formats observed on the site:
  • "Lastname, Firstname"              (full name,   singles)
  • "Lastname, F"                      (initial only, singles)
  • "Lastname, F M"                    (initial + middle initial, comma)
  • "Lastname F"                       (no comma, singles – e.g. "Shimizu Y")
  • "Lastname F M"                     (no comma, first + middle initial – e.g. "Romios M C")
  • "Lastname Firstname M"             (no comma, full first + middle initial)
  • "Lastname1, F1/Lastname2, F2"      (doubles pair, slash no spaces)
  • "Lastname1 F1 / Lastname2 F2"      (doubles pair, slash with spaces)

The matching model compares every live match pair and alerts via Telegram
when the similarity score exceeds SIMILARITY_THRESHOLD.

Quick-start
-----------
1. cp .env.example .env   # fill in credentials (Telegram + Anthropic)
2. pip install -r requirements.txt
3. playwright install chromium
4. python monitor.py

Required .env keys:
  TELEGRAM_BOT_TOKEN   – Telegram bot token
  TELEGRAM_CHAT_ID     – Telegram chat/group ID
  ANTHROPIC_API_KEY    – Anthropic API key (enables Claude Opus 4.6 analysis)

Optional .env keys:
  CHECK_INTERVAL       – seconds between polls (default: 60)
  HEARTBEAT_INTERVAL   – seconds between heartbeat messages (default: 3600)
  SIMILARITY_THRESHOLD – rule-based duplicate threshold (default: 0.75)
  MIN_SIDE_SCORE       – rule-based per-side floor (default: 0.60)
  HEADLESS             – run browser headless (default: true)
  AI_ANALYSIS          – enable Claude Opus 4.6 analysis (default: true)
"""

import asyncio
import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import anthropic
import httpx
from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

# ── Load secrets from .env (never commit .env to git) ─────────────
load_dotenv()

# ══════════════════════════════════════════════════════════════════
#  CONFIG  ← sensitive values live in .env; tune the rest here
# ══════════════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN: str   = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID:   str   = os.environ["TELEGRAM_CHAT_ID"]

# Seconds between each poll of the tennis listing page (default 1 minute)
CHECK_INTERVAL: int       = int(os.getenv("CHECK_INTERVAL", "60"))

# Send a "still alive" heartbeat message every N seconds (default 1 hour)
HEARTBEAT_INTERVAL: int   = int(os.getenv("HEARTBEAT_INTERVAL", "3600"))

# Similarity score (0.0–1.0) above which a pair is flagged as a likely duplicate.
# Lower = more sensitive (more alerts); higher = stricter.
SIMILARITY_THRESHOLD: float = float(os.getenv("SIMILARITY_THRESHOLD", "0.75"))

# Minimum per-side score – both home AND away must exceed this floor
MIN_SIDE_SCORE: float       = float(os.getenv("MIN_SIDE_SCORE", "0.60"))

# Run browser without a visible window.
# MUST be True on a headless VPS (no display); False for local debugging.
HEADLESS: bool = os.getenv("HEADLESS", "true").lower() in ("1", "true", "yes")

# Anthropic API key for AI-powered analysis (optional – falls back to rule-based only)
ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

# Enable AI analysis using Claude Haiku 4.5 (requires ANTHROPIC_API_KEY)
AI_ANALYSIS: bool = os.getenv("AI_ANALYSIS", "true").lower() in ("1", "true", "yes")

# Tennis live page URL
TENNIS_URL: str = "https://sports.dafabet.com/en/live/sport/239-TENN"

# File used to persist alerted pairs across restarts
PAIRS_FILE: Path = Path(".alerted_pairs.json")

# ══════════════════════════════════════════════════════════════════


# ── Name parsing & similarity ──────────────────────────────────────

def _ascii_lower(s: str) -> str:
    """Strip accents and lowercase."""
    nfkd = unicodedata.normalize("NFKD", s)
    return nfkd.encode("ascii", "ignore").decode().lower()


def parse_player(raw: str) -> dict:
    """
    Parse a single player name into structured components.

    Returns dict with keys: surname, first, initial, raw_lower

    Handles all real formats observed on Dafabet tennis pages:
      "Butvilas, Edas"        → surname="butvilas", first="edas",  initial="e"
      "Gorgodze, E"           → surname="gorgodze", first="",      initial="e"
      "Alcala Gurri, M"       → surname="alcala gurri", first="",  initial="m"
      "Mintegi del Olmo, A"   → surname="mintegi del olmo", initial="a"
      "Shimizu Y"             → surname="shimizu", initial="y"
      "Romios M C"            → surname="romios",  initial="m"  (middle initial C ignored)
      "Smith John C"          → surname="smith",   first="john", initial="j"
    """
    s = re.sub(r"\s+", " ", raw.strip())
    raw_lower = _ascii_lower(s)
    first = ""

    if "," in s:
        # "Surname[s], Firstname-or-Initial [MiddleInitial…]"
        # Everything before the first comma is the (possibly compound) surname.
        # Only the first token after the comma matters; trailing middle initials ignored.
        surname_part, rest = s.split(",", 1)
        surname = _ascii_lower(surname_part.strip())
        tokens = rest.strip().split()
        if tokens:
            tok0 = tokens[0].rstrip(".")
            if len(tok0) == 1:
                initial = tok0.lower()
            else:
                initial = tok0[0].lower()
                first   = _ascii_lower(tok0)
        else:
            initial = ""
    else:
        # No-comma formats:
        #   "Surname Initial"         – 2 tokens, last is 1 char  → "Shimizu Y"
        #   "Surname M C"             – 3+ tokens, all trailing are initials → "Romios M C"
        #   "Surname Firstname C"     – 3+ tokens, second is a full name → "Smith John C"
        #   "Surname Firstname"       – 2 tokens, last is multi-char
        tokens = s.split()
        if len(tokens) >= 2:
            last_tok = tokens[-1].rstrip(".")
            if len(last_tok) == 1:
                # Trailing token is an initial.
                # Surname is ALWAYS just the first token (never absorb middle initials).
                surname = _ascii_lower(tokens[0])
                if len(tokens) == 2:
                    # "Shimizu Y" – simple case
                    initial = last_tok.lower()
                else:
                    # 3+ tokens: "Romios M C" or "Smith John C"
                    second = tokens[1].rstrip(".")
                    if len(second) == 1:
                        # All post-surname tokens are initials; use the first one
                        initial = second.lower()
                    else:
                        # Second token is a full first name, last token is middle initial
                        initial = second[0].lower()
                        first   = _ascii_lower(second)
            else:
                # "Surname Firstname" – last token is a full first name
                initial = last_tok[0].lower()
                first   = _ascii_lower(last_tok)
                surname = _ascii_lower(tokens[0])
        else:
            surname = raw_lower
            initial = ""

    return {"surname": surname, "first": first, "initial": initial, "raw_lower": raw_lower}


def _fuzzy(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def player_similarity(name_a: str, name_b: str) -> float:
    """
    Return a 0.0–1.0 similarity score between two player name strings.

    Scoring guide:
      1.00 – identical strings
      0.92 – same surname + same first initial (one may be abbreviated)
      0.85 – same surname + both full first names that look like transliterations
      0.70 – same surname, no initials to compare
      0.15 – same surname but DIFFERENT first initials (different person)
      <0.70– surname mismatch → overall fuzzy fallback
    """
    if not name_a or not name_b:
        return 0.0

    a = parse_player(name_a)
    b = parse_player(name_b)

    if a["raw_lower"] == b["raw_lower"]:
        return 1.0

    surname_sim = _fuzzy(a["surname"], b["surname"])

    if surname_sim >= 0.85:
        ai, bi = a["initial"], b["initial"]
        af, bf = a["first"],   b["first"]

        if ai and bi:
            if ai != bi:
                # Confirmed different first initial → almost certainly different player
                return 0.15
            # Same initial
            if af and bf and af != bf:
                # Both have full first names that differ; allow slight fuzzy for transliteration
                first_sim = _fuzzy(af, bf)
                if first_sim >= 0.70:
                    return 0.85
                return 0.60
            # One or both abbreviated – can't confirm mismatch
            return 0.92

        # At least one side has no initial info
        return 0.70 * surname_sim

    # Surnames differ significantly – fall back to whole-string fuzzy
    return _fuzzy(a["raw_lower"], b["raw_lower"]) * 0.70


def split_doubles(name: str) -> list[str]:
    """Split a doubles entry like "Riera, Julia/Romero Gormaz, Leyre" into two players."""
    parts = re.split(r"\s*/\s*", name)
    return parts if len(parts) == 2 else [name]


def side_similarity(side_a: str, side_b: str) -> float:
    """Compare one side of a match (handles singles and doubles)."""
    pa = split_doubles(side_a)
    pb = split_doubles(side_b)

    if len(pa) == 1 and len(pb) == 1:
        return player_similarity(pa[0], pb[0])

    if len(pa) == 2 and len(pb) == 2:
        # In-order comparison
        s_ordered = (player_similarity(pa[0], pb[0]) + player_similarity(pa[1], pb[1])) / 2
        # Reverse partner order (rare but guard against it)
        s_reversed = (player_similarity(pa[0], pb[1]) + player_similarity(pa[1], pb[0])) / 2
        return max(s_ordered, s_reversed)

    # Mixed singles vs doubles → not the same match
    return 0.0


def match_similarity(entry_a: dict, entry_b: dict) -> tuple[float, str]:
    """
    Compare two match entries (each has 'home', 'away', 'url').
    Returns (overall_score, human-readable explanation).

    Checks both normal pairing (home↔home, away↔away)
    and reversed pairing (home↔away, away↔home).
    """
    # Normal pairing
    h_norm = side_similarity(entry_a["home"], entry_b["home"])
    a_norm = side_similarity(entry_a["away"], entry_b["away"])
    s_norm = (h_norm + a_norm) / 2

    # Reversed pairing (match listed with sides swapped)
    h_rev  = side_similarity(entry_a["home"], entry_b["away"])
    a_rev  = side_similarity(entry_a["away"], entry_b["home"])
    s_rev  = (h_rev + a_rev) / 2

    if s_norm >= s_rev:
        score = s_norm
        min_side = min(h_norm, a_norm)
        expl = (
            f"  Home: {entry_a['home']!r} ↔ {entry_b['home']!r}  [{h_norm:.2f}]\n"
            f"  Away: {entry_a['away']!r} ↔ {entry_b['away']!r}  [{a_norm:.2f}]"
        )
    else:
        score = s_rev
        min_side = min(h_rev, a_rev)
        expl = (
            f"  HomeA↔AwayB: {entry_a['home']!r} ↔ {entry_b['away']!r}  [{h_rev:.2f}]\n"
            f"  AwayA↔HomeB: {entry_a['away']!r} ↔ {entry_b['home']!r}  [{a_rev:.2f}]"
        )

    # Reject if either side scored below the floor (prevents one-sided matches)
    if min_side < MIN_SIDE_SCORE:
        score = min(score, MIN_SIDE_SCORE - 0.01)

    return score, expl


def detect_duplicates(entries: list[dict]) -> list[dict]:
    """
    Compare all n*(n-1)/2 pairs of live match entries.
    Return list of pairs that exceed SIMILARITY_THRESHOLD.
    """
    suspects = []
    for i in range(len(entries)):
        for j in range(i + 1, len(entries)):
            a, b = entries[i], entries[j]
            score, expl = match_similarity(a, b)
            if score >= SIMILARITY_THRESHOLD:
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


# ── Persistence ────────────────────────────────────────────────────

def load_alerted_pairs() -> set[frozenset]:
    """Load previously alerted URL pairs from disk (survives restarts)."""
    if PAIRS_FILE.exists():
        try:
            data = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
            return {frozenset(p) for p in data}
        except Exception as exc:
            print(f"[warn] Could not load alerted pairs: {exc}")
    return set()


def save_alerted_pairs(pairs: set[frozenset]) -> None:
    """Persist alerted URL pairs to disk."""
    try:
        PAIRS_FILE.write_text(
            json.dumps([sorted(p) for p in pairs], indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"[warn] Could not save alerted pairs: {exc}")


async def ai_analyze_matches(
    aclient: anthropic.AsyncAnthropic,
    entries: list[dict],
) -> list[dict]:
    """
    Send the full list of live match entries to Claude in a single batch call.

    Detects two issue types:
      DUPLICATE       – same real match listed twice with different name formats
      PLAYER_CONFLICT – same player appearing in two different live matches simultaneously

    Returns a list of issue dicts (match_indices are 0-based):
      {
        "type":          "DUPLICATE" | "PLAYER_CONFLICT",
        "match_indices": [i, j],
        "explanation":   str,
        "confidence":    "high" | "medium" | "low",
      }
    """
    if len(entries) < 2:
        return []

    match_list = "\n".join(
        f"{i + 1}. Home: {e['home']} | Away: {e['away']} | Section: {e.get('section') or 'unknown'}"
        for i, e in enumerate(entries)
    )

    prompt = (
        "You are a tennis match integrity monitor for a live sports betting site.\n"
        "Below is the complete list of currently live tennis matches.\n\n"
        "Find these issues:\n"
        "1. DUPLICATE — same real match listed twice with different name formats.\n"
        "   Example: 'Butvilas, Edas' vs 'Butvilas, E' are the same person.\n"
        "2. PLAYER_CONFLICT — same real player appearing in two DIFFERENT matches simultaneously.\n\n"
        "Hard rules:\n"
        "- Different sections/tournaments → never a duplicate.\n"
        "- Singles vs doubles (slash in name) → never a duplicate.\n"
        "- Surname alone is NOT enough — need first initial or full name to confirm.\n"
        "- Name format differences (comma/no comma, abbreviated/full) are expected — do not flag these alone.\n"
        "- Only flag issues you are confident about.\n\n"
        "Output ONLY valid JSON, no markdown:\n"
        '{"issues": [{"type": "DUPLICATE" or "PLAYER_CONFLICT", '
        '"indices": [i, j], "confidence": "high" or "medium" or "low", "reason": "..."}]}\n\n'
        "Indices are 1-based. If no issues found, output {\"issues\": []}.\n\n"
        f"LIVE MATCHES ({len(entries)} total):\n{match_list}"
    )

    try:
        response = await aclient.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        text = next((b.text for b in response.content if b.type == "text"), "")
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start == -1 or end <= start:
            print(f"[AI] No JSON in response: {text[:200]}")
            return []
        data = json.loads(text[start:end])
    except Exception as exc:
        print(f"[AI] Batch analysis error: {exc}")
        return []

    issues: list[dict] = []
    for item in data.get("issues", []):
        idxs = item.get("indices", [])
        if len(idxs) < 2:
            continue
        i, j = idxs[0] - 1, idxs[1] - 1   # 1-based → 0-based
        if not (0 <= i < len(entries) and 0 <= j < len(entries) and i != j):
            continue
        issues.append({
            "type":          item.get("type", "DUPLICATE"),
            "match_indices": [i, j],
            "explanation":   item.get("reason", ""),
            "confidence":    item.get("confidence", "medium"),
        })
    return issues


# ── Dafabet scraping ───────────────────────────────────────────────

async def expand_all_sections(page: Page) -> int:
    """
    Click every collapsed league/group header so all hidden matches become visible.

    Confirmed structure (2026-02-27 analysis):
      Collapsed header: div[data-state="closed"][class*="bg-th-card-container"]
      Expanded header:  div[data-state="open"][class*="bg-th-card-container"]

    Returns the number of sections that were expanded.
    """
    # Grab all collapsed headers as element handles (must be done before clicking,
    # since clicking one can reflow the DOM)
    closed = await page.query_selector_all(
        'div[data-state="closed"][class*="bg-th-card-container"]'
    )
    if not closed:
        return 0

    print(f"  [*] Expanding {len(closed)} collapsed section(s)…")
    for header in closed:
        try:
            await header.scroll_into_view_if_needed()
            await header.click()
            await page.wait_for_timeout(250)   # let each section animate open
        except Exception as exc:
            print(f"  [warn] Could not expand section: {exc}")

    # Give the last sections a moment to fully render their match cards
    await page.wait_for_timeout(800)
    return len(closed)


async def extract_matches(page: Page, url: str) -> list[dict]:
    """
    Reload the tennis listing page, expand ALL collapsed sections, then return
    every live match entry: [{"url": ..., "home": ..., "away": ...}, ...]

    Confirmed DOM structure:
      - Collapsed section headers: div[data-state="closed"][class*="bg-th-card-container"]
      - Match links:  a[href] matching /en/live/<id>-...-vs-...
      - Player names: first two div.truncate[class*="text-th-primary-text"] inside
                      the match card (link's parent element)
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=25_000)
    except Exception as exc:
        print(f"[warn] page load: {exc}")
    await page.wait_for_timeout(4_000)

    # ── Expand every collapsed league/group section ───────────────
    n_expanded = await expand_all_sections(page)
    if n_expanded:
        print(f"  [*] {n_expanded} section(s) expanded.")

    entries: list[dict] = await page.evaluate(
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

                // Find section/tournament name (best-effort, graceful on miss)
                let section = "";
                let secEl = link.parentElement;
                for (let sd = 0; sd < 20 && secEl; sd++) {
                    const sc = secEl.getAttribute('class') || '';
                    if (sc.includes('bg-th-card-container') && secEl.hasAttribute('data-state')) {
                        for (const ch of secEl.children) {
                            const t = ch.innerText ? ch.innerText.trim().split(String.fromCharCode(10))[0] : '';
                            if (t.length > 2 && t.length < 120 && !t.includes(' vs ') && !t.includes('/')) {
                                section = t;
                                break;
                            }
                        }
                        break;
                    }
                    secEl = secEl.parentElement;
                }

                // Walk up from the link to find the card container
                let container = link.parentElement;
                let found = false;
                for (let depth = 0; depth < 8 && container; depth++) {
                    const nameDivs = [...container.querySelectorAll('div')].filter(d => {
                        const c = cls(d);
                        return c.includes('truncate') && c.includes('text-th-primary-text');
                    });
                    if (nameDivs.length >= 2) {
                        results.push({
                            url:     href,
                            home:    nameDivs[0].innerText.trim(),
                            away:    nameDivs[1].innerText.trim(),
                            section: section,
                        });
                        found = true;
                        break;
                    }
                    container = container.parentElement;
                }
                if (!found) {
                    // Fallback: parse names from URL slug
                    const slug = new URL(link.href).pathname.replace('/en/live/', '');
                    const vsIdx = slug.indexOf('-vs-');
                    if (vsIdx !== -1) {
                        const numEnd = slug.indexOf('-');
                        const homePart = slug.slice(numEnd + 1, vsIdx).replace(/-/g, ' ');
                        const awayPart = slug.slice(vsIdx + 4).replace(/-/g, ' ');
                        results.push({ url: href, home: homePart, away: awayPart, section: section });
                    }
                }
            }
            return results;
        }
        """
    )
    return entries


# ── Telegram helpers ───────────────────────────────────────────────

async def send_telegram(text: str) -> None:
    api_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(api_url, json=payload)
            if not resp.is_success:
                print(f"[Telegram] {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        print(f"[Telegram] error: {exc}")


async def heartbeat_loop(started_at: datetime, current_matches: list[dict]) -> None:
    """
    Send a Telegram 'still alive' message every HEARTBEAT_INTERVAL seconds.
    Includes the current live match list so you can confirm what is being watched.
    Runs as a background asyncio task alongside the main polling loop.
    """
    await asyncio.sleep(HEARTBEAT_INTERVAL)   # first beat after one full interval
    while True:
        uptime_secs = int((datetime.now(timezone.utc) - started_at).total_seconds())
        hours, rem  = divmod(uptime_secs, 3600)
        minutes     = rem // 60
        now_str     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        if current_matches:
            match_lines = "\n".join(
                f"  {i + 1}. {e['home']} vs {e['away']}"
                for i, e in enumerate(current_matches)
            )
            match_section = f"\n\n🎾 <b>Live matches ({len(current_matches)}):</b>\n{match_lines}"
        else:
            match_section = "\n\n🎾 No live matches right now."

        msg = (
            f"💓 <b>Monitor heartbeat</b>\n"
            f"Uptime: <b>{hours}h {minutes}m</b>  |  {now_str}"
            f"{match_section}"
        )
        print(f"[heartbeat] Sending alive message (uptime {hours}h {minutes}m), "
              f"{len(current_matches)} live match(es)")
        await send_telegram(msg)
        await asyncio.sleep(HEARTBEAT_INTERVAL)


# ── Entry point ────────────────────────────────────────────────────

async def main() -> None:
    started_at    = datetime.now(timezone.utc)
    alerted_pairs = load_alerted_pairs()
    if alerted_pairs:
        print(f"[*] Loaded {len(alerted_pairs)} previously alerted pair(s) from disk.")

    # ── Anthropic client for AI analysis ──────────────────────────────
    aclient: anthropic.AsyncAnthropic | None = None
    if AI_ANALYSIS:
        if ANTHROPIC_API_KEY:
            aclient = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
            print("[*] AI pairwise classifier enabled (Claude Haiku 4.5).")
        else:
            print("[!] AI_ANALYSIS=true but ANTHROPIC_API_KEY not set – AI disabled.")

    # ── Send startup ping BEFORE browser loads ────────────────────────
    # This confirms the script is alive even before the first scrape.
    ai_status = "Haiku 4.5 ✓" if aclient else "rule-based only"
    await send_telegram(
        f"🟢 <b>Tennis duplicate monitor starting…</b>\n"
        f"AI analysis: <b>{ai_status}</b>\n"
        f"Polling every {CHECK_INTERVAL}s · "
        f"Heartbeat every {HEARTBEAT_INTERVAL // 60}min\n"
        f"Started at: {started_at.strftime('%Y-%m-%d %H:%M UTC')}"
    )

    # Shared list updated each scrape cycle; read by heartbeat_loop
    current_matches: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            slow_mo=0 if HEADLESS else 60,   # no artificial delay needed on VPS
            args=["--no-sandbox", "--disable-dev-shm-usage"] if HEADLESS else [],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()

        print(f"[*] Starting tennis duplicate detector. Polling every {CHECK_INTERVAL}s.")
        print(f"[*] Similarity threshold: {SIMILARITY_THRESHOLD}  |  Min per-side: {MIN_SIDE_SCORE}")
        print(f"[*] Heartbeat every {HEARTBEAT_INTERVAL // 60} min")
        print(f"[*] URL: {TENNIS_URL}\n")

        # ── Launch heartbeat as a background task ─────────────────────
        heartbeat_task = asyncio.create_task(heartbeat_loop(started_at, current_matches))

        try:
            while True:
                entries = await extract_matches(page, TENNIS_URL)
                current_urls = {e["url"] for e in entries}

                # Keep heartbeat_loop up to date with latest match list
                current_matches.clear()
                current_matches.extend(entries)

                # ── Expire pairs where a match is no longer live ─────────
                expired = {pk for pk in alerted_pairs if not pk.issubset(current_urls)}
                if expired:
                    print(f"[*] {len(expired)} previously alerted pair(s) expired (match ended).")
                    alerted_pairs -= expired
                    save_alerted_pairs(alerted_pairs)

                if not entries:
                    print("[!] No live tennis matches found – will retry.")
                else:
                    print(f"\n[*] {len(entries)} live match(es):")
                    for e in entries:
                        print(f"    {e['home']} vs {e['away']}")

                    suspects = detect_duplicates(entries)
                    new_suspects = [s for s in suspects if s["pair_key"] not in alerted_pairs]

                    if new_suspects:
                        print(f"\n[!] {len(new_suspects)} new duplicate pair(s) detected!")
                        for s in new_suspects:
                            a = s["match_a"]
                            b = s["match_b"]
                            pct   = int(s["score"] * 100)
                            label = confidence_label(s["score"])

                            print(
                                f"  [{label} – {pct}%]\n"
                                f"    A: {a['home']} vs {a['away']}\n"
                                f"    B: {b['home']} vs {b['away']}\n"
                                f"{s['explanation']}"
                            )

                            msg = (
                                f"🎾 <b>Possible duplicate tennis match!</b>\n"
                                f"Confidence: <b>{label} ({pct}%)</b>\n\n"
                                f"<b>Match A:</b>  {a['home']}  vs  {a['away']}\n"
                                f"<b>Match B:</b>  {b['home']}  vs  {b['away']}\n\n"
                                f"Name comparison:\n"
                                f"{s['explanation']}\n\n"
                                f"<a href='{a['url']}'>Open Match A</a>\n"
                                f"<a href='{b['url']}'>Open Match B</a>"
                            )
                            await send_telegram(msg)
                            alerted_pairs.add(s["pair_key"])
                        save_alerted_pairs(alerted_pairs)
                    else:
                        print("    No duplicates detected in this cycle.")

                    # ── AI analysis (Claude Opus 4.6) ─────────────────────────
                    if aclient:
                        print("\n[AI] Running batch analysis (Claude Haiku 4.5)…")
                        ai_issues = await ai_analyze_matches(aclient, entries)

                        new_ai = []
                        for issue in ai_issues:
                            idxs = issue.get("match_indices", [])
                            if len(idxs) < 2:
                                continue
                            i, j = idxs[0], idxs[1]
                            if i >= len(entries) or j >= len(entries) or i < 0 or j < 0:
                                continue
                            pair_key = frozenset([entries[i]["url"], entries[j]["url"]])
                            if pair_key not in alerted_pairs:
                                issue["pair_key"]  = pair_key
                                issue["match_a"]   = entries[i]
                                issue["match_b"]   = entries[j]
                                new_ai.append(issue)

                        if new_ai:
                            print(f"[AI] {len(new_ai)} new issue(s) detected!")
                            for issue in new_ai:
                                a    = issue["match_a"]
                                b    = issue["match_b"]
                                kind = issue["type"]
                                conf = issue["confidence"].capitalize()
                                expl = issue["explanation"]

                                print(
                                    f"  [{kind} – {conf}]\n"
                                    f"    A: {a['home']} vs {a['away']}\n"
                                    f"    B: {b['home']} vs {b['away']}\n"
                                    f"    {expl}"
                                )

                                if kind == "PLAYER_CONFLICT":
                                    emoji      = "⚠️"
                                    type_label = "Player conflict detected! (AI)"
                                else:  # DUPLICATE
                                    emoji      = "🎾"
                                    type_label = "Possible duplicate tennis match! (AI)"

                                msg = (
                                    f"{emoji} <b>{type_label}</b>\n"
                                    f"Confidence: <b>{conf}</b>\n\n"
                                    f"<b>Match A:</b>  {a['home']}  vs  {a['away']}\n"
                                    f"<b>Match B:</b>  {b['home']}  vs  {b['away']}\n\n"
                                    f"<b>AI analysis:</b> {expl}\n\n"
                                    f"<a href='{a['url']}'>Open Match A</a>\n"
                                    f"<a href='{b['url']}'>Open Match B</a>"
                                )
                                await send_telegram(msg)
                                alerted_pairs.add(issue["pair_key"])
                            save_alerted_pairs(alerted_pairs)
                        else:
                            print("[AI] No new issues detected.")

                print(f"\n--- sleeping {CHECK_INTERVAL}s ---\n")
                await asyncio.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            print("\n[*] Stopped by user.")
        finally:
            heartbeat_task.cancel()
            await send_telegram(
                f"🔴 <b>Tennis duplicate monitor stopped</b>\n"
                f"Stopped at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
            )
            await browser.close()
            print("[*] Browser closed.")


if __name__ == "__main__":
    asyncio.run(main())
