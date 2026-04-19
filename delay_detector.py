"""
Delay Detection Module for Dafabet Tennis Monitor
===================================================
Scrapes live tennis scores from flashscore.mobi and compares them against
Dafabet's live scores. If Dafabet is behind by 1+ sets, triggers a Telegram
alert flagging a score delay anomaly.

Usage:
    This module is imported into monitor.py and called during each polling cycle.

Data source:
    https://www.flashscore.mobi/tennis/?s=2  (LIVE matches only)

    Format on the page:
        "Set 2Zverev A. (Ger) - Sinner J. (Ita) [0:1] (1:6,4:4)"
        ^^^^^^                                  ^^^^  ^^^^^^^^^^^
        current set                          sets won  game scores per set

How it works:
    1. Each poll cycle, fetch flashscore.mobi/tennis/?s=2 in a new Playwright page
    2. Parse all live matches with player names + set scores
    3. For each Dafabet live match, fuzzy-match player names to find the same
       match on Flashscore
    4. Compare the number of completed sets (or current set number)
    5. If Flashscore is ahead by 1+ set → flag as DELAY anomaly → Telegram alert

Integration:
    Add to monitor.py's main loop after duplicate detection:

        delay_results = await check_score_delays(context, live_entries)
        for d in delay_results:
            await send_telegram(d["alert_msg"])
"""

import asyncio
import re
import unicodedata
from difflib import SequenceMatcher


# ── Flashscore parsing ─────────────────────────────────────────────

FLASHSCORE_LIVE_URL = "https://www.flashscore.mobi/tennis/?s=2"


def _ascii_lower(s: str) -> str:
    """Strip accents and lowercase."""
    nfkd = unicodedata.normalize("NFKD", s)
    return nfkd.encode("ascii", "ignore").decode().lower()


def _fuzzy(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def parse_flashscore_line(line: str) -> dict | None:
    """
    Parse a single live match line from flashscore.mobi.

    Examples:
        "Set 2Zverev A. (Ger) - Sinner J. (Ita) [0:1] (1:6,4:4)"
        "Set 3Trungelliti M. (Arg) - Budkov Kjaer N. (Nor) [1:1] (6:4,4:6,2:2)"
        "Set 1Gorgodze E. (Geo) - Bertea E. R. (Rou) [0:0] (4:2)"
        "LiveIndia W - South Korea W [0:1]"

    Returns dict with:
        current_set: int (which set is being played)
        sets_p1: int (completed sets won by player 1)
        sets_p2: int (completed sets won by player 2)
        player1: str (name, lowercased, no country)
        player2: str (name, lowercased, no country)
        game_scores: list of tuples [(g1, g2), ...] per set
        raw: str (original line)

    Returns None if line can't be parsed.
    """
    line = line.strip()
    if not line:
        return None

    # Extract current set indicator
    current_set = 0
    set_match = re.match(r"Set\s*(\d+)", line)
    if set_match:
        current_set = int(set_match.group(1))
        line = line[set_match.end():]
    elif line.startswith("Live"):
        # Team matches like "LiveIndia W - South Korea W [0:1]" — skip these
        return None
    elif line.startswith("Tiebreak"):
        # "TiebreakBencic B./Golubic V. - ..." — handle tiebreak indicator
        line = re.sub(r"^Tiebreak\s*", "", line)
    else:
        # Lines without "Set N" prefix are finished or not started — skip
        return None

    if current_set == 0:
        return None

    # Remove image tags that appear in the raw text
    line = re.sub(r"!\[.*?\]\(.*?\)", "", line).strip()

    # Extract sets won: either "[X:Y]" (old format) or bare "X:Y" right
    # before the games list "(g:g,g:g,...)" (current flashscore.mobi format,
    # observed 2026-04-16).  Examples both accepted:
    #   "Sinner J. (Ita) [0:1] (1:6,4:4)"   ← old
    #   "Riedi L. (Sui) 0:1 (6:7,2:3)"      ← new
    # Use a lookahead to anchor against the games parenthesis and ensure
    # we don't accidentally match digits inside a country code paren.
    sets_match = re.search(r"\[?(\d+):(\d+)\]?(?=\s*\([0-9])", line)
    if not sets_match:
        return None

    sets_p1 = int(sets_match.group(1))
    sets_p2 = int(sets_match.group(2))

    # Extract game scores per set: (6:4,4:6,2:2)
    game_scores = []
    games_match = re.search(r"\(([0-9:,]+)\)", line)
    if games_match:
        for part in games_match.group(1).split(","):
            gs = part.split(":")
            if len(gs) == 2:
                try:
                    game_scores.append((int(gs[0]), int(gs[1])))
                except ValueError:
                    pass

    # Extract player names — everything before [X:Y]
    names_part = line[:sets_match.start()].strip()

    # Split on " - " (the vs separator on flashscore.mobi)
    players = re.split(r"\s+-\s+", names_part, maxsplit=1)
    if len(players) != 2:
        return None

    # Clean player names: remove country codes like "(Ger)", "(Ita)"
    def clean_name(n: str) -> str:
        n = re.sub(r"\([A-Za-z]{2,4}\)", "", n).strip()
        n = re.sub(r"\s+", " ", n)
        return _ascii_lower(n)

    player1 = clean_name(players[0])
    player2 = clean_name(players[1])

    if not player1 or not player2:
        return None

    return {
        "current_set": current_set,
        "sets_p1": sets_p1,
        "sets_p2": sets_p2,
        "player1": player1,
        "player2": player2,
        "game_scores": game_scores,
        "point_score": None,  # (p1_points, p2_points) — filled later from match page
        "match_url": None,    # flashscore match URL — filled from page parse
        "raw": line,
    }


def _parse_point_score(text: str) -> tuple | None:
    """
    Parse tennis point score from text.
    Handles: "30:15", "40:AD", "AD:40", "0:0", "15:0" etc.
    Returns (p1_points, p2_points) as strings, or None.
    """
    # Standard point scores
    m = re.search(r"\b(0|15|30|40|AD|A)\s*[-:]\s*(0|15|30|40|AD|A)\b", text, re.IGNORECASE)
    if m:
        return (m.group(1).upper(), m.group(2).upper())
    return None


def _point_to_number(p: str) -> int:
    """Convert point string to a sortable number for comparison."""
    mapping = {"0": 0, "15": 1, "30": 2, "40": 3, "AD": 4, "A": 4}
    return mapping.get(p.upper(), 0)


def _total_points_value(p1: str, p2: str) -> int:
    """Total 'progress' of points in a game. Higher = further along."""
    return _point_to_number(p1) + _point_to_number(p2)


async def fetch_flashscore_point_scores(browser_context, matches: list[dict]) -> list[dict]:
    """
    For each Flashscore match, open its detail page to get the current
    point score (e.g. 30-15) within the current game.

    The listing page only shows game scores (6:4, 3:2) but NOT point scores.
    The match detail page shows the live point score.

    Updates each match dict in-place with 'point_score' field.
    """
    MAX_CONCURRENT = 3

    async def _fetch_one(match: dict) -> None:
        if not match.get("match_url"):
            return

        page = await browser_context.new_page()
        try:
            await page.goto(match["match_url"], wait_until="domcontentloaded", timeout=15_000)
            await page.wait_for_timeout(2_000)

            point_data = await page.evaluate("""
                () => {
                    const text = (document.body.innerText || '');
                    // Look for point score patterns like "30 - 15", "40 - AD", "0 - 0"
                    // Flashscore shows the current game point score prominently
                    const pointPatterns = [
                        /\\b(0|15|30|40|AD|A)\\s*[-:]\\s*(0|15|30|40|AD|A)\\b/gi
                    ];

                    for (const pat of pointPatterns) {
                        const m = text.match(pat);
                        if (m) {
                            // Return the LAST match (most likely the current point score)
                            const last = m[m.length - 1];
                            const parts = last.split(/[-:]/);
                            if (parts.length === 2) {
                                return {
                                    p1: parts[0].trim(),
                                    p2: parts[1].trim(),
                                };
                            }
                        }
                    }
                    return null;
                }
            """)

            if point_data:
                match["point_score"] = (point_data["p1"].upper(), point_data["p2"].upper())

        except Exception as exc:
            print(f"[delay] Flashscore point fetch error: {exc}")
        finally:
            await page.close()

    # Only fetch point scores for matches that we've matched to Dafabet
    # (caller should filter before calling this)
    for i in range(0, len(matches), MAX_CONCURRENT):
        batch = matches[i:i + MAX_CONCURRENT]
        await asyncio.gather(*[_fetch_one(m) for m in batch])

    return matches


async def fetch_flashscore_live(page) -> list[dict]:
    """
    Navigate to flashscore.mobi LIVE tennis page and parse all live matches.

    Args:
        page: Playwright page object (can be a dedicated, persistent tab)

    Returns:
        List of parsed match dicts from parse_flashscore_line()
        Each dict also gets a 'match_url' field for fetching point scores later.
    """
    try:
        # Reuse already-loaded page when possible (saves a full navigation):
        # if we're already on the live URL, just reload; otherwise goto once.
        current_url = ""
        try:
            current_url = page.url or ""
        except Exception:
            current_url = ""

        if FLASHSCORE_LIVE_URL in current_url:
            await page.reload(wait_until="domcontentloaded", timeout=20_000)
        else:
            await page.goto(FLASHSCORE_LIVE_URL, wait_until="domcontentloaded", timeout=20_000)
        # flashscore.mobi is a static HTML page — 1s is enough post-DOMContentLoaded
        await page.wait_for_timeout(1_000)

        # Get match lines AND their URLs
        raw_data = await page.evaluate("""
            () => {
                const results = [];
                const body = document.body.innerText || '';

                // First collect text lines
                const lines = [];
                for (const line of body.split('\\n')) {
                    const trimmed = line.trim();
                    if (trimmed && (trimmed.startsWith('Set ') || trimmed.startsWith('Tiebreak'))) {
                        lines.push(trimmed);
                    }
                }

                // Also collect match URLs from links
                const urls = [];
                for (const a of document.querySelectorAll('a[href*="/match/"]')) {
                    urls.push(a.href);
                }

                return { lines, urls };
            }
        """)

        matches = []
        urls = raw_data.get("urls", [])

        for idx, line in enumerate(raw_data.get("lines", [])):
            parsed = parse_flashscore_line(line)
            if parsed:
                # Try to pair with a URL (they appear in order)
                if idx < len(urls):
                    parsed["match_url"] = urls[idx]
                matches.append(parsed)

        return matches

    except Exception as exc:
        print(f"[delay] Flashscore fetch error: {exc}")
        return []


# ── Player name matching (Dafabet ↔ Flashscore) ────────────────────

def _extract_surname(name: str) -> str:
    """
    Extract surname from various formats:
        "Butvilas, Edas"    → "butvilas"
        "Butvilas, E"       → "butvilas"
        "Shimizu Y"         → "shimizu"
        "Zverev A."         → "zverev"      (flashscore format)
    """
    name = _ascii_lower(name.strip())
    name = name.replace(".", "")

    if "," in name:
        return name.split(",")[0].strip()

    # No comma — surname is the first token (for both Dafabet and Flashscore)
    tokens = name.split()
    if tokens:
        return tokens[0]
    return name


def _extract_initial(name: str) -> str:
    """Extract first initial from various formats."""
    name = _ascii_lower(name.strip())
    name = name.replace(".", "")

    if "," in name:
        parts = name.split(",", 1)
        rest = parts[1].strip()
        if rest:
            return rest[0]
        return ""

    tokens = name.split()
    if len(tokens) >= 2:
        return tokens[-1][0] if len(tokens[-1]) <= 2 else tokens[1][0]
    return ""


def cross_platform_player_similarity(dafabet_name: str, flashscore_name: str) -> float:
    """
    Compare a Dafabet player name to a Flashscore player name.
    LOOSELY — we want to catch similar names across platforms even if
    spelling, transliteration, or abbreviation differs.

    Dafabet formats:  "Butvilas, Edas" or "Butvilas, E" or "Shimizu Y"
    Flashscore format: "zverev a" or "sinner j" (after cleaning)

    Returns 0.0–1.0 similarity.
    """
    da = _ascii_lower(dafabet_name.strip()).replace(".", "").replace(",", " ")
    fs = _ascii_lower(flashscore_name.strip()).replace(".", "").replace(",", " ")

    # Whole-string fuzzy as a baseline (catches transliteration, reordering)
    whole_sim = _fuzzy(da, fs)

    # Also try structured surname + initial comparison
    surname_d = _extract_surname(dafabet_name)
    surname_f = _extract_surname(flashscore_name)
    initial_d = _extract_initial(dafabet_name)
    initial_f = _extract_initial(flashscore_name)

    surname_sim = _fuzzy(surname_d, surname_f)

    structured_score = 0.0
    if surname_sim >= 0.65:
        # Surname is similar enough — check initials as a bonus, not a gate
        if initial_d and initial_f and initial_d == initial_f:
            structured_score = max(0.90, surname_sim)
        elif initial_d and initial_f and initial_d != initial_f:
            # Different initials — still allow if surname is very strong match
            # (could be transliteration issue with first name)
            structured_score = surname_sim * 0.55
        else:
            # Missing initial on one side — just use surname similarity
            structured_score = surname_sim * 0.85

    # Take the best of whole-string or structured approach
    return max(whole_sim, structured_score)


def match_dafabet_to_flashscore(
    dafabet_entry: dict,
    flashscore_matches: list[dict],
    threshold: float = 0.55,
) -> dict | None:
    """
    Find the best Flashscore match for a given Dafabet entry.
    Uses loose matching — similar names are enough, doesn't need exact.

    Args:
        dafabet_entry: {"home": "Butvilas, Edas", "away": "Imamura, Masamichi", ...}
        flashscore_matches: list of parsed Flashscore dicts
        threshold: minimum average similarity to accept a match (low = more matches)

    Returns:
        The best matching Flashscore dict, or None if no match found.
    """
    dafa_home = dafabet_entry["home"]
    dafa_away = dafabet_entry["away"]

    # Skip doubles matches (contain "/" in names)
    if "/" in dafa_home or "/" in dafa_away:
        return None

    best_match = None
    best_score = 0.0

    for fs in flashscore_matches:
        # Also skip doubles from Flashscore side
        if "/" in fs["player1"] or "/" in fs["player2"]:
            continue

        # Try normal order: home↔p1, away↔p2
        sim_h1 = cross_platform_player_similarity(dafa_home, fs["player1"])
        sim_a2 = cross_platform_player_similarity(dafa_away, fs["player2"])
        score_normal = (sim_h1 + sim_a2) / 2

        # Try reversed order: home↔p2, away↔p1
        sim_h2 = cross_platform_player_similarity(dafa_home, fs["player2"])
        sim_a1 = cross_platform_player_similarity(dafa_away, fs["player1"])
        score_reversed = (sim_h2 + sim_a1) / 2

        score = max(score_normal, score_reversed)
        min_side = min(sim_h1, sim_a2) if score_normal >= score_reversed else min(sim_h2, sim_a1)

        # Loose floor — even one side matching well is enough
        if score > best_score and min_side >= 0.40:
            best_score = score
            best_match = fs

    if best_score >= threshold:
        print(f"[delay] Matched: '{dafa_home} vs {dafa_away}' ↔ "
              f"'{best_match['player1']} vs {best_match['player2']}' "
              f"(sim={best_score:.2f})")
        return best_match

    # Log near-misses for debugging
    if best_match and best_score >= 0.40:
        print(f"[delay] Near miss: '{dafa_home} vs {dafa_away}' ↔ "
              f"'{best_match['player1']} vs {best_match['player2']}' "
              f"(sim={best_score:.2f}, threshold={threshold})")

    return None


# ── Score parsing from Dafabet ──────────────────────────────────────

def parse_dafabet_score(entry: dict) -> dict:
    """
    Extract set information from a Dafabet match entry.

    The Dafabet scraper stores score info in the page text during
    investigation. For the listing page, we need to extract from
    whatever score data is visible.

    We'll extract this from the match page if needed, but for now
    we parse the score text that the existing extract_matches() captures.

    Returns:
        {
            "total_sets": int,     # total completed sets
            "current_set": int,    # which set is being played (1-based)
            "sets_home": int,      # sets won by home player
            "sets_away": int,      # sets won by away player
            "raw_score": str,      # raw score string if available
        }
    """
    # The existing code extracts score as "X-Y" from the match page
    # On the listing page, we need to parse from page text
    # For now, return what we can from the entry
    score_text = entry.get("score_text", "")
    sets_text = entry.get("sets_text", "")

    result = {
        "total_sets": 0,
        "current_set": 1,
        "sets_home": 0,
        "sets_away": 0,
        "raw_score": score_text,
    }

    # Try to parse "X - Y" sets score
    sets_match = re.match(r"(\d+)\s*[-–]\s*(\d+)", score_text)
    if sets_match:
        result["sets_home"] = int(sets_match.group(1))
        result["sets_away"] = int(sets_match.group(2))
        result["total_sets"] = result["sets_home"] + result["sets_away"]
        result["current_set"] = result["total_sets"] + 1

    return result


# ── Delay detection logic ───────────────────────────────────────────

# Minimum game difference to trigger an alert when both are in the same set
GAME_DELAY_THRESHOLD: int = 2  # e.g. Flashscore 5-3 vs Dafabet 3-2 = 3 games ahead

# Minimum point progress difference to trigger a POINT_DELAY alert
# Point values: 0=0, 15=1, 30=2, 40=3, AD=4
# A diff of 2 means e.g. Flashscore at 40-0 (value 3) vs Dafabet at 15-0 (value 1)
POINT_DELAY_THRESHOLD: int = 2

# Minimum games played in Flashscore's NEW set before we fire a SET_DELAY.
# Dafabet commonly trails by ~10-60s when rendering the header of a new set;
# by the time a human clicks the link, the two pages already agree. Require
# the new set to have advanced by at least N games so the window stays open
# long enough to verify. Set to 0 to restore old behaviour.
SET_DELAY_MIN_NEW_SET_GAMES: int = 3


def _total_games(game_scores: list[tuple]) -> int:
    """Sum all games played across all sets."""
    return sum(g[0] + g[1] for g in game_scores)


def _current_set_games(game_scores: list[tuple]) -> tuple[int, int]:
    """Return (p1_games, p2_games) for the last (current) set."""
    if game_scores:
        return game_scores[-1]
    return (0, 0)


def detect_delay(
    dafabet_current_set: int,
    dafabet_games: list[tuple],  # [(g1,g2), ...] per set from Dafabet
    flashscore_current_set: int,
    flashscore_games: list[tuple],  # [(g1,g2), ...] per set from Flashscore
    dafabet_sets_won: tuple = (0, 0),  # (home_sets, away_sets)
    flashscore_sets_won: tuple = (0, 0),
    dafabet_points: tuple | None = None,    # ("30", "15") or None
    flashscore_points: tuple | None = None,  # ("40", "0") or None
) -> dict | None:
    """
    Compare score progress between Dafabet and Flashscore.
    Checks THREE levels: set, game, AND point delays.

    Returns dict with delay info if delay detected, None otherwise.

    Delay types (checked in priority order):
        SET_DELAY   — Flashscore is 1+ sets ahead
        GAME_DELAY  — Same set, but Flashscore is N+ games ahead
        POINT_DELAY — Same set & game, but Flashscore points are ahead
    """
    # ── Set-level delay ──
    set_diff = flashscore_current_set - dafabet_current_set
    if set_diff >= 1:
        # False-positive guard: Dafabet often lags 10-60s when rendering
        # the header of a brand-new set. In practice the user can't click
        # the alert fast enough to actually see a discrepancy. Require
        # Flashscore's new set to have advanced by at least
        # SET_DELAY_MIN_NEW_SET_GAMES games (default 3) before firing.
        #   Dafa [7-6]  FS [7-6, 0-0]  → skip (0 games into new set)
        #   Dafa [7-6]  FS [7-6, 0-1]  → skip (1 game, still too short)
        #   Dafa [7-6]  FS [7-6, 2-1]  → alert (3 games into new set)
        fs_new_set = _current_set_games(flashscore_games)
        fs_new_set_games_played = fs_new_set[0] + fs_new_set[1]
        if fs_new_set_games_played < SET_DELAY_MIN_NEW_SET_GAMES:
            return None
        return {
            "type": "SET_DELAY",
            "set_diff": set_diff,
            "game_diff": 0,
            "point_diff": 0,
            "dafabet_set": dafabet_current_set,
            "flashscore_set": flashscore_current_set,
            "dafabet_sets_won": dafabet_sets_won,
            "flashscore_sets_won": flashscore_sets_won,
            "dafabet_games": dafabet_games,
            "flashscore_games": flashscore_games,
        }

    # ── Game-level delay (same set) ──
    if dafabet_current_set == flashscore_current_set and dafabet_current_set > 0:
        fs_g1, fs_g2 = _current_set_games(flashscore_games)
        da_g1, da_g2 = _current_set_games(dafabet_games)

        fs_total_in_set = fs_g1 + fs_g2
        da_total_in_set = da_g1 + da_g2
        game_diff = fs_total_in_set - da_total_in_set

        if game_diff >= GAME_DELAY_THRESHOLD:
            return {
                "type": "GAME_DELAY",
                "set_diff": 0,
                "game_diff": game_diff,
                "point_diff": 0,
                "dafabet_set": dafabet_current_set,
                "flashscore_set": flashscore_current_set,
                "dafabet_sets_won": dafabet_sets_won,
                "flashscore_sets_won": flashscore_sets_won,
                "dafabet_games": dafabet_games,
                "flashscore_games": flashscore_games,
                "dafabet_current_game": (da_g1, da_g2),
                "flashscore_current_game": (fs_g1, fs_g2),
            }

        # ── Point-level delay (same set AND same game) ──
        if game_diff == 0 and dafabet_points and flashscore_points:
            da_point_val = _total_points_value(dafabet_points[0], dafabet_points[1])
            fs_point_val = _total_points_value(flashscore_points[0], flashscore_points[1])
            point_diff = fs_point_val - da_point_val

            if point_diff >= POINT_DELAY_THRESHOLD:
                return {
                    "type": "POINT_DELAY",
                    "set_diff": 0,
                    "game_diff": 0,
                    "point_diff": point_diff,
                    "dafabet_set": dafabet_current_set,
                    "flashscore_set": flashscore_current_set,
                    "dafabet_sets_won": dafabet_sets_won,
                    "flashscore_sets_won": flashscore_sets_won,
                    "dafabet_games": dafabet_games,
                    "flashscore_games": flashscore_games,
                    "dafabet_current_game": (da_g1, da_g2),
                    "flashscore_current_game": (fs_g1, fs_g2),
                    "dafabet_points": dafabet_points,
                    "flashscore_points": flashscore_points,
                }

    return None


# ── Main integration function ──────────────────────────────────────

async def extract_dafabet_scores(browser_context, entries: list[dict]) -> list[dict]:
    """
    For each live Dafabet match, open the match page in a new tab and
    extract the detailed scoreboard: sets won + game scores per set.

    This gives us game-level granularity like:
        Set 1: 6-4
        Set 2: 3-5  (current)

    We open matches in batches to limit memory usage.
    """
    if not entries:
        return entries

    MAX_CONCURRENT = 3  # limit open tabs

    async def _extract_one(entry: dict) -> None:
        """Open one match page and parse scores into the entry dict."""
        page = await browser_context.new_page()
        try:
            await page.goto(entry["url"], wait_until="domcontentloaded", timeout=20_000)
            await page.wait_for_timeout(2_500)

            score_info = await page.evaluate(r"""
                () => {
                    // ── Primary source: the scoreboard <table> ───────────────
                    // Dafabet match pages render the actual scoreboard as
                    //   <table class="label-small text-center">
                    //     <tr><td>Sets</td><td>1</td><td>2</td><td>3</td></tr>
                    //     <tr><td>1</td><td>6</td><td>4</td><td>2</td></tr>  ← home
                    //     <tr><td>1</td><td>3</td><td>6</td><td>2</td></tr>  ← away
                    // The first cell in each player row is the sets-won count;
                    // every following cell is games in that set. The set
                    // numbers in the header row tell us which set is current
                    // (the LAST column). Confirmed DOM 2026-04-16.
                    const table = document.querySelector('table.label-small.text-center');

                    let setsHome = 0, setsAway = 0;
                    let currentSet = 0;
                    const gameScores = [];   // [[h, a], [h, a], ...] per set
                    let tableFound = false;

                    if (table) {
                        const rows = [...table.querySelectorAll('tr')].map(tr =>
                            [...tr.children].map(c => (c.innerText || '').trim())
                        );
                        // Need header + 2 player rows
                        if (rows.length >= 3) {
                            const header = rows[0];
                            const home = rows[1];
                            const away = rows[2];

                            // Header cells after the "Sets" label are the set
                            // numbers (1..N). currentSet = max of those.
                            for (let i = 1; i < header.length; i++) {
                                const n = parseInt(header[i]);
                                if (Number.isFinite(n) && n > currentSet) currentSet = n;
                            }

                            const homeNums = home.map(v => parseInt(v));
                            const awayNums = away.map(v => parseInt(v));
                            // First cell = sets-won. Treat NaN as 0.
                            setsHome = Number.isFinite(homeNums[0]) ? homeNums[0] : 0;
                            setsAway = Number.isFinite(awayNums[0]) ? awayNums[0] : 0;

                            // Remaining cells = games per set. Clamp at tennis-
                            // realistic 0..20 (tiebreak can push >7 for a brief
                            // moment). Anything bigger means the cell text is
                            // not actually a game count.
                            const nSets = Math.min(homeNums.length, awayNums.length) - 1;
                            for (let i = 0; i < nSets; i++) {
                                const h = homeNums[i + 1];
                                const a = awayNums[i + 1];
                                if (Number.isFinite(h) && Number.isFinite(a) &&
                                    h >= 0 && h <= 20 && a >= 0 && a <= 20) {
                                    gameScores.push([h, a]);
                                }
                            }
                            tableFound = true;
                        }
                    }

                    if (!currentSet) currentSet = 1;

                    // ── Point score (within the current game) ────────────────
                    // Dafabet shows each player's point total as its own token
                    // right next to their name:
                    //     Shimabukuro, Sho
                    //     40
                    //     Zhukayev, Beibit
                    //     40
                    // Just before the scoreboard table. We walk body.innerText
                    // in order, find the two consecutive single-point tokens
                    // that precede the "Sets\t..." row.
                    const text = (document.body.innerText || '');
                    const lines = text.split('\n').map(l => l.trim());

                    // Locate the "Sets\t1\t2..." line (tabs inside the table
                    // collapse into a single string when read through innerText).
                    let setsLineIdx = -1;
                    for (let i = 0; i < lines.length; i++) {
                        if (/^Sets(\s|$)/.test(lines[i])) { setsLineIdx = i; break; }
                    }

                    let pointHome = null, pointAway = null;
                    if (setsLineIdx > 0) {
                        // Walk backwards collecting point tokens; expect the
                        // two most-recent ones (ignoring blank lines) to be
                        // away then home (reverse order).
                        const ptRe = /^(0|15|30|40|AD|A)$/i;
                        const collected = [];
                        for (let i = setsLineIdx - 1; i >= 0 && collected.length < 2; i--) {
                            const v = lines[i];
                            if (!v) continue;
                            if (ptRe.test(v)) collected.push(v.toUpperCase());
                            else if (collected.length > 0) break;  // non-point before we got both
                        }
                        if (collected.length === 2) {
                            // collected[0] = away (closer to table), collected[1] = home
                            pointAway = collected[0];
                            pointHome = collected[1];
                        }
                    }

                    return {
                        sets_home: setsHome,
                        sets_away: setsAway,
                        current_set: currentSet,
                        game_scores: gameScores,
                        table_found: tableFound,
                        point_home: pointHome,
                        point_away: pointAway,
                        page_text: text.substring(0, 800),
                    };
                }
            """)

            entry["sets_home"]   = score_info["sets_home"]
            entry["sets_away"]   = score_info["sets_away"]
            entry["current_set"] = score_info["current_set"]
            entry["page_text"]   = score_info.get("page_text", "")
            entry["game_scores"] = [tuple(gs) for gs in score_info.get("game_scores", [])]

            # Point score within current game
            ph = score_info.get("point_home")
            pa = score_info.get("point_away")
            entry["point_score"] = (ph, pa) if ph and pa else None

            if not score_info.get("table_found"):
                # Scoreboard table missing (match just starting, or page
                # still loading). Leave caller's downstream "implausible
                # score" guard to suppress alerts rather than risk noise.
                print(f"[delay] Dafabet scoreboard table missing for "
                      f"{entry['home']} vs {entry['away']} — "
                      f"will retry next cycle.")

            print(f"[delay] Dafabet score for {entry['home']} vs {entry['away']}: "
                  f"sets={entry['sets_home']}-{entry['sets_away']} "
                  f"games={entry['game_scores']} set={entry['current_set']} "
                  f"points={entry.get('point_score', 'N/A')}")

        except Exception as exc:
            print(f"[delay] Error extracting score from {entry['url']}: {exc}")
            entry["sets_home"] = 0
            entry["sets_away"] = 0
            entry["current_set"] = 1
            entry["game_scores"] = []
        finally:
            await page.close()

    # Process in batches to limit concurrent tabs
    for i in range(0, len(entries), MAX_CONCURRENT):
        batch = entries[i:i + MAX_CONCURRENT]
        await asyncio.gather(*[_extract_one(e) for e in batch])

    return entries


async def check_score_delays(
    browser_context,
    dafabet_entries: list[dict],
    alerted_delays: set | None = None,
    fs_page=None,
    point_detail: bool = False,
) -> list[dict]:
    """
    Main delay detection function. Called once per polling cycle.
    Compares BOTH set-level AND game-level scores.

    Args:
        browser_context: Playwright browser context (to open Flashscore tab)
        dafabet_entries: list of live Dafabet match entries (with score data)
        alerted_delays: set of alert keys already sent (avoids re-sending)

    Returns:
        List of delay alert dicts with alert_msg for Telegram.
    """
    if alerted_delays is None:
        alerted_delays = set()

    if not dafabet_entries:
        return []

    # Prefer the persistent Flashscore tab when provided (lightweight path).
    # Fall back to opening a throwaway tab only if no persistent page given.
    if fs_page is not None:
        flashscore_matches = await fetch_flashscore_live(fs_page)
    else:
        tmp_page = await browser_context.new_page()
        try:
            flashscore_matches = await fetch_flashscore_live(tmp_page)
        finally:
            await tmp_page.close()

    if not flashscore_matches:
        print("[delay] No live matches found on Flashscore.")
        return []

    print(f"[delay] Flashscore: {len(flashscore_matches)} live match(es), "
          f"Dafabet: {len(dafabet_entries)} live match(es)")

    alerts = []

    for dafa in dafabet_entries:
        # Skip doubles
        if "/" in dafa.get("home", "") or "/" in dafa.get("away", ""):
            continue

        # Find matching Flashscore entry
        fs_match = match_dafabet_to_flashscore(dafa, flashscore_matches)
        if not fs_match:
            continue

        # Get Dafabet data
        dafa_current = dafa.get("current_set", 1)
        dafa_games = dafa.get("game_scores", [])  # [(g1,g2), ...] per set
        dafa_sets = (dafa.get("sets_home", 0), dafa.get("sets_away", 0))
        dafa_points = dafa.get("point_score")  # ("30", "15") or None

        # Guard: if Dafabet's scoreboard didn't populate (empty game_scores),
        # we can't meaningfully compare. These produce "Games: [N/A]" alerts
        # that are false positives — the Dafabet page just hadn't rendered
        # yet or the scraper failed for this match. Skip silently. Matches
        # the same guard used in `check_bwin_delays`.
        if not dafa_games:
            print(f"[delay] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet game scores unavailable (N/A); cannot compare.")
            continue
        # Also reject implausible Dafabet states (same sanity floor as bwin).
        if dafa_sets[0] > 5 or dafa_sets[1] > 5 or dafa_current > 5:
            print(f"[delay] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet score implausible (sets={dafa_sets}, "
                  f"set={dafa_current}); likely parse error upstream.")
            continue
        if any(
            (not isinstance(g, (tuple, list))) or len(g) < 2
            or g[0] > 20 or g[1] > 20 or g[0] < 0 or g[1] < 0
            for g in dafa_games
        ):
            print(f"[delay] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet game scores implausible ({dafa_games}).")
            continue

        # Get Flashscore data
        fs_current = fs_match["current_set"]
        fs_games = fs_match["game_scores"]  # [(g1,g2), ...] per set
        fs_sets = (fs_match["sets_p1"], fs_match["sets_p2"])
        fs_points = fs_match.get("point_score")  # filled later if needed

        # For point-level comparison: if same set & same game, fetch
        # Flashscore point scores from the match detail page
        da_cg_total = sum(_current_set_games(dafa_games))
        fs_cg_total = sum(_current_set_games(fs_games))
        if (point_detail
                and dafa_current == fs_current and da_cg_total == fs_cg_total
                and dafa_points and not fs_points and fs_match.get("match_url")):
            # Need to fetch point score from Flashscore match page.
            # Gated behind point_detail flag because it opens an additional
            # per-match tab (expensive on small hardware).
            await fetch_flashscore_point_scores(browser_context, [fs_match])
            fs_points = fs_match.get("point_score")

        # Detect delay (set, game, or point level)
        delay = detect_delay(
            dafabet_current_set=dafa_current,
            dafabet_games=dafa_games,
            flashscore_current_set=fs_current,
            flashscore_games=fs_games,
            dafabet_sets_won=dafa_sets,
            flashscore_sets_won=fs_sets,
            dafabet_points=dafa_points,
            flashscore_points=fs_points,
        )

        if not delay:
            continue

        # For SET_DELAY specifically, require a high-confidence name match.
        # The loose 0.55 threshold used for pairing lets through wrong-player
        # matches (e.g. "Brockmann, T" paired with "balestrieri a." at 55%),
        # which would otherwise generate ghost alerts.
        if delay["type"] == "SET_DELAY":
            sim_h1 = cross_platform_player_similarity(dafa["home"], fs_match["player1"])
            sim_a2 = cross_platform_player_similarity(dafa["away"], fs_match["player2"])
            sim_h2 = cross_platform_player_similarity(dafa["home"], fs_match["player2"])
            sim_a1 = cross_platform_player_similarity(dafa["away"], fs_match["player1"])
            best_pair_sim = max((sim_h1 + sim_a2) / 2, (sim_h2 + sim_a1) / 2)
            if best_pair_sim < 0.75:
                print(f"[delay] Skipping SET_DELAY for {dafa['home']} vs {dafa['away']} "
                      f"— low name match ({best_pair_sim:.0%})")
                continue

        # Build alert key — include delay type + current scores to avoid re-alerting
        # but RE-alert if the delay grows
        if delay["type"] == "SET_DELAY":
            alert_key = (dafa["url"], "SET", fs_current)
        elif delay["type"] == "GAME_DELAY":
            fs_cg = delay.get("flashscore_current_game", (0, 0))
            alert_key = (dafa["url"], "GAME", fs_current, fs_cg[0] + fs_cg[1])
        else:  # POINT_DELAY
            fp = delay.get("flashscore_points", ("0", "0"))
            alert_key = (dafa["url"], "POINT", fs_current, da_cg_total,
                         _total_points_value(fp[0], fp[1]))

        if alert_key in alerted_delays:
            continue
        alerted_delays.add(alert_key)

        # Calculate name match similarity for display
        sim_h1 = cross_platform_player_similarity(dafa["home"], fs_match["player1"])
        sim_a2 = cross_platform_player_similarity(dafa["away"], fs_match["player2"])
        sim_h2 = cross_platform_player_similarity(dafa["home"], fs_match["player2"])
        sim_a1 = cross_platform_player_similarity(dafa["away"], fs_match["player1"])
        match_sim = max((sim_h1 + sim_a2) / 2, (sim_h2 + sim_a1) / 2)

        # Format score strings
        fs_score_str = ", ".join(
            f"{g[0]}-{g[1]}" for g in fs_games
        ) if fs_games else "N/A"
        da_score_str = ", ".join(
            f"{g[0]}-{g[1]}" for g in dafa_games
        ) if dafa_games else "N/A"

        # Build alert message based on delay type
        if delay["type"] == "SET_DELAY":
            delay_desc = f"Dafabet is <b>{delay['set_diff']} set(s) behind</b>"
            emoji = "⏱️"
        elif delay["type"] == "GAME_DELAY":
            da_cg = delay.get("dafabet_current_game", (0, 0))
            fs_cg = delay.get("flashscore_current_game", (0, 0))
            delay_desc = (
                f"Dafabet is <b>{delay['game_diff']} game(s) behind</b> in Set {fs_current}\n"
                f"Dafabet game: {da_cg[0]}-{da_cg[1]} | "
                f"Flashscore game: {fs_cg[0]}-{fs_cg[1]}"
            )
            emoji = "🎾"
        else:  # POINT_DELAY
            dp = delay.get("dafabet_points", ("?", "?"))
            fp = delay.get("flashscore_points", ("?", "?"))
            da_cg = delay.get("dafabet_current_game", (0, 0))
            delay_desc = (
                f"Dafabet is <b>{delay['point_diff']} point step(s) behind</b> "
                f"in Set {fs_current}, Game {da_cg[0]+da_cg[1]+1}\n"
                f"Dafabet points: <b>{dp[0]}-{dp[1]}</b> | "
                f"Flashscore points: <b>{fp[0]}-{fp[1]}</b>"
            )
            emoji = "🏓"

        alert_msg = (
            f"{emoji} <b>SCORE DELAY DETECTED! ({delay['type']})</b>\n\n"
            f"<b>Match:</b> {dafa['home']} vs {dafa['away']}\n\n"
            f"<b>Dafabet:</b> Set {dafa_current} "
            f"(Sets: {dafa_sets[0]}-{dafa_sets[1]}) "
            f"Games: [{da_score_str}]"
            f"{' Points: ' + dafa_points[0] + '-' + dafa_points[1] if dafa_points else ''}\n"
            f"<b>Flashscore:</b> Set {fs_current} "
            f"(Sets: {fs_sets[0]}-{fs_sets[1]}) "
            f"Games: [{fs_score_str}]"
            f"{' Points: ' + fs_points[0] + '-' + fs_points[1] if fs_points else ''}\n\n"
            f"<b>{delay_desc}</b>\n"
            f"<b>Name match:</b> {match_sim:.0%}\n\n"
            f"<b>Flashscore:</b> {fs_match['player1']} vs {fs_match['player2']}\n"
            f"<b>Dafabet link:</b> {dafa['url']}"
            f"{chr(10) + '<b>Flashscore link:</b> ' + fs_match['match_url'] if fs_match.get('match_url') else ''}"
        )

        alerts.append({
            "alert_msg": alert_msg,
            "dafabet_entry": dafa,
            "flashscore_match": fs_match,
            "delay_info": delay,
        })

        print(f"[delay] {emoji} {delay['type']}: {dafa['home']} vs {dafa['away']} — "
              f"Dafabet Set {dafa_current} [{da_score_str}] vs "
              f"Flashscore Set {fs_current} [{fs_score_str}]")

    return alerts


# ═══════════════════════════════════════════════════════════════════
#  BWIN reference source
# ═══════════════════════════════════════════════════════════════════
#
# Mirrors the Flashscore.mobi pipeline but uses bwin.com as the reference
# clock for detecting Dafabet lag. bwin's live page uses a WebSocket
# (wss://cds-push.bwin.com) to keep the DOM continuously fresh — so we keep
# ONE persistent page open across the whole session and just re-read the DOM
# each cycle. No reloads needed.
#
# Two-cycle debounce (user requirement): a delay must be observed on TWO
# consecutive polling cycles for the same Dafabet match before it fires an
# alert. First-cycle observations sit in `bwin_delay_pending`; if the delay
# is still present on the next cycle → alert.
#
# Only SET_DELAY and GAME_DELAY are honoured from the bwin source (point-level
# noise is suppressed per design decision — sub-game flicker is normal).
# ═══════════════════════════════════════════════════════════════════

BWIN_LIVE_URL = "https://www.bwin.com/en/sports/live/tennis-5"
BWIN_EVENT_BASE = "https://www.bwin.com"


async def fetch_bwin_live(page) -> list[dict]:
    """
    Read the current bwin live tennis DOM and return parsed match states.

    Accepts a *persistent* page — if the page is already on BWIN_LIVE_URL,
    no navigation happens; we just re-read the DOM (the bwin WebSocket keeps
    it live). First call navigates; subsequent calls just re-read.

    Returned shape mirrors `fetch_flashscore_live` so `detect_delay` works
    unchanged:
        {
            "player1":       "James Trotter",
            "player2":       "Jake Delaney",
            "country1":      "JPN",
            "country2":      "AUS",
            "current_set":   3,
            "sets_p1":       1,
            "sets_p2":       1,
            "game_scores":   [(4, 0)],      # current set only (bwin listing
                                            # exposes only the current set)
            "point_score":   ("15", "0"),   # or None
            "match_url":     "https://www.bwin.com/en/sports/events/...",
            "event_id":      "19376970",
            "raw_text":      "<innerText dump>",
        }
    """
    try:
        if BWIN_LIVE_URL not in (page.url or ""):
            await page.goto(BWIN_LIVE_URL, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(8_000)
            # Dismiss cookie banner on first load
            for label in ("Allow All", "Accept All", "Accept"):
                try:
                    btn = page.get_by_role("button", name=label)
                    if await btn.count():
                        await btn.first.click(timeout=2_000)
                        break
                except Exception:
                    pass

            # bwin's SPA lazy-loads the live event list — scroll to the
            # bottom and back to trigger rendering of all ms-event nodes,
            # then wait for the count to stabilise. First-fetch only.
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1_500)
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(1_500)

                prev_count = -1
                for _ in range(6):  # up to ~9s of stabilisation polling
                    cur_count = await page.locator("ms-event").count()
                    if cur_count == prev_count and cur_count > 0:
                        break
                    prev_count = cur_count
                    await page.wait_for_timeout(1_500)
            except Exception as exc:
                print(f"[bwin] hydration wait failed (continuing): {exc}")
        else:
            # Persistent page — just give the WebSocket a brief moment to
            # settle any in-flight DOM updates before we read.
            await page.wait_for_timeout(500)

        records = await page.eval_on_selector_all(
            "ms-event",
            """
            els => els.map(el => {
                const a = el.querySelector('a[href*="/sports/events/"]');
                const href = a ? a.getAttribute('href') : '';
                const participants = [...el.querySelectorAll('.participant')];
                const parsePart = p => {
                    const cc = p.querySelector('.participant-country');
                    const ccText = cc ? cc.innerText.trim() : '';
                    let name = (p.innerText || '').trim();
                    if (ccText) {
                        // Remove the CC text (span sits inline inside .participant)
                        name = name.replace(ccText, '').trim();
                    }
                    return {name, cc: ccText};
                };
                const p1 = participants[0] ? parsePart(participants[0]) : {name:'', cc:''};
                const p2 = participants[1] ? parsePart(participants[1]) : {name:'', cc:''};
                return {
                    href: href,
                    player1: p1.name,
                    player2: p2.name,
                    cc1: p1.cc,
                    cc2: p2.cc,
                    text: el.innerText || '',
                };
            })
            """
        )
    except Exception as exc:
        print(f"[bwin] fetch error: {exc}")
        return []

    matches: list[dict] = []
    for r in records:
        parsed = _parse_bwin_event(r)
        if parsed is not None:
            matches.append(parsed)
    return matches


def _parse_bwin_event(raw: dict) -> dict | None:
    """
    Parse a single ms-event's innerText + href into a structured dict.

    bwin innerText layout (confirmed from probe — all newline-separated after
    split, in source order):

        James Trotter JPN          (name + country on one line)
        Jake Delaney AUS
        LIVE                        (status)
        3rd Set                     (current-set label)
        15                          (point-home)
        0                           (point-away)
        P                           (label)
        4                           (games-home in current set)
        0                           (games-away in current set)
        G                           (label)
        1                           (sets-won-home)
        1                           (sets-won-away)
        Sets                        (label)
        ...odds rows...

    Strategy: walk backwards from labels "Sets", "G", "P" to pick the two
    preceding numeric lines. This is resilient to extra odds lines appearing
    before/after. Returns None if the match isn't LIVE.
    """
    text = raw.get("text", "") or ""
    if "LIVE" not in text:
        return None

    href = raw.get("href", "") or ""
    if not href:
        return None

    # Event ID: trailing digits in the slug, e.g. .../james-trotter-...-19376970
    m_id = re.search(r"(\d+)\s*$", href.rstrip("/"))
    event_id = m_id.group(1) if m_id else href

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def _find_label(label: str) -> int:
        for i, ln in enumerate(lines):
            if ln == label:
                return i
        return -1

    def _as_int(s: str, default: int = 0) -> int:
        try:
            return int(s)
        except Exception:
            return default

    sets_h = sets_a = 0
    games_h = games_a = 0
    point_home: str | None = None
    point_away: str | None = None

    idx_sets = _find_label("Sets")
    if idx_sets >= 2:
        sets_h = _as_int(lines[idx_sets - 2])
        sets_a = _as_int(lines[idx_sets - 1])

    idx_g = _find_label("G")
    if idx_g >= 2:
        games_h = _as_int(lines[idx_g - 2])
        games_a = _as_int(lines[idx_g - 1])

    idx_p = _find_label("P")
    if idx_p >= 2:
        ph = lines[idx_p - 2]
        pa = lines[idx_p - 1]
        if re.fullmatch(r"(0|15|30|40|A|AD)", ph, re.I):
            point_home = ph.upper().replace("A", "AD") if ph.upper() == "A" else ph.upper()
        if re.fullmatch(r"(0|15|30|40|A|AD)", pa, re.I):
            point_away = pa.upper().replace("A", "AD") if pa.upper() == "A" else pa.upper()

    # Current set from "Nth Set" label if present
    current_set = 0
    for ln in lines:
        mset = re.match(r"(\d+)\s*(st|nd|rd|th)\s*Set", ln, re.I)
        if mset:
            current_set = int(mset.group(1))
            break
    if current_set == 0:
        current_set = sets_h + sets_a + 1  # fallback

    bwin_url = href if href.startswith("http") else BWIN_EVENT_BASE + href

    return {
        "player1":     raw.get("player1", "").strip(),
        "player2":     raw.get("player2", "").strip(),
        "country1":    raw.get("cc1", "").strip(),
        "country2":    raw.get("cc2", "").strip(),
        "current_set": current_set,
        "sets_p1":     sets_h,
        "sets_p2":     sets_a,
        # bwin listing only exposes the current set — provide a one-element
        # list so detect_delay's _current_set_games() picks it up correctly.
        "game_scores": [(games_h, games_a)],
        "point_score": (point_home, point_away) if point_home and point_away else None,
        "match_url":   bwin_url,
        "event_id":    event_id,
        "raw_text":    text,
    }


def bwin_player_similarity(dafabet_name: str, bwin_name: str) -> float:
    """
    Similarity between a Dafabet name ('Surname, Given' / 'Surname I') and a
    bwin name ('Given Surname' / 'Given Middle Surname').

    The generic `cross_platform_player_similarity` fails here because its
    `_extract_surname()` assumes "first token = surname" for no-comma names
    — which is correct for Flashscore ("Zverev A.") but WRONG for bwin
    ("Marat Sharipov"), where the first token is the GIVEN name.

    Strategy:
      1. Extract the Dafabet surname (possibly compound, e.g. "Torner Sensano")
         and first-initial.
      2. Find each surname word as a fuzzy match against some bwin token.
      3. If every surname word is accounted for, require the Dafabet initial
         to match the first letter of some remaining bwin token — then 0.95.
         If initials differ → 0.50 (likely same surname, different player).
      4. If surname words can't all be located → fall back to whole-string
         fuzzy so near-spellings still get some credit.

    Handles the observed cases correctly:
      'Sharipov, Marat'          ↔ 'Marat Sharipov'              → 0.95
      'Torner Sensano, Neus'     ↔ 'Neus Torner Sensano'         → 0.95
      'Alexandrescou, Yannick T' ↔ 'Yannick Theodor Alexandrescou'→ 0.95
      'Funk, A'                  ↔ 'Aaron Funk'                  → 0.95
      'Kwon, Soon Woo'           ↔ 'Marat Sharipov'              → ~0.15
    """
    dafa_surname = _extract_surname(dafabet_name)
    dafa_initial = _extract_initial(dafabet_name)

    bw_clean = _ascii_lower(bwin_name).replace(".", "").replace(",", " ")
    bw_tokens = [t for t in bw_clean.split() if t]

    if not dafa_surname or not bw_tokens:
        return _fuzzy(
            _ascii_lower(dafabet_name).replace(",", " "),
            bw_clean,
        )

    # Multi-word surname: ALL words must be found as distinct bwin tokens.
    surname_words = dafa_surname.split()
    used: set[int] = set()
    for word in surname_words:
        best_i = -1
        best_sim = 0.0
        for i, tok in enumerate(bw_tokens):
            if i in used:
                continue
            sim = _fuzzy(word, tok)
            if sim > best_sim:
                best_sim = sim
                best_i = i
        if best_sim < 0.80 or best_i < 0:
            # Surname word not confidently located in bwin — fall back.
            return max(
                _fuzzy(
                    _ascii_lower(dafabet_name).replace(",", " "),
                    bw_clean,
                ),
                best_sim * 0.55,
            )
        used.add(best_i)

    # Every surname word matched in bwin. Check the first-name initial against
    # the unmatched bwin tokens (which should be given names / middle names).
    unmatched = [t for i, t in enumerate(bw_tokens) if i not in used and t]

    if not dafa_initial:
        # No initial to verify — strong surname match alone is still good.
        return 0.88

    if not unmatched:
        # Bwin had nothing but the surname tokens — unusual, give moderate score.
        return 0.80

    given_initials = {t[0] for t in unmatched if t}
    if dafa_initial in given_initials:
        return 0.95  # surname + initial confirmed
    return 0.50      # same surname, different initial = likely different player


def match_dafabet_to_bwin(
    dafabet_entry: dict,
    bwin_matches:  list[dict],
    threshold:     float = 0.75,
) -> dict | None:
    """
    Find the best bwin match for a Dafabet entry using `bwin_player_similarity`
    (bwin-aware; handles "Given Surname" token order).

    Threshold 0.75 is calibrated against the stronger similarity signal —
    confirmed surname + matching initial scores 0.95, so 0.75 keeps comfortable
    headroom while rejecting noise. Doubles matches are skipped (bwin name
    format for doubles differs and cross-matching is too noisy).
    """
    dafa_home = dafabet_entry["home"]
    dafa_away = dafabet_entry["away"]

    if "/" in dafa_home or "/" in dafa_away:
        return None

    best_match = None
    best_score = 0.0
    best_min_side = 0.0

    for bw in bwin_matches:
        if "/" in bw["player1"] or "/" in bw["player2"]:
            continue

        sim_h1 = bwin_player_similarity(dafa_home, bw["player1"])
        sim_a2 = bwin_player_similarity(dafa_away, bw["player2"])
        score_normal = (sim_h1 + sim_a2) / 2

        sim_h2 = bwin_player_similarity(dafa_home, bw["player2"])
        sim_a1 = bwin_player_similarity(dafa_away, bw["player1"])
        score_reversed = (sim_h2 + sim_a1) / 2

        if score_normal >= score_reversed:
            score    = score_normal
            min_side = min(sim_h1, sim_a2)
        else:
            score    = score_reversed
            min_side = min(sim_h2, sim_a1)

        # Both sides must score well — prevents one-sided false matches.
        if score > best_score and min_side >= 0.60:
            best_score    = score
            best_min_side = min_side
            best_match    = bw

    if best_score >= threshold:
        print(f"[bwin] Matched: '{dafa_home} vs {dafa_away}' ↔ "
              f"'{best_match['player1']} vs {best_match['player2']}' "
              f"(sim={best_score:.2f})")
        return best_match

    if best_match and best_score >= 0.55:
        print(f"[bwin] Near miss: '{dafa_home} vs {dafa_away}' ↔ "
              f"'{best_match['player1']} vs {best_match['player2']}' "
              f"(sim={best_score:.2f}, threshold={threshold})")
    return None


async def check_bwin_delays(
    bwin_matches:        list[dict],
    dafabet_entries:     list[dict],
    alerted_bwin_delays: set,
    bwin_delay_pending:  dict,
) -> list[dict]:
    """
    Cross-check Dafabet state against bwin (reference clock) and return alerts
    for matches where Dafabet is lagging.

    Two-cycle debounce:
        Cycle N:   first observation of a lag on a given Dafabet match → entry
                   added to `bwin_delay_pending[url] = delay_info` and NO alert
                   is fired.
        Cycle N+1: if the Dafabet match is *still* lagging (same or larger
                   delay), an alert fires. If Dafabet caught up, the pending
                   entry is cleared silently (false alarm).

    Only SET_DELAY and GAME_DELAY trigger alerts from bwin — POINT_DELAY is
    intentionally suppressed per design (sub-game flicker is too noisy for
    a cross-source reference check).

    Args:
        bwin_matches:        pre-fetched bwin live matches (from fetch_bwin_live).
                             Passed in by the caller so the same list can be
                             reused for heartbeat snapshots without re-fetching.
        dafabet_entries:     Dafabet live entries already enriched with scores
                             by `extract_dafabet_scores`
        alerted_bwin_delays: set of alert keys already sent (dedup across time)
        bwin_delay_pending:  dict[dafa_url → delay_info] — first-cycle candidates
                             awaiting confirmation. Caller owns this dict and
                             retains it across cycles.

    Returns:
        list of alert dicts with 'alert_msg' ready for send_telegram().
    """
    if not dafabet_entries or not bwin_matches:
        if not bwin_matches:
            print("[bwin] No live matches parsed from bwin.")
        return []

    print(f"[bwin] {len(bwin_matches)} live match(es) on bwin, "
          f"{len(dafabet_entries)} on Dafabet")

    alerts: list[dict] = []
    seen_urls_this_cycle: set[str] = set()

    for dafa in dafabet_entries:
        if "/" in dafa.get("home", "") or "/" in dafa.get("away", ""):
            continue

        bw = match_dafabet_to_bwin(dafa, bwin_matches)
        if not bw:
            continue

        dafa_url = dafa["url"]
        seen_urls_this_cycle.add(dafa_url)

        dafa_current = dafa.get("current_set", 1)
        dafa_games   = dafa.get("game_scores", [])
        dafa_sets    = (dafa.get("sets_home", 0), dafa.get("sets_away", 0))

        # ── Sanity check: Dafabet score extraction is known-brittle and
        # sometimes parses odds/IDs as set/game numbers (e.g. sets=80-4,
        # games=(40,40)) or fails entirely and returns an empty game list.
        # Reject impossible tennis states so we don't fire phantom delay
        # alerts. Real tennis: best of 5 → max 3 sets per side, games per
        # set max ~13 (tiebreak or 12-12 in some formats).
        if dafa_sets[0] > 5 or dafa_sets[1] > 5 or dafa_current > 5:
            print(f"[bwin] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet score implausible (sets={dafa_sets}, "
                  f"set={dafa_current}); likely parse error upstream.")
            continue
        # No game data at all → Dafabet extractor failed for this match
        # (reported as "Games: [N/A]" in alerts). Treating empty as (0,0)
        # makes any live bwin state look like a lag → false positives.
        if not dafa_games:
            print(f"[bwin] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet game scores unavailable (N/A); cannot compare.")
            continue
        implausible_games = any(
            (not isinstance(g, (tuple, list)))
            or len(g) < 2
            or g[0] > 20 or g[1] > 20
            or g[0] < 0 or g[1] < 0
            for g in dafa_games
        )
        if implausible_games:
            print(f"[bwin] Skipping {dafa['home']} vs {dafa['away']} — "
                  f"Dafabet game scores implausible ({dafa_games}); "
                  f"likely parse error upstream.")
            continue

        bw_current = bw["current_set"]
        bw_games   = bw["game_scores"]
        bw_sets    = (bw["sets_p1"], bw["sets_p2"])

        # Ignore point level — pass None to detect_delay so POINT_DELAY is
        # never returned from the bwin path.
        delay = detect_delay(
            dafabet_current_set=dafa_current,
            dafabet_games=dafa_games,
            flashscore_current_set=bw_current,
            flashscore_games=bw_games,
            dafabet_sets_won=dafa_sets,
            flashscore_sets_won=bw_sets,
            dafabet_points=None,
            flashscore_points=None,
        )

        if not delay or delay["type"] == "POINT_DELAY":
            # No lag (or sub-game noise) — clear any stale pending entry.
            if dafa_url in bwin_delay_pending:
                print(f"[bwin] Delay cleared for {dafa['home']} vs {dafa['away']} "
                      f"(pending entry dropped)")
                bwin_delay_pending.pop(dafa_url, None)
            continue

        # Build stable alert key — includes delay magnitude so a growing
        # delay can re-alert even after the first one.
        if delay["type"] == "SET_DELAY":
            alert_key = (dafa_url, "BWIN_SET", bw_current)
        else:  # GAME_DELAY
            fs_cg = delay.get("flashscore_current_game", (0, 0))
            alert_key = (dafa_url, "BWIN_GAME", bw_current, fs_cg[0] + fs_cg[1])

        # ── Two-cycle debounce ──
        prev = bwin_delay_pending.get(dafa_url)
        if prev is None:
            # First observation this cycle — record and wait.
            bwin_delay_pending[dafa_url] = {
                "alert_key": alert_key,
                "delay":     delay,
                "bwin":      bw,
            }
            print(f"[bwin] Candidate delay (1st obs) for "
                  f"{dafa['home']} vs {dafa['away']}: {delay['type']} — "
                  f"will confirm next cycle")
            continue

        # Second observation — confirmed. (Any same-or-larger delay type
        # counts; delay growing from GAME→SET also confirms.)
        if alert_key in alerted_bwin_delays:
            # Already alerted at this magnitude — refresh pending but stay quiet.
            bwin_delay_pending[dafa_url] = {
                "alert_key": alert_key,
                "delay":     delay,
                "bwin":      bw,
            }
            continue

        alerted_bwin_delays.add(alert_key)
        bwin_delay_pending[dafa_url] = {
            "alert_key": alert_key,
            "delay":     delay,
            "bwin":      bw,
        }

        # ── Build alert message ──
        da_score_str = ", ".join(f"{g[0]}-{g[1]}" for g in dafa_games) if dafa_games else "N/A"
        bw_score_str = ", ".join(f"{g[0]}-{g[1]}" for g in bw_games)   if bw_games   else "N/A"

        if delay["type"] == "SET_DELAY":
            delay_desc = f"Dafabet is <b>{delay['set_diff']} set(s) behind bwin</b>"
            emoji = "⏱️"
        else:  # GAME_DELAY
            da_cg = delay.get("dafabet_current_game", (0, 0))
            bw_cg = delay.get("flashscore_current_game", (0, 0))
            delay_desc = (
                f"Dafabet is <b>{delay['game_diff']} game(s) behind bwin</b> "
                f"in Set {bw_current}\n"
                f"Dafabet game: {da_cg[0]}-{da_cg[1]} | "
                f"bwin game: {bw_cg[0]}-{bw_cg[1]}"
            )
            emoji = "🎾"

        cc_line = ""
        if bw.get("country1") or bw.get("country2"):
            cc_line = f" ({bw.get('country1', '?')} / {bw.get('country2', '?')})"

        alert_msg = (
            f"{emoji} <b>BWIN REFERENCE DELAY ANOMALY ({delay['type']})</b>\n"
            f"<i>Confirmed over 2 consecutive cycles</i>\n\n"
            f"<b>Match:</b> {dafa['home']} vs {dafa['away']}\n"
            f"<b>bwin:</b> {bw['player1']} vs {bw['player2']}{cc_line}\n\n"
            f"<b>Dafabet state:</b> Set {dafa_current} "
            f"(Sets: {dafa_sets[0]}-{dafa_sets[1]}) "
            f"Games: [{da_score_str}]\n"
            f"<b>bwin state:</b>    Set {bw_current} "
            f"(Sets: {bw_sets[0]}-{bw_sets[1]}) "
            f"Games: [{bw_score_str}]\n\n"
            f"<b>{delay_desc}</b>\n\n"
            f"<a href='{dafa['url']}'>🔗 Open Dafabet (click to verify)</a>\n"
            f"<a href='{bw['match_url']}'>🔗 Open bwin reference</a>"
        )

        alerts.append({
            "alert_msg":     alert_msg,
            "dafabet_entry": dafa,
            "bwin_match":    bw,
            "delay_info":    delay,
        })

        print(f"[bwin] {emoji} CONFIRMED {delay['type']}: "
              f"{dafa['home']} vs {dafa['away']} — "
              f"Dafabet Set {dafa_current} [{da_score_str}] vs "
              f"bwin Set {bw_current} [{bw_score_str}]")

    # ── Prune pending entries for matches we didn't see this cycle ──
    # (match ended, player names changed on one side, etc. — forget them)
    stale = [u for u in bwin_delay_pending if u not in seen_urls_this_cycle]
    for u in stale:
        bwin_delay_pending.pop(u, None)

    return alerts


# ── Heartbeat snapshot (Dafabet ↔ bwin summary for hourly report) ──

def build_bwin_heartbeat_section(
    bwin_matches:        list[dict],
    dafabet_entries:     list[dict],
    alerted_bwin_delays: set,
    bwin_delay_pending:  dict,
    max_chars:           int = 2500,
) -> str:
    """
    Build a compact HTML section describing the current Dafabet↔bwin
    cross-reference state. Called by the main polling loop once per cycle
    and stored in a shared dict; the hourly heartbeat reads the last
    rendered section verbatim.

    Layout:
        🔗 <b>bwin cross-reference</b>
        Coverage: 21 / 38 Dafabet matched · 5 skipped (bad data)

        ⏳ <b>Pending (awaiting 2nd-cycle confirm):</b>
          • Fancutt, T vs El Feky, Karim
            GAME_DELAY — bwin Set 2 (4-2) vs Dafabet Set 2 (2-2)

        🔴 <b>Confirmed delays (active):</b>
          • Maduzzi, Gaia vs Moccia, Carlotta
            SET_DELAY — bwin Set 3 vs Dafabet Set 2

    If there are no delays (pending or confirmed), shows
    "✅ All cross-referenced matches in sync."

    Output is truncated to roughly `max_chars` to fit alongside the
    match-list in the Telegram heartbeat (4096-char cap).
    """
    if not dafabet_entries:
        return (
            "\n\n🔗 <b>bwin cross-reference:</b>\n"
            "  No Dafabet live matches this cycle."
        )
    if not bwin_matches:
        return (
            "\n\n🔗 <b>bwin cross-reference:</b>\n"
            "  bwin has no live matches / feed unavailable."
        )

    pending_rows: list[str] = []
    confirmed_rows: list[str] = []
    cross_matched = 0
    skipped_invalid = 0

    # Build stable alert-key helper (must mirror the one in check_bwin_delays
    # so we can tell whether a pair was alerted or only pending).
    def _alert_key(dafa_url: str, delay: dict, bw_current: int) -> tuple:
        if delay["type"] == "SET_DELAY":
            return (dafa_url, "BWIN_SET", bw_current)
        fs_cg = delay.get("flashscore_current_game", (0, 0))
        return (dafa_url, "BWIN_GAME", bw_current, fs_cg[0] + fs_cg[1])

    for dafa in dafabet_entries:
        if "/" in dafa.get("home", "") or "/" in dafa.get("away", ""):
            continue

        bw = match_dafabet_to_bwin(dafa, bwin_matches)
        if not bw:
            continue

        cross_matched += 1

        dafa_sets    = (dafa.get("sets_home", 0), dafa.get("sets_away", 0))
        dafa_current = dafa.get("current_set", 1)
        dafa_games   = dafa.get("game_scores", [])

        # Same sanity filter as check_bwin_delays — skip entries whose
        # Dafabet state is garbage so we don't report phantom delays.
        if (dafa_sets[0] > 5 or dafa_sets[1] > 5 or dafa_current > 5):
            skipped_invalid += 1
            continue
        # Dafabet extractor failed to parse game data (renders as "N/A"
        # in alerts). Without games we can't compare — empty list would
        # be treated as (0,0) and produce phantom delays.
        if not dafa_games:
            skipped_invalid += 1
            continue
        bad_games = any(
            (not isinstance(g, (tuple, list))) or len(g) < 2
            or g[0] > 20 or g[1] > 20 or g[0] < 0 or g[1] < 0
            for g in dafa_games
        )
        if bad_games:
            skipped_invalid += 1
            continue

        bw_current = bw["current_set"]
        bw_games   = bw["game_scores"]
        bw_sets    = (bw["sets_p1"], bw["sets_p2"])

        delay = detect_delay(
            dafabet_current_set=dafa_current,
            dafabet_games=dafa_games,
            flashscore_current_set=bw_current,
            flashscore_games=bw_games,
            dafabet_sets_won=dafa_sets,
            flashscore_sets_won=bw_sets,
            dafabet_points=None,
            flashscore_points=None,
        )

        if not delay or delay["type"] == "POINT_DELAY":
            continue  # in-sync matches are not listed individually

        bw_cg = delay.get("flashscore_current_game", (0, 0))
        da_cg = delay.get("dafabet_current_game", (0, 0))

        if delay["type"] == "SET_DELAY":
            descr = (
                f"SET_DELAY — bwin Set {bw_current} "
                f"({bw_sets[0]}-{bw_sets[1]}) "
                f"vs Dafabet Set {dafa_current} "
                f"({dafa_sets[0]}-{dafa_sets[1]})"
            )
        else:  # GAME_DELAY
            descr = (
                f"GAME_DELAY — bwin Set {bw_current} "
                f"({bw_cg[0]}-{bw_cg[1]}) "
                f"vs Dafabet Set {dafa_current} "
                f"({da_cg[0]}-{da_cg[1]})"
            )

        row = (
            f"  • {dafa['home']} vs {dafa['away']}\n"
            f"    {descr}"
        )

        key = _alert_key(dafa["url"], delay, bw_current)
        if key in alerted_bwin_delays:
            confirmed_rows.append(row)
        else:
            pending_rows.append(row)

    header = (
        f"\n\n🔗 <b>bwin cross-reference</b>\n"
        f"  Coverage: {cross_matched} / {len(dafabet_entries)} "
        f"Dafabet matched"
    )
    if skipped_invalid:
        header += f" · {skipped_invalid} skipped (bad data)"

    if not pending_rows and not confirmed_rows:
        section = f"{header}\n  ✅ All cross-referenced matches in sync."
    else:
        body_parts: list[str] = []
        if pending_rows:
            body_parts.append(
                "\n⏳ <b>Pending (awaiting 2nd-cycle confirm):</b>\n"
                + "\n".join(pending_rows)
            )
        if confirmed_rows:
            body_parts.append(
                "\n🔴 <b>Confirmed delays (active):</b>\n"
                + "\n".join(confirmed_rows)
            )
        section = header + "".join(body_parts)

    # Hard cap so the heartbeat stays under Telegram's 4096-char limit
    # even with a long match list appended by heartbeat_loop.
    if len(section) > max_chars:
        section = section[: max_chars - 15] + "\n  […truncated]"
    return section
