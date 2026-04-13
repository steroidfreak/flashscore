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

    # Extract sets won: [X:Y]
    sets_match = re.search(r"\[(\d+):(\d+)\]", line)
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
        page: Playwright page object (can be a dedicated tab)

    Returns:
        List of parsed match dicts from parse_flashscore_line()
        Each dict also gets a 'match_url' field for fetching point scores later.
    """
    try:
        await page.goto(FLASHSCORE_LIVE_URL, wait_until="domcontentloaded", timeout=20_000)
        await page.wait_for_timeout(2_000)

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

            score_info = await page.evaluate("""
                () => {
                    const text = (document.body.innerText || '').trim();
                    const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);

                    // Collect all short numeric-looking text elements
                    // Dafabet scoreboard typically has rows like:
                    //   Player1   6  3
                    //   Player2   4  5
                    // Or score cells showing set-by-set scores

                    let allNums = [];
                    const divs = document.querySelectorAll('div, span, td');
                    for (const el of divs) {
                        const t = (el.innerText || '').trim();
                        // Look for single/double digit scores
                        if (/^\\d{1,2}$/.test(t)) {
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {
                                allNums.push({
                                    val: parseInt(t),
                                    x: Math.round(rect.x),
                                    y: Math.round(rect.y),
                                    w: Math.round(rect.width),
                                    h: Math.round(rect.height),
                                });
                            }
                        }
                    }

                    // Find set score patterns in the full text
                    // Look for patterns like "6 4 3" (player1 set scores) and "4 6 5" (player2)
                    // Also look for "Set 1", "Set 2" indicators
                    const setPattern = /(?:set|s)\\s*(\\d)/gi;
                    let currentSet = 0;
                    let m;
                    while ((m = setPattern.exec(text)) !== null) {
                        const n = parseInt(m[1]);
                        if (n > currentSet) currentSet = n;
                    }

                    // Try to find score rows — lines that are just numbers separated by spaces/tabs
                    const scoreRows = [];
                    for (const line of lines) {
                        // Match lines like "6  4  3" or "6 - 4" patterns
                        const nums = line.match(/\\b\\d{1,2}\\b/g);
                        if (nums && nums.length >= 1 && nums.length <= 6) {
                            const allSmall = nums.every(n => parseInt(n) <= 50);
                            // Check if line is mostly numbers (score row)
                            const nonNum = line.replace(/[\\d\\s\\-–|:]/g, '').length;
                            if (allSmall && nonNum < line.length * 0.5) {
                                scoreRows.push(nums.map(Number));
                            }
                        }
                    }

                    // Also search for explicit "X - Y" set score format
                    const setsWon = text.match(/(\\d+)\\s*[-–]\\s*(\\d+)/);
                    let setsHome = 0, setsAway = 0;
                    if (setsWon) {
                        setsHome = parseInt(setsWon[1]);
                        setsAway = parseInt(setsWon[2]);
                    }

                    // Fallback current set from sets won
                    if (currentSet === 0) {
                        currentSet = setsHome + setsAway + 1;
                    }

                    // Extract point score within current game (e.g. "30", "15", "40", "AD")
                    // Dafabet shows point scores like "30 - 15" or "40 - AD" on the match page
                    let pointHome = null, pointAway = null;
                    const pointPatterns = [
                        /\\b(0|15|30|40|AD|A)\\s*[-–:]\\s*(0|15|30|40|AD|A)\\b/gi,
                    ];
                    for (const pat of pointPatterns) {
                        const pm = text.match(pat);
                        if (pm) {
                            // Use the last match (usually the current live point score)
                            const last = pm[pm.length - 1];
                            const parts = last.split(/[-–:]/);
                            if (parts.length === 2) {
                                pointHome = parts[0].trim().toUpperCase();
                                pointAway = parts[1].trim().toUpperCase();
                            }
                        }
                    }

                    return {
                        sets_home: setsHome,
                        sets_away: setsAway,
                        current_set: currentSet,
                        score_rows: scoreRows,
                        num_elements: allNums.length,
                        point_home: pointHome,
                        point_away: pointAway,
                        page_text: text.substring(0, 800),
                    };
                }
            """)

            entry["sets_home"] = score_info["sets_home"]
            entry["sets_away"] = score_info["sets_away"]
            entry["current_set"] = score_info["current_set"]
            entry["page_text"] = score_info.get("page_text", "")

            # Point score within current game
            ph = score_info.get("point_home")
            pa = score_info.get("point_away")
            entry["point_score"] = (ph, pa) if ph and pa else None

            # Parse game scores per set from score_rows
            # Typically two rows: player1 scores [6, 3] and player2 scores [4, 5]
            game_scores = []
            rows = score_info.get("score_rows", [])
            if len(rows) >= 2:
                p1_scores = rows[0]
                p2_scores = rows[1]
                n_sets = min(len(p1_scores), len(p2_scores))
                for i in range(n_sets):
                    game_scores.append((p1_scores[i], p2_scores[i]))

            entry["game_scores"] = game_scores

            print(f"[delay] Dafabet score for {entry['home']} vs {entry['away']}: "
                  f"sets={entry['sets_home']}-{entry['sets_away']} "
                  f"games={game_scores} set={entry['current_set']} "
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

    # Open a new tab for Flashscore
    fs_page = await browser_context.new_page()
    try:
        flashscore_matches = await fetch_flashscore_live(fs_page)
    finally:
        await fs_page.close()

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

        # Get Flashscore data
        fs_current = fs_match["current_set"]
        fs_games = fs_match["game_scores"]  # [(g1,g2), ...] per set
        fs_sets = (fs_match["sets_p1"], fs_match["sets_p2"])
        fs_points = fs_match.get("point_score")  # filled later if needed

        # For point-level comparison: if same set & same game, fetch
        # Flashscore point scores from the match detail page
        da_cg_total = sum(_current_set_games(dafa_games))
        fs_cg_total = sum(_current_set_games(fs_games))
        if (dafa_current == fs_current and da_cg_total == fs_cg_total
                and dafa_points and not fs_points and fs_match.get("match_url")):
            # Need to fetch point score from Flashscore match page
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
