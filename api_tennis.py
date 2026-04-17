"""
api-tennis.com livescore fetcher
=================================
Fetches live tennis scores from api-tennis.com API.

Usage:
    python api_tennis.py          # fetch and print all live matches
    python api_tennis.py --raw    # print raw JSON response

Requires in .env:
    API_TENNIS_KEY=your_api_key
"""

import asyncio
import json
import os
import sys

import httpx
from dotenv import load_dotenv

load_dotenv()

API_TENNIS_KEY: str = os.getenv("API_TENNIS_KEY", "")
API_TENNIS_URL: str = "https://api.api-tennis.com/tennis/"


async def fetch_livescore(timezone: str = "UTC") -> list[dict]:
    """
    Fetch all live tennis matches from api-tennis.com.

    Returns a list of match dicts, each containing:
        event_key, event_status, home_name, away_name,
        scores (set-by-set), serve, tournament, etc.
    """
    if not API_TENNIS_KEY:
        print("[api-tennis] ERROR: API_TENNIS_KEY not set in .env")
        return []

    params = {
        "method":   "get_livescore",
        "APIkey":   API_TENNIS_KEY,
        "timezone": timezone,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(API_TENNIS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        print(f"[api-tennis] HTTP error: {exc.response.status_code} — {exc.response.text[:200]}")
        return []
    except Exception as exc:
        print(f"[api-tennis] Request error: {exc}")
        return []

    # Response structure: {"success": 1, "result": [...]}
    if not data.get("success"):
        print(f"[api-tennis] API returned failure: {data}")
        return []

    return data.get("result", [])


def _parse_set_scores(pointbypoint: list[dict]) -> dict[str, str]:
    """
    Extract the latest game score per set from pointbypoint data.
    Returns e.g. {"Set 1": "6-4", "Set 2": "3-2"}
    """
    set_scores: dict[str, str] = {}
    for game in pointbypoint:
        set_name = game.get("set_number", "")
        score    = game.get("score", "")
        if set_name and score:
            set_scores[set_name] = score.replace(" ", "")  # "4 - 3" → "4-3"
    return set_scores


def print_matches(matches: list[dict]) -> None:
    """Pretty-print live matches to stdout."""
    if not matches:
        print("No live matches right now.")
        return

    # Separate live vs finished
    live     = [m for m in matches if m.get("event_status", "") != "Finished"]
    finished = [m for m in matches if m.get("event_status", "") == "Finished"]

    print(f"\n{'='*60}")
    print(f"  API-TENNIS LIVE SCORES")
    print(f"  {len(live)} live  |  {len(finished)} just finished  |  {len(matches)} total")
    print(f"{'='*60}")

    for m in live + finished:
        home   = m.get("event_first_player",  "?")
        away   = m.get("event_second_player", "?")
        sets   = m.get("event_final_result",  "-")   # sets won  e.g. "1 - 0"
        game   = m.get("event_game_result",   "-")   # points    e.g. "40 - 15"
        serve  = m.get("event_serve",         "")    # "First Player" / "Second Player" / null
        status = m.get("event_status",        "")    # "Set 1", "Finished" etc.
        tourn  = m.get("tournament_name",     "")
        round_ = m.get("tournament_round",    "")
        pbp    = m.get("pointbypoint",        [])

        # Per-set game scores from pointbypoint
        set_scores = _parse_set_scores(pbp)
        set_str = "  ".join(
            f"{k}: {v}" for k, v in set_scores.items()
        )

        # Serve indicator
        if serve == "First Player":
            serve_str = f"  * {home.split('/')[0].strip()} serving"
        elif serve == "Second Player":
            serve_str = f"  * {away.split('/')[0].strip()} serving"
        else:
            serve_str = ""

        # Status tag
        tag = "[FINISHED]" if status == "Finished" else f"[LIVE]  {status}"

        print(f"\n  {home}  vs  {away}")
        print(f"  Sets: {sets}  |  Game: {game}{serve_str}")
        if set_str:
            print(f"  Progress: {set_str}")
        print(f"  {tag}  |  {tourn}  ({round_})")

    print(f"\n{'='*60}\n")


async def main() -> None:
    raw_mode = "--raw" in sys.argv

    print("[api-tennis] Fetching live scores…")
    matches = await fetch_livescore()

    if raw_mode:
        print(json.dumps(matches, indent=2))
    else:
        print_matches(matches)

    print(f"[api-tennis] Done. {len(matches)} live match(es) returned.")


if __name__ == "__main__":
    asyncio.run(main())
