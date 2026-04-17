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


def print_matches(matches: list[dict]) -> None:
    """Pretty-print live matches to stdout."""
    if not matches:
        print("No live matches right now.")
        return

    print(f"\n{'='*60}")
    print(f"  API-TENNIS LIVE SCORES  ({len(matches)} match(es))")
    print(f"{'='*60}")

    for m in matches:
        home  = m.get("event_first_player",  "?")
        away  = m.get("event_second_player", "?")
        score = m.get("event_score",         "?")
        serve = m.get("event_serve",         "")
        sets  = m.get("event_sets",          [])
        tourn = m.get("tournament_name",     "")
        status = m.get("event_status",       "")

        # Build per-set breakdown
        set_str = ""
        if isinstance(sets, list):
            set_str = "  |  " + "  ".join(
                f"S{i+1}: {s.get('score_first','?')}-{s.get('score_second','?')}"
                for i, s in enumerate(sets)
            )

        serve_indicator = f"  ({'*' if serve == 'home' else ' '}{home[:3]} serving)" if serve else ""

        print(f"\n  {home}  vs  {away}")
        print(f"  Score: {score}{set_str}")
        print(f"  Status: {status}  |  Tournament: {tourn}{serve_indicator}")

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
