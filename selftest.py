"""
Self-test for the duplicate-detection model
============================================
Runs the same tennis player-similarity and team-sport similarity used by
the live monitor against either a built-in suite of cases or a file you
supply. Use this to validate that a new pair of names you spotted on
Dafabet would (or wouldn't) be flagged, without having to wait for the
next live cycle.

Usage
-----
  python selftest.py                       # built-in cases only
  python selftest.py path/to/cases.json    # JSON test file
  python selftest.py path/to/cases.txt     # plain-text test file
  python monitor.py --test [path]          # equivalent entry point

Built-in cases include the tennis examples from MEMORY.md and the
basketball "Colegio Los Leones / Leones de Quilpue" example from the
user's screenshots.

JSON file format
----------------
  {
    "cases": [
      {
        "sport": "basketball",
        "match_a": {"home": "Colegio Los Leones", "away": "CD Espanol De Talca"},
        "match_b": {"home": "Leones de Quilpue",  "away": "CD Espanol De Talca"},
        "expected": "duplicate"
      },
      {
        "sport": "tennis",
        "match_a": {"home": "Butvilas, Edas", "away": "Imamura, Masamichi"},
        "match_b": {"home": "Butvilas, E",    "away": "Imamura, M"},
        "expected": "duplicate"
      }
    ]
  }

Plain-text format
-----------------
One case per line. Comment lines start with '#'.

  # sport       verdict   match A (home / away) | match B (home / away)
  basketball    DUP       Colegio Los Leones / CD Espanol De Talca | Leones de Quilpue / CD Espanol De Talca
  tennis        DUP       Butvilas, Edas / Imamura, Masamichi | Butvilas, E / Imamura, M
  basketball    NOT       Lakers / Celtics                    | Warriors / Bulls

  • sport:    "tennis" | "basketball" | "volleyball"
  • verdict:  "DUP" / "DUPLICATE" / "Y"   ⇢ expected duplicate
              "NOT" / "DISTINCT"   / "N"  ⇢ expected NOT duplicate

Image input
-----------
This tool intentionally does not OCR images (keeps deps light). To test
a pair you see in a screenshot, transcribe the four team / player names
into either format above. The example file `selftest_cases.example.txt`
contains the screenshot pair that motivated the basketball monitor.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Make sure we can import sibling modules when run from any cwd
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Tennis player similarity lives in monitor.py. We rely on monitor.py's
# top-level being import-safe (defensive _build_telegram_recipients).
from monitor import (
    match_similarity   as tennis_match_similarity,
    SIMILARITY_THRESHOLD as TENNIS_THRESHOLD,
)
from team_sport_dup import (
    team_match_similarity,
)

# Team-sport defaults (mirror monitor.py env defaults)
TEAM_THRESHOLD   = float(os.getenv("BASK_SIMILARITY_THRESHOLD", "0.70"))
TEAM_MIN_SIDE    = float(os.getenv("BASK_MIN_SIDE_SCORE",       "0.35"))
TEAM_STRONG_SIDE = float(os.getenv("BASK_STRONG_SIDE_SCORE",    "0.90"))


# ── Built-in cases ────────────────────────────────────────────────

BUILTIN_CASES: list[dict] = [
    # Basketball — the user's screenshot pair
    {
        "sport": "basketball",
        "match_a": {"home": "Colegio Los Leones", "away": "CD Espanol De Talca"},
        "match_b": {"home": "Leones de Quilpue",  "away": "CD Espanol De Talca"},
        "expected": True,
        "label": "screenshot pair (Chile)",
    },
    {
        "sport": "basketball",
        "match_a": {"home": "CD Espanol De Talca", "away": "Colegio Los Leones"},
        "match_b": {"home": "Leones de Quilpue",   "away": "CD Espanol De Talca"},
        "expected": True,
        "label": "screenshot pair (sides swapped)",
    },
    {
        "sport": "basketball",
        "match_a": {"home": "Real Madrid", "away": "FC Barcelona"},
        "match_b": {"home": "Madrid",      "away": "Barcelona"},
        "expected": True,
        "label": "prefix drift (Real / FC)",
    },
    {
        "sport": "basketball",
        "match_a": {"home": "Real Madrid",     "away": "FC Barcelona"},
        "match_b": {"home": "Atletico Madrid", "away": "Valencia CF"},
        "expected": False,
        "label": "different match (city collision only)",
    },
    {
        "sport": "basketball",
        "match_a": {"home": "Lakers",   "away": "Celtics"},
        "match_b": {"home": "Warriors", "away": "Bulls"},
        "expected": False,
        "label": "fully distinct",
    },

    # Volleyball — same model as basketball, sanity checks
    {
        "sport": "volleyball",
        "match_a": {"home": "VK Dukla Liberec", "away": "VC Praha"},
        "match_b": {"home": "Dukla Liberec",    "away": "Praha"},
        "expected": True,
        "label": "VC/VK prefix drift",
    },
    {
        "sport": "volleyball",
        "match_a": {"home": "Zenit Kazan",  "away": "Belogorie Belgorod"},
        "match_b": {"home": "Lokomotiv",    "away": "Dynamo Moscow"},
        "expected": False,
        "label": "fully distinct",
    },

    # Tennis — from MEMORY.md
    {
        "sport": "tennis",
        "match_a": {"home": "Butvilas, Edas", "away": "Imamura, Masamichi"},
        "match_b": {"home": "Butvilas, E",    "away": "Imamura, M"},
        "expected": True,
        "label": "comma full vs initials",
    },
    {
        "sport": "tennis",
        "match_a": {"home": "Shimizu Y",      "away": "Romios M C"},
        "match_b": {"home": "Shimizu, Yuki",  "away": "Romios, Marc"},
        "expected": True,
        "label": "no-comma vs comma",
    },
    {
        "sport": "tennis",
        "match_a": {"home": "Butvilas, Edas", "away": "Kopp, S"},
        "match_b": {"home": "Imamura, M",     "away": "Popovic, S"},
        "expected": False,
        "label": "same-initial different surname",
    },
]


# ── Scoring ────────────────────────────────────────────────────────

def score_case(case: dict) -> tuple[float, str, bool]:
    """
    Run the appropriate similarity model for `case`.
    Returns (score, explanation, predicted_duplicate).
    """
    sport = case["sport"].lower().strip()
    a = {"home": case["match_a"]["home"], "away": case["match_a"]["away"], "url": "A"}
    b = {"home": case["match_b"]["home"], "away": case["match_b"]["away"], "url": "B"}

    if sport == "tennis":
        score, expl = tennis_match_similarity(a, b)
        predicted = score >= TENNIS_THRESHOLD
        return score, expl, predicted

    if sport in ("basketball", "volleyball", "team"):
        score, expl = team_match_similarity(a, b, TEAM_MIN_SIDE, TEAM_STRONG_SIDE)
        predicted = score >= TEAM_THRESHOLD
        return score, expl, predicted

    raise ValueError(f"Unknown sport in case: {sport!r}")


# ── File loaders ───────────────────────────────────────────────────

def _normalize_expected(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("duplicate", "dup", "y", "yes", "true", "1"):
            return True
        if v in ("distinct", "not", "n", "no", "false", "0"):
            return False
    raise ValueError(f"Bad expected value: {value!r}")


def load_json_cases(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_cases = data.get("cases", data) if isinstance(data, dict) else data
    cases: list[dict] = []
    for c in raw_cases:
        cases.append({
            "sport":    c["sport"],
            "match_a":  c["match_a"],
            "match_b":  c["match_b"],
            "expected": _normalize_expected(c.get("expected", "duplicate")),
            "label":    c.get("label", ""),
        })
    return cases


def load_text_cases(path: Path) -> list[dict]:
    """
    Plain-text format, one case per line:
      sport  verdict  homeA / awayA | homeB / awayB
    """
    cases: list[dict] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Split off sport + verdict from the names by finding the "|"
        if "|" not in line:
            print(f"  [skip] no '|' separator: {line}")
            continue

        # Two halves around the "|" — left contains "<sport> <verdict> homeA / awayA"
        left, right = line.split("|", 1)
        left_parts = left.split(None, 2)
        if len(left_parts) < 3:
            print(f"  [skip] malformed left side: {line}")
            continue
        sport, verdict, names_a = left_parts
        names_b = right.strip()

        try:
            home_a, away_a = (s.strip() for s in names_a.split("/", 1))
            home_b, away_b = (s.strip() for s in names_b.split("/", 1))
        except ValueError:
            print(f"  [skip] malformed names: {line}")
            continue

        try:
            expected = _normalize_expected(verdict)
        except ValueError as exc:
            print(f"  [skip] {exc}: {line}")
            continue

        cases.append({
            "sport":    sport.lower(),
            "match_a":  {"home": home_a, "away": away_a},
            "match_b":  {"home": home_b, "away": away_b},
            "expected": expected,
            "label":    "",
        })
    return cases


def load_cases(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Test case file not found: {p}")
    if p.suffix.lower() == ".json":
        return load_json_cases(p)
    return load_text_cases(p)


# ── Runner ─────────────────────────────────────────────────────────

def run_selftest(extra_path: str | None = None) -> bool:
    """
    Run every built-in case plus any from `extra_path` (JSON or text).
    Returns True if every case matched its expected outcome.
    """
    cases = list(BUILTIN_CASES)
    if extra_path:
        try:
            extra = load_cases(extra_path)
            cases.extend(extra)
            print(f"Loaded {len(extra)} extra case(s) from {extra_path}\n")
        except Exception as exc:
            print(f"[error] Could not load {extra_path}: {exc}")
            return False

    print(f"Running {len(cases)} self-test case(s)…")
    print(
        f"  Tennis threshold: {TENNIS_THRESHOLD:.2f}   "
        f"Team threshold: {TEAM_THRESHOLD:.2f} "
        f"(min_side={TEAM_MIN_SIDE:.2f} strong_side={TEAM_STRONG_SIDE:.2f})\n"
    )

    passed = 0
    failed_cases: list[tuple[dict, float, str]] = []

    for i, case in enumerate(cases, start=1):
        try:
            score, expl, predicted = score_case(case)
        except Exception as exc:
            print(f"  [{i:>3}] ERROR  ({case.get('sport','?')})  {exc}")
            failed_cases.append((case, 0.0, str(exc)))
            continue

        expected = case["expected"]
        ok = predicted == expected
        verdict_str = "DUPLICATE" if predicted else "distinct"
        expected_str = "DUPLICATE" if expected else "distinct"
        marker = "OK  " if ok else "FAIL"
        label = case.get("label", "")
        label_str = f"  ({label})" if label else ""

        print(
            f"  [{i:>3}] {marker} {case['sport']:<10} score={score:.3f}  "
            f"-> {verdict_str:<9} (expected {expected_str}){label_str}"
        )
        a, b = case["match_a"], case["match_b"]
        print(f"         A: {a['home']} / {a['away']}")
        print(f"         B: {b['home']} / {b['away']}")
        if not ok:
            print(expl)
            failed_cases.append((case, score, expl))

        if ok:
            passed += 1

    total = len(cases)
    print(f"\n{passed} / {total} passed")
    if failed_cases:
        print(f"\n{len(failed_cases)} FAILURE(S):")
        for case, score, expl in failed_cases:
            a, b = case["match_a"], case["match_b"]
            print(
                f"  - [{case['sport']}] expected "
                f"{'DUPLICATE' if case['expected'] else 'distinct'}, "
                f"got score={score:.3f}"
            )
            print(f"      A: {a['home']} / {a['away']}")
            print(f"      B: {b['home']} / {b['away']}")
        return False
    return True


if __name__ == "__main__":
    extra = sys.argv[1] if len(sys.argv) > 1 else None
    ok = run_selftest(extra)
    sys.exit(0 if ok else 1)
