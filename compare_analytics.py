#!/usr/bin/env python3
"""
Compare two analytics.json-style snapshots (e.g. old_analytics.json vs current_analytics.json).

Prints page counts, longest page, and a quick heuristic on whether top-word lists look
"spammy" (calendar/month tokens dominating) vs more technical vocabulary overlap.

Usage:
  python compare_analytics.py old_analytics.json current_analytics.json
"""

import argparse
import json
import sys
from pathlib import Path


# Tokens that dominated some crawls with calendar / blog noise (not authoritative spam detection).
CALENDAR_NOISE = frozenset(
    "jan feb mar apr may jun jul aug sep oct nov dec mon tue wed thu fri sat sun".split()
)


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def top_words(word_counts, n=50):
    return sorted(word_counts.items(), key=lambda x: (-x[1], x[0]))[:n]


def noise_score(words):
    """Fraction of top-50 slots that look like calendar noise."""
    if not words:
        return 0.0
    noisy = sum(1 for w, _ in words if w.lower() in CALENDAR_NOISE)
    return noisy / len(words)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("old_json", type=Path)
    ap.add_argument("new_json", type=Path)
    args = ap.parse_args()

    a = load(args.old_json)
    b = load(args.new_json)

    old_pages = a.get("unique_pages") or []
    new_pages = b.get("unique_pages") or []
    wc_a = a.get("word_counts") or {}
    wc_b = b.get("word_counts") or {}

    print(f"A: {args.old_json}  unique_pages={len(old_pages)}")
    print(f"B: {args.new_json}  unique_pages={len(new_pages)}")
    print()

    lp_a = a.get("longest_page") or ("", 0)
    lp_b = b.get("longest_page") or ("", 0)
    print("Longest page (URL, word count):")
    print(f"  A: {lp_a[0]}  ({lp_a[1]})")
    print(f"  B: {lp_b[0]}  ({lp_b[1]})")
    print()

    ta = top_words(wc_a, 50)
    tb = top_words(wc_b, 50)
    print("Calendar-noise share in top-50 words (higher => more month/day tokens):")
    print(f"  A: {noise_score(ta):.2f}")
    print(f"  B: {noise_score(tb):.2f}")
    print()

    sa = set(w for w, _ in ta)
    sb = set(w for w, _ in tb)
    print(f"Top-50 word overlap (Jaccard): {len(sa & sb) / len(sa | sb):.2f}")
    print()

    print("Top 15 words — A vs B:")
    for i in range(15):
        wa = f"{ta[i][0]}:{ta[i][1]}" if i < len(ta) else ""
        wb = f"{tb[i][0]}:{tb[i][1]}" if i < len(tb) else ""
        print(f"  {wa:30}  |  {wb}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
