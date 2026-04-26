"""
bracket.py
----------
Bracket structure and chalk match-pair precomputation.

"Chalk" assumption: in every match the lower seed number (higher seed) wins.
Given this, the complete set of matches for any seeding is fully determined
by the bracket layout — no results are needed.
"""

from __future__ import annotations

import math


def _standard_bracket_slots(n: int) -> list[int]:
    """
    Return the seed order of bracket positions for a standard 2^k bracket.

    Standard seeding ensures top seeds can only meet in later rounds:
        n=4  ->  [1, 4, 2, 3]   round-1 pairs: (1 vs 4), (2 vs 3)
        n=8  ->  [1, 8, 4, 5, 2, 7, 3, 6]

    Built by expanding each seed s at each doubling step into (s, 2k+1-s)
    where 2k is the new bracket size.
    """
    if n == 1:
        return [1]
    slots: list[int] = [1, 2]
    while len(slots) < n:
        new_n = len(slots) * 2
        next_slots: list[int] = []
        for s in slots:
            next_slots.append(s)
            next_slots.append(new_n + 1 - s)
        slots = next_slots
    return slots


def chalk_match_pairs(n: int) -> list[tuple[int, int, int]]:
    """
    Return every (seed_a, seed_b, round_number) triple that occurs in a
    standard chalk bracket of size n (must be a power of 2).

    seed_a < seed_b always.  round_number starts at 1.

    Examples
    --------
    >>> chalk_match_pairs(4)
    [(1, 4, 1), (2, 3, 1), (1, 2, 2)]
    """
    if n < 2 or (n & (n - 1)) != 0:
        raise ValueError(f"n must be a power of 2, got {n}")

    slots = _standard_bracket_slots(n)
    pairs: list[tuple[int, int, int]] = []
    num_rounds = int(math.log2(n))

    current_slots = list(slots)
    for rnd in range(1, num_rounds + 1):
        next_slots: list[int] = []
        for i in range(0, len(current_slots), 2):
            s1, s2 = current_slots[i], current_slots[i + 1]
            lo, hi = min(s1, s2), max(s1, s2)
            pairs.append((lo, hi, rnd))
            next_slots.append(lo)  # chalk winner = lower seed number
        current_slots = next_slots

    return pairs
