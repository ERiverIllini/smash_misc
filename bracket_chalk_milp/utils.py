"""
utils.py
--------
Shared helpers: weight normalization and feasibility checks.
"""

from __future__ import annotations

from collections import defaultdict


def symmetrize_weights(
    weights: dict[tuple[str, str], float],
) -> defaultdict[tuple[str, str], float]:
    """
    Return a defaultdict(float) covering both directions of every pair.

    If only (p1, p2) is supplied, (p2, p1) inherits the same value unless
    it was explicitly provided.  Missing pairs return 0.0.
    """
    w: defaultdict[tuple[str, str], float] = defaultdict(float)
    for (a, b), v in weights.items():
        w[(a, b)] = v
        if (b, a) not in weights:
            w[(b, a)] = v
    return w


def is_feasible(
    assignment: dict[str, int],
    allowed_seeds: dict[str, set[int]],
) -> bool:
    """Return True iff every player is assigned a seed in their allowed set."""
    return all(assignment[p] in allowed_seeds.get(p, set()) for p in assignment)
