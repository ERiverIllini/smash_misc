"""
enumerate.py
------------
Pure-Python enumeration of all feasible chalk seedings and their matchup sets.

No solver is required — this iterates all valid seed assignments and computes
the resulting matches and costs directly.
"""

from __future__ import annotations

from itertools import permutations

from bracket_chalk_milp.bracket import chalk_match_pairs
from bracket_chalk_milp.utils import is_feasible, symmetrize_weights


def enumerate_matchup_sets(
    players: list[str],
    allowed_seeds: dict[str, set[int]],
    weights: dict[tuple[str, str], float],
) -> None:
    """
    Print every feasible seeding and the matches it produces under chalk,
    along with the per-match cost and total cost.

    Parameters
    ----------
    players       : list of player names (length must be a power of 2)
    allowed_seeds : maps each player to the set of seeds they may receive
    weights       : (player_i, player_j) -> match cost; missing pairs cost 0
    """
    n = len(players)
    seeds = list(range(1, n + 1))
    chalk_pairs = chalk_match_pairs(n)
    w = symmetrize_weights(weights)

    print("=" * 60)
    print("ENUMERATION OF FEASIBLE CHALK SEEDINGS")
    print("=" * 60)

    found = 0
    for perm in permutations(players):
        assignment: dict[str, int] = {p: seeds[i] for i, p in enumerate(perm)}
        if not is_feasible(assignment, allowed_seeds):
            continue

        found += 1
        seed_to_player = {v: k for k, v in assignment.items()}
        total_cost = 0.0
        match_lines: list[tuple[int, str]] = []

        for s1, s2, rnd in chalk_pairs:
            p1 = seed_to_player[s1]
            p2 = seed_to_player[s2]
            cost = w[(p1, p2)]
            total_cost += cost
            match_lines.append(
                (rnd, f"  Round {rnd}: {p1}(seed {s1}) vs {p2}(seed {s2})  cost={cost}")
            )

        seeding_str = ", ".join(f"{p}={assignment[p]}" for p in players)
        print(f"\nSeeding {found}: {seeding_str}")
        for _rnd, line in sorted(match_lines):
            print(line)
        print(f"  --> Total cost: {total_cost}")

    print(f"\n{found} feasible seeding(s) found.")
    print("=" * 60)
