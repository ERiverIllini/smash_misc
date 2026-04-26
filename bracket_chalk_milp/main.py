"""
main.py
-------
Entry point.  Edit the INPUTS block below to define your problem, then run:

    python -m bracket_chalk_milp
"""

from bracket_chalk_milp.enumerate import enumerate_matchup_sets
from bracket_chalk_milp.solver import build_and_solve


def main() -> None:
    # ── INPUTS ── edit here to change the problem ─────────────────────────
    PLAYERS = ["A", "B", "C", "D"]

    ALLOWED_SEEDS = {
        "A": {1, 2},
        "B": {1, 2},
        "C": {3},
        "D": {4},
    }

    # weights[(p1, p2)] = cost when p1 and p2 play a match.
    # Symmetric: only one direction needed; missing pairs default to 0.
    WEIGHTS = {
        ("A", "D"): 1.0,
        ("B", "D"): 2.0,
        ("A", "C"): 6.0,
        ("B", "C"): 4.0,
        ("A", "B"): 5.0,
    }
    # ── END INPUTS ─────────────────────────────────────────────────────────

    # enumerate_matchup_sets(PLAYERS, ALLOWED_SEEDS, WEIGHTS)

    print("\nSOLVING MILP...\n")
    assignment, cost = build_and_solve(PLAYERS, ALLOWED_SEEDS, WEIGHTS,)

    seeding_str = ", ".join(f"{p}=seed {assignment[p]}" for p in PLAYERS)
    print(f"Optimal seeding : {seeding_str}")
    print(f"Optimal cost    : {cost}")


if __name__ == "__main__":
    main()
