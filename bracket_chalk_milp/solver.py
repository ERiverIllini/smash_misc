"""
solver.py
---------
PySCIPOpt MILP that finds the seeding minimizing total chalk-bracket match cost.

Formulation
-----------
Variables
    S[p, s]              binary  — player p receives seed s
    Z[p1, s1, p2, s2]   binary  — linearization of S[p1,s1] * S[p2,s2]
                                   created only for chalk match-pair slots (s1, s2)

Assignment constraints
    sum_s S[p, s]  = 1   for all p      (each player gets one seed)
    sum_p S[p, s]  = 1   for all s      (each seed goes to one player)
    S[p, s]        = 0   if s not in allowed_seeds[p]

McCormick linearization  (for each chalk pair (s1,s2) and each player pair (p1,p2))
    Z[p1,s1,p2,s2] <= S[p1,s1]
    Z[p1,s1,p2,s2] <= S[p2,s2]
    Z[p1,s1,p2,s2] >= S[p1,s1] + S[p2,s2] - 1

Objective
    minimize  sum_{chalk pairs (s1,s2)}  sum_{p1 != p2}  w[p1,p2] * Z[p1,s1,p2,s2]

Requirements
------------
    pip install pyscipopt
    SCIP backend: https://www.scipopt.org  or  conda install -c conda-forge pyscipopt
"""

from __future__ import annotations

from typing import Any

from pyscipopt import Model

from bracket_chalk_milp.bracket import chalk_match_pairs
from bracket_chalk_milp.utils import symmetrize_weights


def build_and_solve(
    players: list[str],
    allowed_seeds: dict[str, set[int]],
    weights: dict[tuple[str, str], float],
    verbose: bool = False,
) -> tuple[dict[str, int], float]:
    """
    Build and solve the chalk-bracket MILP.

    Parameters
    ----------
    players       : ordered list of player names (length must be a power of 2)
    allowed_seeds : maps each player to the set of seeds they may receive
    weights       : (player_i, player_j) -> match cost; missing pairs cost 0;
                    only one direction needed for symmetric costs
    verbose       : if True, show SCIP solver output

    Returns
    -------
    (assignment, optimal_cost)
        assignment   : dict mapping each player to their optimal seed
        optimal_cost : total cost of all chalk matches under that seeding
    """
    n = len(players)
    seeds = list(range(1, n + 1))
    chalk_pairs = chalk_match_pairs(n)
    w = symmetrize_weights(weights)

    model = Model("chalk_bracket")
    if not verbose:
        model.hideOutput()

    # ------------------------------------------------------------------
    # Decision variables: S[p, s] = 1 iff player p gets seed s
    # ------------------------------------------------------------------
    S: dict[tuple[str, int], Any] = {}
    for p in players:
        for s in seeds:
            if s in allowed_seeds.get(p, set()):
                S[(p, s)] = model.addVar(vtype="B", name=f"S_{p}_{s}")
            else:
                S[(p, s)] = model.addVar(vtype="B", name=f"S_{p}_{s}_fixed",
                                         lb=0.0, ub=0.0)

    # ------------------------------------------------------------------
    # Assignment constraints
    # ------------------------------------------------------------------
    for p in players:
        model.addCons(
            sum(S[(p, s)] for s in seeds) == 1,
            name=f"one_seed_{p}",
        )
    for s in seeds:
        model.addCons(
            sum(S[(p, s)] for p in players) == 1,
            name=f"one_player_{s}",
        )

    # ------------------------------------------------------------------
    # Linearization: Z[p1, s1, p2, s2] = S[p1, s1] * S[p2, s2]
    # Only for chalk match-pair seed slots (sparse)
    # ------------------------------------------------------------------
    Z: dict[tuple[str, int, str, int], Any] = {}
    for s1, s2, _rnd in chalk_pairs:
        for p1 in players:
            for p2 in players:
                if p1 == p2:
                    continue
                key = (p1, s1, p2, s2)
                z = model.addVar(vtype="B", name=f"Z_{p1}_{s1}_{p2}_{s2}")
                Z[key] = z
                model.addCons(z <= S[(p1, s1)],                         name=f"Zmcl1_{p1}_{s1}_{p2}_{s2}")
                model.addCons(z <= S[(p2, s2)],                         name=f"Zmcl2_{p1}_{s1}_{p2}_{s2}")
                model.addCons(z >= S[(p1, s1)] + S[(p2, s2)] - 1,      name=f"Zmcl3_{p1}_{s1}_{p2}_{s2}")

    # ------------------------------------------------------------------
    # Objective
    # ------------------------------------------------------------------
    obj = 0.0
    for s1, s2, _rnd in chalk_pairs:
        for p1 in players:
            for p2 in players:
                if p1 == p2:
                    continue
                cost = w[(p1, p2)]
                if cost != 0.0:
                    obj = obj + cost * Z[(p1, s1, p2, s2)]

    model.setObjective(obj, "minimize")
    model.optimize()

    # ------------------------------------------------------------------
    # Extract solution
    # ------------------------------------------------------------------
    status = model.getStatus()
    if status not in ("optimal", "bestsol"):
        raise RuntimeError(f"SCIP did not find an optimal solution (status: {status})")

    assignment: dict[str, int] = {}
    for p in players:
        for s in seeds:
            if model.getVal(S[(p, s)]) > 0.5:
                assignment[p] = s
                break

    return assignment, model.getObjVal()
