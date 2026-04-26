"""Tests for solver.py — PySCIPOpt MILP correctness."""

import pytest

from bracket_chalk_milp.solver import build_and_solve


PLAYERS = ["A", "B", "C", "D"]
ALLOWED_SEEDS = {
    "A": {1, 2},
    "B": {1, 2},
    "C": {3},
    "D": {4},
}


class TestBuildAndSolve:
    def test_returns_valid_assignment(self):
        weights = {("A", "B"): 1.0}
        assignment, _ = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert set(assignment.keys()) == set(PLAYERS)
        assert set(assignment.values()) == {1, 2, 3, 4}

    def test_fixed_seeds_respected(self):
        weights = {("A", "B"): 1.0}
        assignment, _ = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert assignment["C"] == 3
        assert assignment["D"] == 4

    def test_a_or_b_gets_seed_1(self):
        weights = {("A", "B"): 1.0}
        assignment, _ = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert assignment["A"] in {1, 2}
        assert assignment["B"] in {1, 2}
        assert assignment["A"] != assignment["B"]

    def test_optimal_chooses_cheaper_seeding(self):
        # Seeding A=1,B=2: cost = W(A,D) + W(B,C) + W(A,B) = 1 + 6 + 5 = 12
        # Seeding B=1,A=2: cost = W(B,D) + W(A,C) + W(A,B) = 2 + 3 + 5 = 10  <- optimal
        weights = {
            ("A", "D"): 1.0,
            ("B", "D"): 2.0,
            ("A", "C"): 3.0,
            ("B", "C"): 6.0,
            ("A", "B"): 5.0,
        }
        assignment, cost = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert assignment["B"] == 1
        assert assignment["A"] == 2
        assert abs(cost - 10.0) < 1e-6

    def test_objective_value_matches_manual_calculation(self):
        # Seeding A=1,B=2: cost = W(A,D) + W(B,C) + W(A,B) = 10 + 20 + 30 = 60
        # Seeding B=1,A=2: cost = W(B,D) + W(A,C) + W(A,B) = 1 + 1 + 30 = 32  <- optimal
        weights = {
            ("A", "D"): 10.0,
            ("B", "D"): 1.0,
            ("A", "C"): 1.0,
            ("B", "C"): 20.0,
            ("A", "B"): 30.0,
        }
        _, cost = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert abs(cost - 32.0) < 1e-6

    def test_zero_weights_gives_zero_cost(self):
        weights = {}
        _, cost = build_and_solve(PLAYERS, ALLOWED_SEEDS, weights)
        assert abs(cost) < 1e-6

    def test_infeasible_raises(self):
        # No valid assignment: A and B both restricted to seed 1
        bad_allowed = {"A": {1}, "B": {1}, "C": {3}, "D": {4}}
        with pytest.raises(Exception):
            build_and_solve(PLAYERS, bad_allowed, {})
