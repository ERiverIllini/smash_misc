"""Tests for utils.py — weight symmetrization and feasibility checks."""

from bracket_chalk_milp.utils import is_feasible, symmetrize_weights


class TestSymmetrizeWeights:
    def test_missing_pair_returns_zero(self):
        w = symmetrize_weights({("A", "B"): 3.0})
        assert w[("X", "Y")] == 0.0

    def test_explicit_direction_preserved(self):
        raw = {("A", "B"): 3.0}
        w = symmetrize_weights(raw)
        assert w[("A", "B")] == 3.0

    def test_reverse_filled_when_missing(self):
        w = symmetrize_weights({("A", "B"): 3.0})
        assert w[("B", "A")] == 3.0

    def test_asymmetric_both_directions_respected(self):
        raw = {("A", "B"): 3.0, ("B", "A"): 7.0}
        w = symmetrize_weights(raw)
        assert w[("A", "B")] == 3.0
        assert w[("B", "A")] == 7.0

    def test_empty_input(self):
        w = symmetrize_weights({})
        assert w[("A", "B")] == 0.0


class TestIsFeasible:
    def test_all_allowed(self):
        assignment = {"A": 1, "B": 2, "C": 3}
        allowed = {"A": {1, 2}, "B": {2}, "C": {3}}
        assert is_feasible(assignment, allowed) is True

    def test_one_violation(self):
        assignment = {"A": 1, "B": 3, "C": 3}
        allowed = {"A": {1}, "B": {2}, "C": {3}}
        assert is_feasible(assignment, allowed) is False

    def test_player_missing_from_allowed_seeds(self):
        # Player with no entry in allowed_seeds → infeasible for any seed
        assignment = {"A": 1}
        allowed = {}
        assert is_feasible(assignment, allowed) is False

    def test_singleton_fixed_seed(self):
        assignment = {"C": 3}
        allowed = {"C": {3}}
        assert is_feasible(assignment, allowed) is True

    def test_singleton_wrong_seed(self):
        assignment = {"C": 2}
        allowed = {"C": {3}}
        assert is_feasible(assignment, allowed) is False
