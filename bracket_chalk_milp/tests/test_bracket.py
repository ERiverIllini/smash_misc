"""Tests for bracket.py — bracket structure and chalk match-pair generation."""

import pytest

from bracket_chalk_milp.bracket import _standard_bracket_slots, chalk_match_pairs


class TestStandardBracketSlots:
    def test_n2(self):
        assert _standard_bracket_slots(2) == [1, 2]

    def test_n4(self):
        # Round-1 pairs: (1 vs 4), (2 vs 3)
        assert _standard_bracket_slots(4) == [1, 4, 2, 3]

    def test_n8(self):
        slots = _standard_bracket_slots(8)
        assert len(slots) == 8
        # Adjacent pairs in round 1 should sum to n+1=9
        for i in range(0, 8, 2):
            assert slots[i] + slots[i + 1] == 9

    def test_n1(self):
        assert _standard_bracket_slots(1) == [1]


class TestChalkMatchPairs:
    def test_n4_exact(self):
        pairs = chalk_match_pairs(4)
        assert set(pairs) == {(1, 4, 1), (2, 3, 1), (1, 2, 2)}

    def test_n2_exact(self):
        pairs = chalk_match_pairs(2)
        assert pairs == [(1, 2, 1)]

    def test_n8_count(self):
        # 8-player bracket has 7 total matches
        pairs = chalk_match_pairs(8)
        assert len(pairs) == 7

    def test_n8_seed1_reaches_final(self):
        pairs = chalk_match_pairs(8)
        num_rounds = 3
        # Seed 1 must appear in a match in every round under chalk
        for rnd in range(1, num_rounds + 1):
            assert any(s1 == 1 or s2 == 1 for s1, s2, r in pairs if r == rnd)

    def test_seed_a_less_than_seed_b(self):
        for n in (2, 4, 8):
            for s1, s2, _ in chalk_match_pairs(n):
                assert s1 < s2

    def test_invalid_n_raises(self):
        with pytest.raises(ValueError):
            chalk_match_pairs(3)

    def test_invalid_n1_raises(self):
        with pytest.raises(ValueError):
            chalk_match_pairs(1)
