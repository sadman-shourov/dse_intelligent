from __future__ import annotations

from analysis.engine import _positive_float_list


def test_positive_float_list_accepts_valid_prices():
    assert _positive_float_list([10, "11.5", 12.0]) == [10.0, 11.5, 12.0]


def test_positive_float_list_rejects_zero_null_and_non_finite_prices():
    assert _positive_float_list([10, 0, 12]) is None
    assert _positive_float_list([10, None, 12]) is None
    assert _positive_float_list([10, float("nan"), 12]) is None
