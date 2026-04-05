"""Tests for WoW/YoY comparison logic."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analytics import compute_trends, _pct_change


# ── WoW Delta tests ──────────────────────────────────────────────────────────

def test_wow_delta_from_pw_column():
    """When pw_ columns are present, WoW delta should use them."""
    data = [
        {"period_start": "2026-03-16", "val": 80, "pw_val": 75},
        {"period_start": "2026-03-23", "val": 100, "pw_val": 80},
    ]
    trends = compute_trends(data, ["val"])
    t = trends["val"]
    # WoW should be (100 - 80) / 80 * 100 = 25%
    assert t["wow_delta"] is not None
    assert abs(t["wow_delta"] - 25.0) < 0.1


def test_wow_delta_fallback_to_adjacent():
    """When pw_ columns are absent, WoW delta falls back to adjacent values."""
    data = [
        {"period_start": "2026-03-16", "val": 80},
        {"period_start": "2026-03-23", "val": 100},
    ]
    trends = compute_trends(data, ["val"])
    t = trends["val"]
    # Should use adjacent: (100 - 80) / 80 * 100 = 25%
    assert t["wow_delta"] is not None
    assert abs(t["wow_delta"] - 25.0) < 0.1


# ── YoY Delta tests ──────────────────────────────────────────────────────────

def test_yoy_delta_from_py_column():
    """YoY delta should use py_ columns."""
    data = [
        {"period_start": "2026-03-16", "val": 100, "py_val": 90},
        {"period_start": "2026-03-23", "val": 110, "py_val": 95},
    ]
    trends = compute_trends(data, ["val"])
    t = trends["val"]
    # YoY: (110 - 95) / 95 * 100 ≈ 15.8%
    assert t["yoy_delta"] is not None
    assert abs(t["yoy_delta"] - 15.789) < 0.1


def test_yoy_delta_missing_py():
    """YoY delta should be None when py_ column is missing."""
    data = [
        {"period_start": "2026-03-16", "val": 100},
        {"period_start": "2026-03-23", "val": 110},
    ]
    trends = compute_trends(data, ["val"])
    t = trends["val"]
    assert t["yoy_delta"] is None


# ── Comparison with None values ──────────────────────────────────────────────

def test_delta_with_none_current():
    """None current value should result in None delta."""
    assert _pct_change(None, 100) is None


def test_delta_with_none_prior():
    """None prior value should result in None delta."""
    assert _pct_change(100, None) is None


def test_delta_with_zero_prior():
    """Zero prior value should result in None (avoid division by zero)."""
    assert _pct_change(100, 0) is None


def test_delta_negative_change():
    """Negative change should be reported correctly."""
    result = _pct_change(80, 100)
    assert result == -20.0


def test_delta_no_change():
    """No change should be 0%."""
    result = _pct_change(100, 100)
    assert result == 0.0


# ── Comparison across metrics ────────────────────────────────────────────────

def test_multiple_metrics_comparison():
    """Trends should work with multiple metrics simultaneously."""
    data = [
        {"period_start": "2026-01-01", "metric_a": 10, "metric_b": 50,
         "pw_metric_a": 8, "pw_metric_b": 55},
        {"period_start": "2026-01-08", "metric_a": 12, "metric_b": 48,
         "pw_metric_a": 10, "pw_metric_b": 50},
    ]
    trends = compute_trends(data, ["metric_a", "metric_b"])

    assert "metric_a" in trends
    assert "metric_b" in trends

    # metric_a: increasing
    assert trends["metric_a"]["wow_delta"] is not None
    assert trends["metric_a"]["wow_delta"] > 0  # 12 vs pw=10 = +20%

    # metric_b: decreasing
    assert trends["metric_b"]["wow_delta"] is not None
    assert trends["metric_b"]["wow_delta"] < 0  # 48 vs pw=50 = -4%


# ── Edge cases ───────────────────────────────────────────────────────────────

def test_single_data_point():
    """Single data point should not produce trends."""
    data = [{"period_start": "2026-01-01", "val": 100}]
    trends = compute_trends(data, ["val"])
    assert trends == {}


def test_all_none_values():
    """All None values should produce no trends."""
    data = [
        {"period_start": "2026-01-01", "val": None},
        {"period_start": "2026-01-08", "val": None},
    ]
    trends = compute_trends(data, ["val"])
    assert "val" not in trends


def test_mixed_none_values():
    """Mix of None and real values should still compute what it can."""
    data = [
        {"period_start": "2026-01-01", "val": None},
        {"period_start": "2026-01-08", "val": 50},
        {"period_start": "2026-01-15", "val": 60},
    ]
    trends = compute_trends(data, ["val"])
    assert "val" in trends
    assert trends["val"]["current"] == 60
    assert trends["val"]["previous"] == 50
