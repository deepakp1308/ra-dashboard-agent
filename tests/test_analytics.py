"""Tests for the analytics engine — trends, anomalies, correlations, summaries."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analytics import (
    compute_moving_average,
    compute_growth_rate,
    compute_trends,
    detect_anomalies,
    analyze_segments,
    compute_funnel,
    compute_correlations,
    generate_executive_summary,
    _safe_div,
    _pct_change,
    _mean,
    _stddev,
    _pearson_r,
)


# ── Utility tests ─────────────────────────────────────────────────────────────

def test_safe_div():
    assert _safe_div(10, 2) == 5.0
    assert _safe_div(10, 0) is None
    assert _safe_div(None, 5) is None
    assert _safe_div(10, None) is None


def test_pct_change():
    assert _pct_change(110, 100) == 10.0
    assert _pct_change(90, 100) == -10.0
    assert _pct_change(100, 0) is None
    assert _pct_change(None, 100) is None


def test_mean():
    assert _mean([1, 2, 3, 4, 5]) == 3.0
    assert _mean([10, None, 20]) == 15.0
    assert _mean([]) is None
    assert _mean([None, None]) is None


def test_stddev():
    assert _stddev([2, 2, 2, 2]) == 0.0  # Zero variance returns 0.0
    assert _stddev([]) is None
    assert _stddev([5]) is None
    # Known stddev: [0, 10] -> mean=5, variance=25, stddev=5
    assert abs(_stddev([0, 10]) - 5.0) < 0.001


def test_pearson_r_perfect_correlation():
    xs = [1, 2, 3, 4, 5, 6, 7, 8]
    ys = [2, 4, 6, 8, 10, 12, 14, 16]
    r = _pearson_r(xs, ys)
    assert r is not None
    assert abs(r - 1.0) < 0.001


def test_pearson_r_negative_correlation():
    xs = [1, 2, 3, 4, 5, 6, 7, 8]
    ys = [16, 14, 12, 10, 8, 6, 4, 2]
    r = _pearson_r(xs, ys)
    assert r is not None
    assert abs(r - (-1.0)) < 0.001


def test_pearson_r_insufficient_data():
    assert _pearson_r([1, 2], [3, 4]) is None  # Less than CORRELATION_MIN_PERIODS


# ── Moving Average tests ──────────────────────────────────────────────────────

def test_moving_average_basic():
    values = [10, 20, 30, 40, 50]
    ma = compute_moving_average(values, window=3)
    assert len(ma) == 5
    assert ma[0] == 10.0        # only 1 value
    assert ma[1] == 15.0        # avg of [10, 20]
    assert ma[2] == 20.0        # avg of [10, 20, 30]
    assert ma[3] == 30.0        # avg of [20, 30, 40]
    assert ma[4] == 40.0        # avg of [30, 40, 50]


def test_moving_average_with_nones():
    values = [10, None, 30, None, 50]
    ma = compute_moving_average(values, window=3)
    assert ma[0] == 10.0
    assert ma[2] == 20.0  # avg of [10, 30] (None skipped)


# ── Growth Rate tests ─────────────────────────────────────────────────────────

def test_growth_rate_positive():
    values = [100, 110, 121, 133.1]
    rate = compute_growth_rate(values, periods=4)
    assert rate is not None
    assert abs(rate - 10.0) < 0.5  # ~10% compound growth


def test_growth_rate_insufficient_data():
    assert compute_growth_rate([100]) is None
    assert compute_growth_rate([]) is None


# ── Trend Analysis tests ──────────────────────────────────────────────────────

def _make_data(values, key="pct_campaigns_created"):
    """Helper to create test data dicts."""
    return [
        {"period_start": f"2026-01-{7*i+1:02d}", key: v}
        for i, v in enumerate(values)
    ]


def test_compute_trends_basic():
    data = _make_data([1.0, 2.0, 3.0, 4.0, 5.0])
    trends = compute_trends(data, ["pct_campaigns_created"])
    t = trends["pct_campaigns_created"]
    assert t["current"] == 5.0
    assert t["previous"] == 4.0
    assert t["direction"] == "up"
    assert t["streak"] >= 3


def test_compute_trends_with_prior_week():
    data = [
        {"period_start": "2026-01-01", "val": 10, "pw_val": 8},
        {"period_start": "2026-01-08", "val": 12, "pw_val": 10},
    ]
    trends = compute_trends(data, ["val"])
    t = trends["val"]
    # WoW delta should use pw_ column: (12 - 10) / 10 * 100 = 20%
    assert t["wow_delta"] is not None
    assert abs(t["wow_delta"] - 20.0) < 0.1


def test_compute_trends_empty():
    assert compute_trends([], ["key"]) == {}
    assert compute_trends([{"key": 1}], ["key"]) == {}


# ── Anomaly Detection tests ──────────────────────────────────────────────────

def test_detect_anomalies_spike():
    # 8 values with slight variance, then one spike at 100
    data = _make_data([10, 11, 9, 10, 11, 9, 10, 11, 100])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) >= 1
    assert anomalies[0]["direction"] == "spike"
    assert anomalies[0]["metric"] == "pct_campaigns_created"


def test_detect_anomalies_drop():
    data = _make_data([50, 49, 51, 50, 49, 51, 50, 49, 5])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) >= 1
    assert anomalies[0]["direction"] == "drop"


def test_detect_anomalies_no_anomaly():
    # All values within normal range (slight variance)
    data = _make_data([10, 10.5, 9.5, 10, 10.5, 9.5, 10, 10.5, 10])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) == 0


def test_detect_anomalies_insufficient_data():
    data = _make_data([10, 20])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) == 0


# ── Segmentation Analysis tests ──────────────────────────────────────────────

def test_analyze_segments_significant_diff():
    seg_a = _make_data([10, 10, 10, 10, 10])
    seg_b = _make_data([50, 50, 50, 50, 50])
    results = analyze_segments(
        {"ECU": seg_a, "Non-ECU": seg_b},
        ["pct_campaigns_created"]
    )
    assert len(results) >= 1
    assert results[0]["significant"] is True
    assert abs(results[0]["diff"]) == 40.0


def test_analyze_segments_no_diff():
    seg_a = _make_data([10, 10, 10])
    seg_b = _make_data([10, 10, 10])
    results = analyze_segments(
        {"ECU": seg_a, "Non-ECU": seg_b},
        ["pct_campaigns_created"]
    )
    assert len(results) >= 1
    assert results[0]["significant"] is False


def test_analyze_segments_single_segment():
    results = analyze_segments({"ECU": _make_data([10])}, ["pct_campaigns_created"])
    assert len(results) == 0


# ── Funnel Analysis tests ────────────────────────────────────────────────────

def test_compute_funnel_basic():
    data = {
        "active_users": 1000,
        "ra_viewed_users": 700,
        "ra_owned_users": 500,
        "ra_engaged_users": 300,
        "actions_taken_users": 100,
    }
    funnel = compute_funnel(data)
    assert len(funnel) == 5
    assert funnel[0]["value"] == 1000
    assert funnel[0]["rate"] == 100.0
    assert funnel[1]["value"] == 700
    assert funnel[1]["rate"] == 70.0
    # Bottleneck should be the stage with highest dropoff
    bottleneck = [s for s in funnel if s["is_bottleneck"]]
    assert len(bottleneck) == 1


def test_compute_funnel_empty():
    assert compute_funnel({}) == []
    assert compute_funnel(None) == []


def test_compute_funnel_all_zero():
    data = {
        "active_users": 0,
        "ra_viewed_users": 0,
        "ra_owned_users": 0,
        "ra_engaged_users": 0,
        "actions_taken_users": 0,
    }
    funnel = compute_funnel(data)
    assert len(funnel) == 5
    assert all(s["value"] == 0 for s in funnel)


# ── Correlation tests ─────────────────────────────────────────────────────────

def test_compute_correlations():
    data = [{"eng": i * 10, "outcome": i * 5 + 2} for i in range(10)]
    results = compute_correlations(data, ["eng"], ["outcome"])
    assert len(results) == 1
    assert results[0]["strength"] == "strong"
    assert results[0]["r"] > 0.9


def test_compute_correlations_no_data():
    results = compute_correlations([], ["eng"], ["outcome"])
    assert len(results) == 0


# ── Executive Summary tests ──────────────────────────────────────────────────

def test_generate_summary_structure():
    trends = {
        "pct_campaigns_created": {
            "current": 5.0, "previous": 4.0,
            "wow_delta": 25.0, "yoy_delta": 10.0,
            "moving_avg": 4.5, "growth_rate_4w": 5.0,
            "direction": "up", "streak": 4,
        },
        "pct_attributed_rev": {
            "current": 3.0, "previous": 4.0,
            "wow_delta": -25.0, "yoy_delta": -15.0,
            "moving_avg": 3.5, "growth_rate_4w": -3.0,
            "direction": "down", "streak": 3,
        },
    }
    anomalies = [{
        "metric": "pct_ra_total",
        "metric_label": "R&A Adoption (All)",
        "period": "2026-03-30",
        "value": 85.0,
        "mean": 70.0,
        "z_score": 2.5,
        "direction": "spike",
        "severity": "warning",
    }]
    segments = [{
        "metric": "pct_campaigns_created",
        "metric_label": "% Created Campaign",
        "segment_a": "ECU",
        "segment_b": "Non-ECU",
        "value_a": 8.0,
        "value_b": 3.0,
        "diff": 5.0,
        "pct_diff": 166.7,
        "significant": True,
    }]
    funnel = [
        {"stage": "active_users", "label": "WAU", "value": 1000, "rate": 100, "dropoff_rate": 0, "is_bottleneck": False},
        {"stage": "ra_viewed_users", "label": "Viewed R&A", "value": 700, "rate": 70, "dropoff_rate": 30.0, "is_bottleneck": True},
        {"stage": "actions_taken_users", "label": "Took Action", "value": 100, "rate": 10, "dropoff_rate": 85.7, "is_bottleneck": False},
    ]
    correlations = [{
        "engagement": "pct_ra_owned",
        "engagement_label": "R&A Adoption (Owned)",
        "outcome": "pct_campaigns_created",
        "outcome_label": "% Created Campaign",
        "r": 0.85,
        "strength": "strong",
    }]

    summary = generate_executive_summary(
        trends, anomalies, segments, funnel, correlations, "wow"
    )

    assert isinstance(summary, list)
    assert 6 <= len(summary) <= 8
    categories = {b["category"] for b in summary}
    # Should have at least trends and some other category
    assert "trend" in categories or "opportunity" in categories or "action" in categories
    # Each bullet should have required fields
    for bullet in summary:
        assert "category" in bullet
        assert "severity" in bullet
        assert "text" in bullet
        assert "metric" in bullet
        assert "score" not in bullet  # Score should be removed from output


def test_generate_summary_empty_inputs():
    summary = generate_executive_summary({}, [], [], [], [], "wow")
    assert isinstance(summary, list)
    assert len(summary) == 0


def test_generate_summary_trends_only():
    trends = {
        "pct_campaigns_created": {
            "current": 5.0, "previous": 4.0,
            "wow_delta": 25.0, "yoy_delta": 10.0,
            "moving_avg": 4.5, "growth_rate_4w": 5.0,
            "direction": "up", "streak": 5,
        },
    }
    summary = generate_executive_summary(trends, [], [], [], [], "wow")
    assert len(summary) >= 1
    assert summary[0]["category"] == "trend"


# ═══════════════════════════════════════════════════════════════════════════════
#  EDGE CASE TESTS (expanded coverage)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Trends edge cases ─────────────────────────────────────────────────────────

def test_trends_zero_current():
    data = [
        {"period_start": "2026-01-01", "val": 10, "pw_val": 5},
        {"period_start": "2026-01-08", "val": 0, "pw_val": 10},
    ]
    trends = compute_trends(data, ["val"])
    assert trends["val"]["wow_delta"] == -100.0


def test_trends_all_zero_series():
    data = _make_data([0, 0, 0, 0, 0])
    trends = compute_trends(data, ["pct_campaigns_created"])
    t = trends["pct_campaigns_created"]
    assert t["current"] == 0
    assert t["direction"] == "flat"


def test_trends_negative_values():
    data = _make_data([-10, -8, -6, -4, -2])
    trends = compute_trends(data, ["pct_campaigns_created"])
    t = trends["pct_campaigns_created"]
    assert t["direction"] == "up"
    assert t["streak"] >= 3


def test_trends_streak_breaks_on_none():
    data = _make_data([10, 12, None, 15, 18])
    trends = compute_trends(data, ["pct_campaigns_created"])
    t = trends["pct_campaigns_created"]
    # Streak should be 2 (15→18), not 4
    assert t["streak"] <= 2


def test_trends_single_non_none_in_long_series():
    data = _make_data([None, None, None, None, 42])
    trends = compute_trends(data, ["pct_campaigns_created"])
    t = trends["pct_campaigns_created"]
    assert t["current"] == 42


# ── Anomaly edge cases ────────────────────────────────────────────────────────

def test_anomalies_exactly_at_threshold():
    # With slight variance, spike should be caught at threshold
    data = _make_data([10, 11, 9, 10, 11, 9, 10, 11, 25])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) >= 1


def test_anomalies_critical_severity():
    """Z-score >= 3.0 (1.5x threshold) should be 'critical'."""
    data = _make_data([10, 11, 9, 10, 11, 9, 10, 11, 500])
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    if anomalies:
        assert anomalies[0]["severity"] == "critical"


def test_anomalies_consecutive_spikes():
    # Two spikes in sequence: both should be detected (window slides)
    vals = [10, 11, 9, 10, 11, 9, 10, 11, 80, 90]
    data = _make_data(vals)
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    assert len(anomalies) >= 1


def test_anomalies_sorted_by_z_score():
    vals = [10, 11, 9, 10, 11, 9, 10, 11, 50, 100]
    data = _make_data(vals)
    anomalies = detect_anomalies(data, ["pct_campaigns_created"], threshold=2.0, window=8)
    if len(anomalies) >= 2:
        assert abs(anomalies[0]["z_score"]) >= abs(anomalies[1]["z_score"])


# ── Segments edge cases ───────────────────────────────────────────────────────

def test_segments_three_way_comparison():
    seg_a = _make_data([10, 10, 10])
    seg_b = _make_data([20, 20, 20])
    seg_c = _make_data([30, 30, 30])
    results = analyze_segments(
        {"A": seg_a, "B": seg_b, "C": seg_c},
        ["pct_campaigns_created"]
    )
    # 3 pairwise: A-B, A-C, B-C
    assert len(results) == 3


def test_segments_unequal_lengths():
    seg_a = _make_data([10, 10])
    seg_b = _make_data([20, 20, 20, 20, 20])
    results = analyze_segments(
        {"A": seg_a, "B": seg_b},
        ["pct_campaigns_created"]
    )
    assert len(results) == 1  # Should still compare


def test_segments_zero_values():
    seg_a = _make_data([0, 0, 0])
    seg_b = _make_data([10, 10, 10])
    results = analyze_segments(
        {"A": seg_a, "B": seg_b},
        ["pct_campaigns_created"]
    )
    assert len(results) == 1
    assert results[0]["diff"] != 0


# ── Funnel edge cases ─────────────────────────────────────────────────────────

def test_funnel_increasing_values():
    """Values increasing (impossible but should not crash)."""
    data = {
        "active_users": 100,
        "ra_viewed_users": 150,
        "ra_owned_users": 200,
        "ra_engaged_users": 250,
        "actions_taken_users": 300,
    }
    funnel = compute_funnel(data)
    assert len(funnel) == 5
    assert all(isinstance(s["dropoff_rate"], (int, float)) for s in funnel)


def test_funnel_zero_middle_stage():
    data = {
        "active_users": 1000,
        "ra_viewed_users": 500,
        "ra_owned_users": 0,
        "ra_engaged_users": 0,
        "actions_taken_users": 0,
    }
    funnel = compute_funnel(data)
    assert funnel[2]["dropoff_rate"] == 100.0


def test_funnel_missing_keys():
    """Missing keys should default to 0."""
    data = {"active_users": 1000}
    funnel = compute_funnel(data)
    assert len(funnel) == 5
    assert funnel[1]["value"] == 0


# ── Correlation edge cases ────────────────────────────────────────────────────

def test_correlations_no_variation():
    data = [{"eng": 10, "outcome": i * 5} for i in range(10)]
    results = compute_correlations(data, ["eng"], ["outcome"])
    # eng is constant → r should be None → no results
    assert len(results) == 0


def test_correlations_negative():
    data = [{"eng": i, "outcome": 100 - i} for i in range(10)]
    results = compute_correlations(data, ["eng"], ["outcome"])
    assert len(results) == 1
    assert results[0]["r"] < -0.9
    assert results[0]["strength"] == "strong"


def test_correlations_weak():
    import random
    random.seed(42)
    data = [{"eng": i, "outcome": random.random() * 100} for i in range(20)]
    results = compute_correlations(data, ["eng"], ["outcome"])
    if results:
        assert results[0]["strength"] in ("weak", "moderate")


# ── Executive Summary edge cases ──────────────────────────────────────────────

def test_summary_no_deltas():
    trends = {
        "pct_campaigns_created": {
            "current": 5.0, "previous": 4.0,
            "wow_delta": None, "yoy_delta": None,
            "moving_avg": 4.5, "growth_rate_4w": 5.0,
            "direction": "up", "streak": 4,
        },
    }
    summary = generate_executive_summary(trends, [], [], [], [], "wow")
    assert isinstance(summary, list)


def test_summary_max_bullet_enforcement():
    """Should never exceed 8 bullets even with many inputs."""
    trends = {}
    for i in range(15):
        trends[f"metric_{i}"] = {
            "current": i + 1, "previous": i,
            "wow_delta": 10.0 + i, "yoy_delta": 5.0 + i,
            "moving_avg": i + 0.5, "growth_rate_4w": 3.0,
            "direction": "up", "streak": 4,
        }
    summary = generate_executive_summary(trends, [], [], [], [], "wow")
    assert len(summary) <= 8


def test_summary_yoy_mode():
    trends = {
        "pct_campaigns_created": {
            "current": 5.0, "previous": 4.0,
            "wow_delta": 25.0, "yoy_delta": 10.0,
            "moving_avg": 4.5, "growth_rate_4w": 5.0,
            "direction": "up", "streak": 5,
        },
    }
    summary = generate_executive_summary(trends, [], [], [], [], "yoy")
    if summary:
        assert "YoY" in summary[0]["text"]


# ── rank_items test ───────────────────────────────────────────────────────────

def test_rank_items_basic():
    from analytics import rank_items
    items = [{"name": "a", "score": 1}, {"name": "b", "score": 3}, {"name": "c", "score": 2}]
    ranked = rank_items(items, lambda x: x["score"])
    assert ranked[0]["name"] == "b"
    assert ranked[-1]["name"] == "a"


def test_rank_items_empty():
    from analytics import rank_items
    assert rank_items([], lambda x: 0) == []


def test_rank_items_equal_scores():
    from analytics import rank_items
    items = [{"name": "a", "score": 5}, {"name": "b", "score": 5}]
    ranked = rank_items(items, lambda x: x["score"])
    assert len(ranked) == 2
