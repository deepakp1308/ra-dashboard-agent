"""Config completeness and cross-reference validation tests."""

import re
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (
    ENGAGEMENT_TAXONOMY, ALL_ENGAGEMENT_METRICS, CORE_METRICS,
    I2A_METRICS, METRIC_DISPLAY, FUNNEL_STAGES, ACTION_TEMPLATES,
)
from tests.conftest import RATE_KEYS, BQ_UNPIVOT_METRICS


# ── METRIC_DISPLAY completeness ──────────────────────────────────────────────

def test_all_rate_keys_have_display_config():
    """Every rate key used in executive summary must have a METRIC_DISPLAY entry."""
    for key in RATE_KEYS:
        assert key in METRIC_DISPLAY, f"Missing METRIC_DISPLAY for '{key}'"


def test_metric_display_entries_have_required_fields():
    """Each METRIC_DISPLAY entry must have label, format, higher_is_better."""
    for key, conf in METRIC_DISPLAY.items():
        assert "label" in conf, f"{key}: missing 'label'"
        assert "format" in conf, f"{key}: missing 'format'"
        assert "higher_is_better" in conf, f"{key}: missing 'higher_is_better'"
        assert conf["format"] in ("pct", "ratio", "count", "currency"), \
            f"{key}: invalid format '{conf['format']}'"
        assert isinstance(conf["higher_is_better"], bool), \
            f"{key}: higher_is_better must be bool"


def test_metric_display_labels_are_unique():
    """No two metrics should have the same display label."""
    labels = [v["label"] for v in METRIC_DISPLAY.values()]
    assert len(labels) == len(set(labels)), "Duplicate labels in METRIC_DISPLAY"


# ── FUNNEL_STAGES validation ─────────────────────────────────────────────────

def test_funnel_stages_have_required_fields():
    for stage in FUNNEL_STAGES:
        assert "key" in stage, "Stage missing 'key'"
        assert "label" in stage, "Stage missing 'label'"
        assert isinstance(stage["key"], str)
        assert isinstance(stage["label"], str)
        assert len(stage["label"]) > 0


def test_funnel_stages_keys_are_valid():
    """Funnel stage keys must match actual data field names."""
    valid_keys = {
        "active_users", "ra_viewed_users", "ra_owned_users",
        "ra_engaged_users", "actions_taken_users",
    }
    for stage in FUNNEL_STAGES:
        assert stage["key"] in valid_keys, f"Invalid funnel key: {stage['key']}"


def test_funnel_stages_ordered_broadest_first():
    """First stage must be broadest (active_users), last must be narrowest."""
    assert FUNNEL_STAGES[0]["key"] == "active_users"
    assert FUNNEL_STAGES[-1]["key"] == "actions_taken_users"


# ── ACTION_TEMPLATES validation ──────────────────────────────────────────────

def test_action_templates_use_valid_placeholders():
    """Templates must only use {segment} and {metric} placeholders."""
    valid_placeholders = {"segment", "metric"}
    for category, templates in ACTION_TEMPLATES.items():
        assert isinstance(templates, list), f"{category}: must be a list"
        assert len(templates) > 0, f"{category}: empty template list"
        for tmpl in templates:
            placeholders = set(re.findall(r"\{(\w+)\}", tmpl))
            invalid = placeholders - valid_placeholders
            assert not invalid, \
                f"{category}: invalid placeholders {invalid} in '{tmpl}'"


def test_action_templates_cover_expected_categories():
    """Should have templates for key scenarios."""
    expected = {
        "declining_engagement", "rising_engagement",
        "low_conversion", "high_conversion",
        "anomaly_positive", "anomaly_negative",
    }
    assert set(ACTION_TEMPLATES.keys()) == expected


# ── I2A / CORE metrics consistency ───────────────────────────────────────────

def test_i2a_metrics_subset_of_core():
    """All I2A metrics must be in CORE_METRICS."""
    missing = set(I2A_METRICS) - set(CORE_METRICS)
    assert not missing, f"I2A metrics not in CORE: {missing}"


def test_core_metrics_match_bigquery_unpivot():
    """All CORE_METRICS must exist in the BigQuery UNPIVOT output."""
    missing = set(CORE_METRICS) - BQ_UNPIVOT_METRICS
    assert not missing, f"CORE_METRICS not in BQ UNPIVOT: {missing}"


# ── ENGAGEMENT_TAXONOMY combined group ───────────────────────────────────────

def test_combined_taxonomy_has_all_entries():
    combined = ENGAGEMENT_TAXONOMY["combined"]
    expected_keys = {
        "RA Viewed (All)", "RA Engaged (All)", "RA Owned Viewed",
        "RA Owned Engaged", "RA Supported Engaged",
    }
    assert set(combined.keys()) == expected_keys


def test_all_engagement_metrics_in_bigquery():
    """Every metric in the taxonomy must exist in BigQuery UNPIVOT."""
    missing = set(ALL_ENGAGEMENT_METRICS) - BQ_UNPIVOT_METRICS
    assert not missing, f"Engagement metrics not in BQ: {missing}"
