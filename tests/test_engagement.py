"""Tests for engagement taxonomy grouping and funnel analysis."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (
    ENGAGEMENT_TAXONOMY,
    ALL_ENGAGEMENT_METRICS,
    FUNNEL_STAGES,
    I2A_METRICS,
    CORE_METRICS,
)
from analytics import compute_funnel


# ── Taxonomy structure tests ──────────────────────────────────────────────────

def test_taxonomy_has_all_categories():
    """Taxonomy should have owned_viewers, supported_viewers, owned_engaged, supported_engaged, combined."""
    expected = {"owned_viewers", "supported_viewers", "owned_engaged", "supported_engaged", "combined"}
    assert set(ENGAGEMENT_TAXONOMY.keys()) == expected


def test_owned_viewers_pages():
    """Owned viewers should include all known R&A owned pages."""
    owned = ENGAGEMENT_TAXONOMY["owned_viewers"]
    expected_pages = {
        "Marketing Dashboard", "Audience Analytics", "Conversion Insights",
        "Custom Reports", "Reports", "SMS Report", "Email Report",
        "Journey Report", "Replicated",
    }
    assert set(owned.keys()) == expected_pages


def test_supported_viewers_pages():
    """Supported viewers should include all known supported pages."""
    supported = ENGAGEMENT_TAXONOMY["supported_viewers"]
    expected_pages = {
        "Homepage", "All Campaigns", "All Journeys", "Automation Overview",
        "All Contacts", "Audience Dashboard", "Audience Segments", "SMS Overview",
    }
    assert set(supported.keys()) == expected_pages


def test_owned_engaged_pages():
    """Owned engaged should have entries matching the owned pages (minus Replicated)."""
    owned_eng = ENGAGEMENT_TAXONOMY["owned_engaged"]
    # Replicated doesn't have engagement tracking
    assert "Marketing Dashboard" in owned_eng
    assert "Email Report" in owned_eng
    assert len(owned_eng) == 8  # 9 owned minus Replicated


def test_supported_engaged_pages():
    """Supported engaged should match supported viewers pages."""
    supported_eng = ENGAGEMENT_TAXONOMY["supported_engaged"]
    assert "Homepage" in supported_eng
    assert "All Campaigns" in supported_eng
    assert len(supported_eng) == 8


# ── Metric name consistency tests ─────────────────────────────────────────────

def test_all_engagement_metrics_unique():
    """All engagement metric names should be unique."""
    assert len(ALL_ENGAGEMENT_METRICS) == len(set(ALL_ENGAGEMENT_METRICS))


def test_taxonomy_values_are_strings():
    """All taxonomy values should be strings (metric names)."""
    for group_name, group in ENGAGEMENT_TAXONOMY.items():
        for display_name, metric_name in group.items():
            assert isinstance(metric_name, str), \
                f"Non-string value in {group_name}.{display_name}: {metric_name}"


def test_no_duplicate_display_names_within_group():
    """Within each taxonomy group, display names should be unique."""
    for group_name, group in ENGAGEMENT_TAXONOMY.items():
        names = list(group.keys())
        assert len(names) == len(set(names)), \
            f"Duplicate display names in {group_name}"


# ── I2A Metrics tests ────────────────────────────────────────────────────────

def test_i2a_metrics_include_all_action_types():
    """I2A metrics should cover campaigns, automations, segments."""
    assert "campaign_created_users" in I2A_METRICS
    assert "automation_created_users" in I2A_METRICS
    assert "segment_created_users" in I2A_METRICS
    assert "actions_taken_users" in I2A_METRICS
    assert "total_actions" in I2A_METRICS


def test_core_metrics_include_i2a():
    """Core metrics should include the I2A expansion metrics."""
    assert "automation_created_users" in CORE_METRICS
    assert "total_automations_created" in CORE_METRICS
    assert "segment_created_users" in CORE_METRICS
    assert "total_segments_created" in CORE_METRICS


# ── Funnel tests ──────────────────────────────────────────────────────────────

def test_funnel_stages_order():
    """Funnel stages should be ordered from broadest to most specific."""
    keys = [s["key"] for s in FUNNEL_STAGES]
    assert keys[0] == "active_users"
    assert keys[-1] == "actions_taken_users"
    assert len(keys) == 5


def test_funnel_dropoff_calculation():
    """Funnel should correctly calculate drop-off rates."""
    data = {
        "active_users": 1000,
        "ra_viewed_users": 700,
        "ra_owned_users": 400,
        "ra_engaged_users": 200,
        "actions_taken_users": 50,
    }
    funnel = compute_funnel(data)

    assert funnel[0]["rate"] == 100.0
    assert funnel[1]["rate"] == 70.0
    assert funnel[1]["dropoff_rate"] == 30.0  # 30% drop from 1000 to 700

    # Rate should be percentage of total (first stage)
    assert funnel[4]["rate"] == 5.0  # 50/1000 = 5%


def test_funnel_bottleneck_detection():
    """Funnel should identify the stage with highest drop-off as bottleneck."""
    data = {
        "active_users": 1000,
        "ra_viewed_users": 900,    # 10% drop
        "ra_owned_users": 300,     # 66.7% drop — bottleneck
        "ra_engaged_users": 250,   # 16.7% drop
        "actions_taken_users": 200, # 20% drop
    }
    funnel = compute_funnel(data)

    bottlenecks = [s for s in funnel if s["is_bottleneck"]]
    assert len(bottlenecks) == 1
    assert bottlenecks[0]["stage"] == "ra_owned_users"


def test_funnel_equal_stages():
    """When all values are equal, there's no meaningful bottleneck."""
    data = {
        "active_users": 100,
        "ra_viewed_users": 100,
        "ra_owned_users": 100,
        "ra_engaged_users": 100,
        "actions_taken_users": 100,
    }
    funnel = compute_funnel(data)
    # All dropoff rates should be 0
    assert all(s["dropoff_rate"] == 0 for s in funnel)


def test_funnel_progressive_halving():
    """Test with each stage halving the previous."""
    data = {
        "active_users": 1000,
        "ra_viewed_users": 500,
        "ra_owned_users": 250,
        "ra_engaged_users": 125,
        "actions_taken_users": 62,
    }
    funnel = compute_funnel(data)
    # Each stage should show ~50% dropoff
    for s in funnel[1:]:
        assert 49 <= s["dropoff_rate"] <= 51


# ── Engagement classification tests ──────────────────────────────────────────

def test_engagement_metric_names_match_bigquery():
    """Metric names in taxonomy should match BigQuery column names from Dataform."""
    # These are the exact names from the UNPIVOT in rpt_RA_L1L3_Test_03_04.sqlx
    bq_owned_viewers = {
        "Marketing_Dashboard_Viewers", "Audience_analytic_viewers",
        "Conversion_Insights_viewers", "Custom_Report_Viewers",
        "Reports_Viewers", "SmS_Report_Viewers", "Email_Report_Viewers",
        "Journey_Report_Viewers", "Replicated_Viewers",
    }
    taxonomy_owned = set(ENGAGEMENT_TAXONOMY["owned_viewers"].values())
    assert taxonomy_owned == bq_owned_viewers


def test_engagement_supported_viewers_match_bigquery():
    bq_supported = {
        "Homepage_Viewers", "All_Campaigns_Viewers", "All_Journeys_Viewers",
        "Automation_Overview_Viewers", "All_Contacts_Viewers",
        "Audience_Dashboard_Viewers", "Audience_Segment_Viewers",
        "SMS_Overview_Viewers",
    }
    taxonomy_supported = set(ENGAGEMENT_TAXONOMY["supported_viewers"].values())
    assert taxonomy_supported == bq_supported


def test_engagement_owned_engaged_match_bigquery():
    bq_owned_engaged = {
        "Marketing_Dashboard_Engaged", "Audience_analytic_Engaged",
        "Conversion_Insights_Engaged", "Custom_analytics_Engaged",
        "Report_Engaged", "SmS_Report_Engaged", "Email_Report_Engaged",
        "CJB_Report_Engaged",
    }
    taxonomy_owned_engaged = set(ENGAGEMENT_TAXONOMY["owned_engaged"].values())
    assert taxonomy_owned_engaged == bq_owned_engaged


def test_engagement_supported_engaged_match_bigquery():
    bq_supported_engaged = {
        "Homepage_Engaged", "All_campaign_Engaged",
        "Automation_overview_Engaged_users", "All_journeys_Engaged",
        "All_contacts_Engaged", "Audience_dashboard_Engaged",
        "Audience_Segment_Engaged", "Sms_overview_Engaged",
    }
    taxonomy_supported_engaged = set(ENGAGEMENT_TAXONOMY["supported_engaged"].values())
    assert taxonomy_supported_engaged == bq_supported_engaged
