"""Shared fixtures and mock data factory for all R&A dashboard tests."""

import sys
import os
import datetime
import json
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Mock Metrics Data ─────────────────────────────────────────────────────────

def _make_period(week_offset, base_date="2026-01-05"):
    """Generate a period_start date string offset by N weeks."""
    base = datetime.date.fromisoformat(base_date)
    d = base + datetime.timedelta(weeks=week_offset)
    return d.isoformat()


def make_metrics_row(week_offset, seed=1.0):
    """Generate one row of /api/metrics response with all 50+ columns."""
    s = seed + week_offset * 0.1
    au = int(180000 + week_offset * 200)
    cu = int(au * 0.045 * s)
    tc = int(cu * 2.3)
    ar = int(au * 120 * s)
    tr = int(au * 800)
    au_auto = int(au * 0.012 * s)
    ta = int(au_auto * 1.5)
    su = int(au * 0.031 * s)
    ts = int(su * 1.2)
    atu = cu + au_auto + su
    total_act = tc + ta + ts
    ra_active = int(au * 0.72 * s)
    ra_owned = int(au * 0.55 * s)

    def rate(n, d):
        return round(min(n / d * 100, 100.0), 2) if d else 0

    def ratio(n, d):
        return round(n / d, 2) if d else 0

    # Prior year: ~90% of current
    py_factor = 0.9
    # Prior week: ~98% of current
    pw_factor = 0.98

    return {
        "period_start": _make_period(week_offset),
        "active_users": au,
        "c1s_users": int(au * 0.6),
        "attributed_rev": ar,
        "total_rev": tr,
        "total_campaigns": tc,
        "campaign_users": cu,
        "ra_active_users": ra_active,
        "automation_users": au_auto,
        "total_automations": ta,
        "segment_users": su,
        "total_segments": ts,
        "actions_taken_users": atu,
        "total_actions": total_act,
        "campaign_sent_users": int(cu * 0.8),
        "total_campaigns_sent": int(tc * 0.7),
        "ra_owned_users": ra_owned,
        # Rates
        "pct_attributed_rev": rate(ar, tr),
        "py_pct_attributed_rev": rate(int(ar * py_factor), int(tr * py_factor)),
        "pw_pct_attributed_rev": rate(int(ar * pw_factor), int(tr * pw_factor)),
        "pct_campaigns_created": rate(cu, au),
        "py_pct_campaigns_created": rate(int(cu * py_factor), int(au * py_factor)),
        "pw_pct_campaigns_created": rate(int(cu * pw_factor), int(au * pw_factor)),
        "campaigns_per_user": ratio(tc, au),
        "py_campaigns_per_user": ratio(int(tc * py_factor), int(au * py_factor)),
        "pw_campaigns_per_user": ratio(int(tc * pw_factor), int(au * pw_factor)),
        "pct_automations_created": rate(au_auto, au),
        "py_pct_automations_created": rate(int(au_auto * py_factor), int(au * py_factor)),
        "pw_pct_automations_created": rate(int(au_auto * pw_factor), int(au * pw_factor)),
        "automations_per_user": ratio(ta, au),
        "py_automations_per_user": ratio(int(ta * py_factor), int(au * py_factor)),
        "pw_automations_per_user": ratio(int(ta * pw_factor), int(au * pw_factor)),
        "pct_segments_created": rate(su, au),
        "py_pct_segments_created": rate(int(su * py_factor), int(au * py_factor)),
        "pw_pct_segments_created": rate(int(su * pw_factor), int(au * pw_factor)),
        "segments_per_user": ratio(ts, au),
        "py_segments_per_user": ratio(int(ts * py_factor), int(au * py_factor)),
        "pw_segments_per_user": ratio(int(ts * pw_factor), int(au * pw_factor)),
        "pct_actions_taken": rate(atu, au),
        "py_pct_actions_taken": rate(int(atu * py_factor), int(au * py_factor)),
        "pw_pct_actions_taken": rate(int(atu * pw_factor), int(au * pw_factor)),
        "pct_ra_owned": rate(ra_owned, au),
        "py_pct_ra_owned": rate(int(ra_owned * py_factor), int(au * py_factor)),
        "pw_pct_ra_owned": rate(int(ra_owned * pw_factor), int(au * pw_factor)),
        "pct_ra_total": rate(ra_active, au),
        "py_pct_ra_total": rate(int(ra_active * py_factor), int(au * py_factor)),
        "pw_pct_ra_total": rate(int(ra_active * pw_factor), int(au * pw_factor)),
    }


@pytest.fixture
def mock_metrics_data():
    """12 weeks of realistic metrics data."""
    return [make_metrics_row(i) for i in range(12)]


@pytest.fixture
def mock_adoption_data():
    """12 weeks of adoption data."""
    rows = []
    for i in range(12):
        au = 185000 + i * 150
        rows.append({
            "period_start": _make_period(i),
            "active_users": au,
            "pct_ra_owned": round(55 + i * 0.3, 2),
            "pct_ra_total": round(72 + i * 0.2, 2),
        })
    return rows


@pytest.fixture
def mock_engagement_data():
    """12 weeks of engagement breakdown data."""
    rows = []
    for i in range(12):
        denom = 185000 + i * 150
        rows.append({
            "period_start": _make_period(i),
            "owned": {
                "Marketing Dashboard": {"users": int(denom * 0.25), "rate": 25.0, "py_users": int(denom * 0.22), "pw_users": int(denom * 0.245), "denominator": denom},
                "Email Report": {"users": int(denom * 0.18), "rate": 18.0, "py_users": int(denom * 0.16), "pw_users": int(denom * 0.175), "denominator": denom},
                "Custom Reports": {"users": int(denom * 0.08), "rate": 8.0, "py_users": int(denom * 0.07), "pw_users": int(denom * 0.078), "denominator": denom},
            },
            "supported": {
                "Homepage": {"users": int(denom * 0.60), "rate": 60.0, "py_users": int(denom * 0.55), "pw_users": int(denom * 0.59), "denominator": denom},
                "All Campaigns": {"users": int(denom * 0.35), "rate": 35.0, "py_users": int(denom * 0.32), "pw_users": int(denom * 0.34), "denominator": denom},
            },
            "owned_engaged": {
                "Marketing Dashboard": {"users": int(denom * 0.15), "rate": 15.0, "py_users": int(denom * 0.13), "pw_users": int(denom * 0.145), "denominator": denom},
            },
            "supported_engaged": {
                "Homepage": {"users": int(denom * 0.40), "rate": 40.0, "py_users": int(denom * 0.36), "pw_users": int(denom * 0.39), "denominator": denom},
            },
            "combined": {
                "RA Viewed (All)": {"users": int(denom * 0.72), "rate": 72.0, "py_users": int(denom * 0.65), "pw_users": int(denom * 0.71), "denominator": denom},
                "RA Engaged (All)": {"users": int(denom * 0.45), "rate": 45.0, "py_users": int(denom * 0.40), "pw_users": int(denom * 0.44), "denominator": denom},
            },
        })
    return rows


# ── Mock BigQuery Row ─────────────────────────────────────────────────────────

class MockBQRow(dict):
    """Dict that also supports attribute access like BigQuery Row objects."""
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)


def bq_rows_from_dicts(dicts):
    """Convert list of dicts to mock BigQuery result rows."""
    return [MockBQRow(d) for d in dicts]


class MockQueryJob:
    """Mock BigQuery QueryJob."""
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


@pytest.fixture
def mock_bq_client(mock_metrics_data):
    """Patch BigQuery client to return mock data."""
    mock_client = MagicMock()

    def mock_query(sql):
        # Convert mock_metrics_data to BQ row format
        rows = []
        for d in mock_metrics_data:
            row = MockBQRow(d)
            # Convert period_start string to date object (like BQ returns)
            row["period_start"] = datetime.date.fromisoformat(d["period_start"])
            rows.append(row)
        return MockQueryJob(rows)

    mock_client.query = mock_query
    return mock_client


@pytest.fixture
def flask_test_client(mock_bq_client):
    """Flask test client with mocked BigQuery."""
    with patch("server.client", mock_bq_client):
        # Clear caches
        import server
        server._cache.clear()
        yield server.app.test_client()


# ── Rate Keys (used across multiple test files) ──────────────────────────────

RATE_KEYS = [
    "pct_attributed_rev", "pct_campaigns_created", "campaigns_per_user",
    "pct_automations_created", "automations_per_user",
    "pct_segments_created", "segments_per_user",
    "pct_actions_taken", "pct_ra_owned", "pct_ra_total",
]

# BigQuery metric names from Dataform UNPIVOT
BQ_UNPIVOT_METRICS = {
    "RA_Supported_Viewed_Users", "RA_Owned_Viewed_Users",
    "Homepage_Viewers", "Automation_Overview_Viewers", "All_Campaigns_Viewers",
    "All_Journeys_Viewers", "All_Contacts_Viewers", "Audience_Dashboard_Viewers",
    "Audience_Segment_Viewers", "SMS_Overview_Viewers",
    "Marketing_Dashboard_Viewers", "Audience_analytic_viewers",
    "Conversion_Insights_viewers", "Custom_Report_Viewers", "Reports_Viewers",
    "SmS_Report_Viewers", "Email_Report_Viewers", "Journey_Report_Viewers",
    "Replicated_Viewers",
    "RA_Engaged_Users", "RA_Supported_Engaged_Users", "RA_Owned_Engaged_Users",
    "Homepage_Engaged", "All_campaign_Engaged", "Automation_overview_Engaged_users",
    "All_journeys_Engaged", "All_contacts_Engaged", "Audience_dashboard_Engaged",
    "Audience_Segment_Engaged", "Sms_overview_Engaged",
    "Marketing_Dashboard_Engaged", "Audience_analytic_Engaged",
    "Conversion_Insights_Engaged", "Custom_analytics_Engaged", "Report_Engaged",
    "SmS_Report_Engaged", "Email_Report_Engaged", "CJB_Report_Engaged",
    "RA_Viewed_Users", "actions_taken_users", "total_campaigns_created",
    "total_campaigns_sent", "total_automations_created", "total_segments_created",
    "total_actions", "campaign_created_users", "campaign_sent_users",
    "automation_created_users", "segment_created_users",
    "c2_users", "total_attributable_revenue",
    "total_c2s_collected_from_form_or_integrations",
    "c1s_collecting_c2s_from_form_or_integrations",
    "total_attributable_orders", "total_attributable_aov",
    "ecomm_integration_users", "one_plus_ecomm_integration_users",
    "ecomm_users_engaging_with_RA",
}
