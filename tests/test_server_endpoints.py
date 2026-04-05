"""Tests for Flask server endpoints and filter helpers (BigQuery mocked)."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server import _ecu_clause, _hvc_clause, _period_expr, _metrics_in_clause


# ═══════════════════════════════════════════════════════════════════════════════
#  FILTER CLAUSE GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestEcuClause:
    def test_ecu_only(self):
        assert _ecu_clause("ecu") == "ecomm_level_end = 'ecu'"

    def test_non_ecu(self):
        assert _ecu_clause("non_ecu") == "ecomm_level_end IN ('non', 'ecomm')"

    def test_all(self):
        assert _ecu_clause("all") == "ecomm_level_end IN ('ecu', 'non', 'ecomm')"

    def test_with_prefix(self):
        assert _ecu_clause("ecu", "w") == "w.ecomm_level_end = 'ecu'"

    def test_non_ecu_with_prefix(self):
        assert _ecu_clause("non_ecu", "t") == "t.ecomm_level_end IN ('non', 'ecomm')"

    def test_all_with_prefix(self):
        assert _ecu_clause("all", "x") == "x.ecomm_level_end IN ('ecu', 'non', 'ecomm')"

    def test_unknown_value_treated_as_all(self):
        result = _ecu_clause("bogus")
        assert "'ecu'" in result and "'non'" in result


class TestHvcClause:
    def test_hvc_string_type(self):
        assert _hvc_clause("hvc") == "is_high_value = 'true'"

    def test_non_hvc_string_type(self):
        assert _hvc_clause("non_hvc") == "is_high_value = 'false'"

    def test_all_string_type(self):
        assert _hvc_clause("all") == "is_high_value IN ('true', 'false')"

    def test_hvc_boolean_type(self):
        assert _hvc_clause("hvc", "", string_type=False) == "is_high_value = TRUE"

    def test_non_hvc_boolean_type(self):
        assert _hvc_clause("non_hvc", "", string_type=False) == "is_high_value = FALSE"

    def test_all_boolean_type(self):
        assert _hvc_clause("all", "", string_type=False) == "is_high_value IN (TRUE, FALSE)"

    def test_with_prefix_boolean(self):
        assert _hvc_clause("hvc", "w", string_type=False) == "w.is_high_value = TRUE"

    def test_with_prefix_string(self):
        assert _hvc_clause("hvc", "t", string_type=True) == "t.is_high_value = 'true'"

    def test_adoption_uses_boolean_metrics_uses_string(self):
        """Adoption query table has BOOLEAN is_high_value; report table has STRING."""
        adoption_clause = _hvc_clause("hvc", "w", string_type=False)
        metrics_clause = _hvc_clause("hvc", string_type=True)
        assert "TRUE" in adoption_clause
        assert "'true'" in metrics_clause


class TestPeriodExpr:
    def test_weekly(self):
        assert _period_expr("weekly") == "week"

    def test_monthly(self):
        assert _period_expr("monthly") == "DATE_TRUNC(week, MONTH)"

    def test_custom_column(self):
        assert _period_expr("monthly", "b.week") == "DATE_TRUNC(b.week, MONTH)"

    def test_weekly_custom_column(self):
        assert _period_expr("weekly", "b.week") == "b.week"


class TestMetricsInClause:
    def test_single_metric(self):
        result = _metrics_in_clause(["foo"])
        assert result == "'foo'"

    def test_multiple_metrics(self):
        result = _metrics_in_clause(["a", "b", "c"])
        assert result == "'a', 'b', 'c'"

    def test_empty_list(self):
        result = _metrics_in_clause([])
        assert result == ""


# ═══════════════════════════════════════════════════════════════════════════════
#  ENDPOINT RESPONSE SHAPE TESTS (with mocked BigQuery)
# ═══════════════════════════════════════════════════════════════════════════════

class TestMetricsEndpoint:
    def test_returns_200(self, flask_test_client):
        resp = flask_test_client.get("/api/metrics?ecu=all&hvc=all&granularity=weekly")
        assert resp.status_code == 200

    def test_returns_list(self, flask_test_client):
        resp = flask_test_client.get("/api/metrics")
        data = resp.get_json()
        assert isinstance(data, list)

    def test_rows_have_period_start(self, flask_test_client):
        data = flask_test_client.get("/api/metrics").get_json()
        if data:
            assert "period_start" in data[0]
            assert isinstance(data[0]["period_start"], str)

    def test_rows_have_core_rate_columns(self, flask_test_client):
        data = flask_test_client.get("/api/metrics").get_json()
        if data:
            row = data[0]
            for col in ["pct_attributed_rev", "pct_campaigns_created",
                        "campaigns_per_user", "active_users"]:
                assert col in row, f"Missing column: {col}"

    def test_rows_have_wow_columns(self, flask_test_client):
        """WoW (pw_) columns must be present."""
        data = flask_test_client.get("/api/metrics").get_json()
        if data:
            row = data[0]
            for col in ["pw_pct_attributed_rev", "pw_pct_campaigns_created",
                        "pw_campaigns_per_user"]:
                assert col in row, f"Missing WoW column: {col}"

    def test_rows_have_yoy_columns(self, flask_test_client):
        """YoY (py_) columns must be present."""
        data = flask_test_client.get("/api/metrics").get_json()
        if data:
            row = data[0]
            for col in ["py_pct_attributed_rev", "py_pct_campaigns_created",
                        "py_campaigns_per_user"]:
                assert col in row, f"Missing YoY column: {col}"

    def test_rows_have_i2a_expansion_columns(self, flask_test_client):
        """I2A expansion columns must be present."""
        data = flask_test_client.get("/api/metrics").get_json()
        if data:
            row = data[0]
            for col in ["automation_users", "total_automations",
                        "segment_users", "total_segments",
                        "actions_taken_users", "total_actions",
                        "pct_automations_created", "pct_segments_created",
                        "pct_actions_taken"]:
                assert col in row, f"Missing I2A column: {col}"

    def test_sorted_by_period(self, flask_test_client):
        data = flask_test_client.get("/api/metrics").get_json()
        if len(data) >= 2:
            dates = [r["period_start"] for r in data]
            assert dates == sorted(dates)


class TestExecSummaryEndpoint:
    def test_returns_200(self, flask_test_client):
        resp = flask_test_client.get("/api/executive-summary?compare=wow")
        assert resp.status_code == 200

    def test_response_has_required_fields(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary?compare=wow").get_json()
        assert "period" in data
        assert "comparison_mode" in data
        assert "summary" in data
        assert "analytics" in data

    def test_comparison_mode_echoed(self, flask_test_client):
        wow = flask_test_client.get("/api/executive-summary?compare=wow").get_json()
        yoy = flask_test_client.get("/api/executive-summary?compare=yoy").get_json()
        assert wow["comparison_mode"] == "wow"
        assert yoy["comparison_mode"] == "yoy"

    def test_summary_is_list(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary").get_json()
        assert isinstance(data["summary"], list)

    def test_summary_max_8_bullets(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary").get_json()
        assert len(data["summary"]) <= 8

    def test_bullet_structure(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary").get_json()
        for bullet in data["summary"]:
            assert "category" in bullet
            assert "severity" in bullet
            assert "text" in bullet
            assert "metric" in bullet
            assert "score" not in bullet, "Score should be stripped from output"

    def test_bullet_categories_valid(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary").get_json()
        valid = {"trend", "anomaly", "opportunity", "threat", "action"}
        for bullet in data["summary"]:
            assert bullet["category"] in valid, \
                f"Invalid category: {bullet['category']}"

    def test_analytics_section_present(self, flask_test_client):
        data = flask_test_client.get("/api/executive-summary").get_json()
        analytics = data["analytics"]
        assert "trends" in analytics
        assert "anomalies" in analytics
        assert "funnel" in analytics
        assert "top_correlations" in analytics


class TestIndexRoute:
    def test_serves_html(self, flask_test_client):
        resp = flask_test_client.get("/")
        assert resp.status_code == 200
        assert b"<!DOCTYPE html>" in resp.data
        assert b"R&A Executive Dashboard" in resp.data
