"""Data quality tests — SQL correctness, field matching, rate formulas, aggregation checks."""

import sys
import os
import re
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import (
    CORE_METRICS, ALL_ENGAGEMENT_METRICS, ENGAGEMENT_TAXONOMY,
    I2A_METRICS,
)
from tests.conftest import BQ_UNPIVOT_METRICS, RATE_KEYS


# ═══════════════════════════════════════════════════════════════════════════════
#  A. FIELD-LEVEL CORRECTNESS
# ═══════════════════════════════════════════════════════════════════════════════

class TestFieldCorrectness:

    def test_core_metrics_exact_match_to_bigquery(self):
        """Every CORE_METRIC must exist in Dataform UNPIVOT output."""
        for metric in CORE_METRICS:
            assert metric in BQ_UNPIVOT_METRICS, \
                f"'{metric}' not in BigQuery UNPIVOT — typo or renamed?"

    def test_engagement_metrics_exact_match_to_bigquery(self):
        """Every engagement metric in taxonomy must exist in Dataform UNPIVOT."""
        for metric in ALL_ENGAGEMENT_METRICS:
            assert metric in BQ_UNPIVOT_METRICS, \
                f"Engagement metric '{metric}' not in BigQuery UNPIVOT"

    def test_no_orphaned_engagement_metrics(self):
        """Every metric in ALL_ENGAGEMENT_METRICS must appear in some taxonomy group."""
        taxonomy_metrics = set()
        for group in ENGAGEMENT_TAXONOMY.values():
            taxonomy_metrics.update(group.values())
        for metric in ALL_ENGAGEMENT_METRICS:
            assert metric in taxonomy_metrics, \
                f"'{metric}' in ALL_ENGAGEMENT_METRICS but not in any taxonomy group"

    def test_filter_column_names_in_sql(self):
        """Verify server.py filter helpers use correct column names."""
        import server
        # Check helpers that generate the SQL clauses
        assert "ecomm_level_end" in server._ecu_clause("all")
        assert "is_high_value" in server._hvc_clause("all")

    def test_hvc_type_consistency(self):
        """Report table uses STRING is_high_value; rollup uses BOOLEAN.
        Server must handle both correctly."""
        import server
        metrics_src = inspect.getsource(server.get_metrics)
        adoption_src = inspect.getsource(server.get_adoption)
        # Metrics query should use string comparison
        assert "'true'" in metrics_src or "string_type=True" in metrics_src \
            or "_hvc_clause(hvc)" in metrics_src
        # Adoption query should use boolean
        assert "string_type=False" in adoption_src


# ═══════════════════════════════════════════════════════════════════════════════
#  B. SQL STRUCTURE CORRECTNESS
# ═══════════════════════════════════════════════════════════════════════════════

class TestSQLCorrectness:

    def _get_metrics_sql(self):
        """Extract SQL template from get_metrics function source."""
        import server
        return inspect.getsource(server.get_metrics)

    def _get_adoption_sql(self):
        import server
        return inspect.getsource(server.get_adoption)

    def _get_engagement_sql(self):
        import server
        return inspect.getsource(server.get_engagement)

    def test_metrics_uses_safe_divide(self):
        """All rate calculations must use SAFE_DIVIDE to avoid division-by-zero."""
        src = self._get_metrics_sql()
        # Count SAFE_DIVIDE occurrences — should be many (one per rate)
        count = src.count("SAFE_DIVIDE")
        assert count >= 10, f"Only {count} SAFE_DIVIDE calls — rates may crash on 0 denominator"

    def test_metrics_coalesces_py(self):
        """Prior year must be COALESCEd to 0 to prevent NULL arithmetic."""
        src = self._get_metrics_sql()
        assert "COALESCE(py, 0)" in src, "Missing COALESCE(py, 0)"
        assert "COALESCE(py_denominator, 0)" in src, "Missing COALESCE(py_denominator, 0)"

    def test_metrics_coalesces_pw(self):
        """Prior week must be COALESCEd to 0."""
        src = self._get_metrics_sql()
        assert "COALESCE(pw, 0)" in src, "Missing COALESCE(pw, 0)"
        assert "COALESCE(pw_denominator, 0)" in src, "Missing COALESCE(pw_denominator, 0)"

    def test_metrics_groups_by_period(self):
        src = self._get_metrics_sql()
        assert "GROUP BY" in src

    def test_metrics_orders_by_period(self):
        src = self._get_metrics_sql()
        assert "ORDER BY period_start ASC" in src

    def test_metrics_filters_null_period(self):
        src = self._get_metrics_sql()
        assert "period_start IS NOT NULL" in src

    def test_adoption_uses_dedup_union(self):
        """Adoption must use OR (union) not + (sum) to avoid double counting."""
        src = self._get_adoption_sql()
        assert "is_owned = 1 OR is_cta = 1" in src, \
            "Adoption should use OR for de-duplication, not addition"

    def test_adoption_filters_null_is_high_value(self):
        """Adoption WAU base must filter is_high_value IS NOT NULL."""
        src = self._get_adoption_sql()
        assert "is_high_value IS NOT NULL" in src, \
            "Missing is_high_value IS NOT NULL — WAU denominator will be wrong (~909K vs ~185K)"

    def test_adoption_excludes_current_week(self):
        """Adoption should not include incomplete current week."""
        src = self._get_adoption_sql()
        assert "DATE_TRUNC(CURRENT_DATE, WEEK)" in src

    def test_engagement_groups_by_metric(self):
        src = self._get_engagement_sql()
        assert "GROUP BY" in src
        assert "metric_name" in src


# ═══════════════════════════════════════════════════════════════════════════════
#  C. AGGREGATION / RATE FORMULA CORRECTNESS
# ═══════════════════════════════════════════════════════════════════════════════

class TestAggregationCorrectness:

    def test_pct_campaigns_formula_in_sql(self):
        """pct_campaigns_created = campaign_users / active_users * 100"""
        import server
        src = inspect.getsource(server.get_metrics)
        assert "SAFE_DIVIDE(campaign_users" in src
        assert "active_users)" in src

    def test_campaigns_per_user_formula(self):
        """campaigns_per_user = total_campaigns / active_users (NOT campaign_users)"""
        import server
        src = inspect.getsource(server.get_metrics)
        assert "SAFE_DIVIDE(total_campaigns" in src

    def test_pct_attributed_rev_formula(self):
        """pct_attributed_rev = attributed_rev / total_rev * 100"""
        import server
        src = inspect.getsource(server.get_metrics)
        assert "SAFE_DIVIDE(attributed_rev" in src
        assert "total_rev)" in src

    def test_wow_rates_use_pw_columns(self):
        """WoW rates must use pw_ prefixed columns, not adjacent rows."""
        import server
        src = inspect.getsource(server.get_metrics)
        assert "pw_campaign_users" in src
        assert "pw_active_users" in src
        assert "pw_attributed_rev" in src

    def test_yoy_rates_use_py_columns(self):
        """YoY rates must use py_ prefixed columns."""
        import server
        src = inspect.getsource(server.get_metrics)
        assert "py_campaign_users" in src
        assert "py_active_users" in src

    def test_rate_values_in_range(self, mock_metrics_data):
        """All computed rates should be 0-105 (allow minor overshoot from rounding)."""
        for row in mock_metrics_data:
            for key in RATE_KEYS:
                val = row.get(key)
                if val is not None:
                    assert 0 <= val <= 105, \
                        f"{key}={val} out of range in period {row['period_start']}"

    def test_prior_rates_in_range(self, mock_metrics_data):
        """Prior week/year rates should also be in reasonable range."""
        for row in mock_metrics_data:
            for prefix in ("py_", "pw_"):
                for key in RATE_KEYS:
                    val = row.get(prefix + key)
                    if val is not None:
                        assert 0 <= val <= 105, \
                            f"{prefix}{key}={val} out of range"


# ═══════════════════════════════════════════════════════════════════════════════
#  D. DATA INTEGRITY RULES
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataIntegrity:

    def test_no_negative_user_counts(self, mock_metrics_data):
        user_cols = ["active_users", "campaign_users", "automation_users",
                     "segment_users", "actions_taken_users", "ra_active_users",
                     "ra_owned_users"]
        for row in mock_metrics_data:
            for col in user_cols:
                val = row.get(col)
                if val is not None:
                    assert val >= 0, f"{col}={val} is negative"

    def test_periods_sorted_ascending(self, mock_metrics_data):
        dates = [r["period_start"] for r in mock_metrics_data]
        assert dates == sorted(dates), "Periods not sorted ascending"

    def test_no_duplicate_periods(self, mock_metrics_data):
        dates = [r["period_start"] for r in mock_metrics_data]
        assert len(dates) == len(set(dates)), "Duplicate period_start values"

    def test_period_start_is_valid_date(self, mock_metrics_data):
        import datetime
        for row in mock_metrics_data:
            ps = row["period_start"]
            # Should parse as ISO date
            try:
                datetime.date.fromisoformat(ps)
            except ValueError:
                assert False, f"Invalid date: {ps}"

    def test_active_users_positive_when_data_exists(self, mock_metrics_data):
        for row in mock_metrics_data:
            assert row["active_users"] > 0, \
                f"active_users=0 in {row['period_start']} — denominator will break rates"

    def test_numerators_lte_denominators(self, mock_metrics_data):
        """Campaign users should not exceed active users."""
        for row in mock_metrics_data:
            au = row.get("active_users", 0)
            for num_col in ["campaign_users", "automation_users", "segment_users",
                            "actions_taken_users"]:
                val = row.get(num_col, 0)
                if val is not None and au > 0:
                    assert val <= au * 1.01, \
                        f"{num_col}={val} > active_users={au} — numerator exceeds denominator"


# ═══════════════════════════════════════════════════════════════════════════════
#  E. ADOPTION DE-DUPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestAdoptionDedup:

    def test_total_gte_owned(self, mock_adoption_data):
        """pct_ra_total must always >= pct_ra_owned (de-duped union >= subset)."""
        for row in mock_adoption_data:
            owned = row["pct_ra_owned"]
            total = row["pct_ra_total"]
            assert total >= owned - 0.01, \
                f"De-dup failed: total={total} < owned={owned} in {row['period_start']}"

    def test_adoption_rates_in_range(self, mock_adoption_data):
        for row in mock_adoption_data:
            for key in ["pct_ra_owned", "pct_ra_total"]:
                val = row[key]
                assert 0 <= val <= 100.5, f"{key}={val} out of range"


# ═══════════════════════════════════════════════════════════════════════════════
#  F. ENGAGEMENT TAXONOMY COVERAGE
# ═══════════════════════════════════════════════════════════════════════════════

class TestEngagementCoverage:

    def test_all_owned_pages_mapped(self):
        """All 9 owned page viewer metrics must be in taxonomy."""
        owned = ENGAGEMENT_TAXONOMY["owned_viewers"]
        assert len(owned) == 9

    def test_all_supported_pages_mapped(self):
        """All 8 supported page viewer metrics must be in taxonomy."""
        supported = ENGAGEMENT_TAXONOMY["supported_viewers"]
        assert len(supported) == 8

    def test_owned_engaged_count(self):
        """8 owned engaged metrics (no Replicated engagement)."""
        assert len(ENGAGEMENT_TAXONOMY["owned_engaged"]) == 8

    def test_supported_engaged_count(self):
        assert len(ENGAGEMENT_TAXONOMY["supported_engaged"]) == 8

    def test_engagement_display_names_human_readable(self):
        """Display names should not contain underscores or be ALL_CAPS."""
        for group_name, group in ENGAGEMENT_TAXONOMY.items():
            for display_name in group.keys():
                assert "_" not in display_name, \
                    f"{group_name}: '{display_name}' has underscore — use human name"
                assert display_name != display_name.upper() or len(display_name) <= 3, \
                    f"{group_name}: '{display_name}' looks like a raw metric name"

    def test_engagement_data_shape(self, mock_engagement_data):
        """Each engagement period must have expected sections."""
        for period in mock_engagement_data:
            assert "period_start" in period
            assert "owned" in period
            assert "supported" in period
            assert "combined" in period
            # Each page entry must have users, rate, denominator
            for section in ["owned", "supported"]:
                for page_name, entry in period[section].items():
                    assert "users" in entry, f"{page_name}: missing 'users'"
                    assert "rate" in entry, f"{page_name}: missing 'rate'"
                    assert "denominator" in entry, f"{page_name}: missing 'denominator'"

    def test_engagement_rate_matches_formula(self, mock_engagement_data):
        """rate should equal users / denominator * 100."""
        for period in mock_engagement_data:
            for section in ["owned", "supported"]:
                for page_name, entry in period[section].items():
                    users = entry["users"]
                    denom = entry["denominator"]
                    rate = entry["rate"]
                    if denom > 0:
                        expected = round(users / denom * 100, 1)
                        assert abs(rate - expected) < 1.0, \
                            f"{page_name}: rate={rate}, expected~{expected}"
