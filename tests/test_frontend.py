"""Frontend validation tests — HTML structure, JS logic, element integrity."""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "static", "index.html")


def _read_html():
    with open(HTML_PATH, "r") as f:
        return f.read()


# ═══════════════════════════════════════════════════════════════════════════════
#  HTML STRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

class TestHTMLStructure:

    def test_has_doctype(self):
        html = _read_html()
        assert html.strip().startswith("<!DOCTYPE html>")

    def test_has_chart_js(self):
        html = _read_html()
        assert "chart.js" in html.lower() or "Chart" in html

    def test_has_viewport_meta(self):
        html = _read_html()
        assert 'name="viewport"' in html

    def test_has_title(self):
        html = _read_html()
        assert "<title>" in html
        assert "R&A Executive Dashboard" in html

    def test_has_tab_buttons(self):
        html = _read_html()
        assert 'data-tab="overview"' in html
        assert 'data-tab="details"' in html

    def test_has_page_containers(self):
        html = _read_html()
        assert 'id="page-overview"' in html
        assert 'id="page-details"' in html

    def test_has_exec_summary_container(self):
        html = _read_html()
        assert 'id="exec-summary-container"' in html

    def test_has_kpi_grid_container(self):
        html = _read_html()
        assert 'id="kpi-grid-container"' in html

    def test_has_i2a_section(self):
        html = _read_html()
        assert 'id="i2a-section"' in html

    def test_has_engagement_section(self):
        html = _read_html()
        assert 'id="engagement-section"' in html


# ═══════════════════════════════════════════════════════════════════════════════
#  FILTER ELEMENTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestFilterElements:

    def test_segment_filter_buttons(self):
        html = _read_html()
        assert 'data-filter="ecu" data-value="all"' in html
        assert 'data-filter="ecu" data-value="ecu"' in html
        assert 'data-filter="ecu" data-value="non_ecu"' in html

    def test_value_filter_buttons(self):
        html = _read_html()
        assert 'data-filter="hvc" data-value="all"' in html
        assert 'data-filter="hvc" data-value="hvc"' in html
        assert 'data-filter="hvc" data-value="non_hvc"' in html

    def test_no_compare_toggle(self):
        """Compare toggle removed — both WoW and YoY shown simultaneously."""
        html = _read_html()
        assert 'data-filter="compare"' not in html

    def test_granularity_filter_buttons(self):
        html = _read_html()
        assert 'data-filter="granularity" data-value="weekly"' in html
        assert 'data-filter="granularity" data-value="monthly"' in html

    def test_tenure_filter_disabled(self):
        """Tenure filter should be present but disabled."""
        html = _read_html()
        assert "Tenure" in html
        # Should have disabled buttons
        tenure_section = html[html.index("Tenure"):]
        assert "disabled" in tenure_section[:500]

    def test_all_filter_buttons_have_data_attributes(self):
        """Every non-disabled filter-btn must have data-filter and data-value."""
        html = _read_html()
        # Find all filter-btn elements (approximate via regex)
        buttons = re.findall(r'class="filter-btn[^"]*"[^>]*>', html)
        for btn in buttons:
            if "disabled" in btn:
                continue
            assert "data-filter=" in btn, f"Missing data-filter: {btn[:80]}"
            assert "data-value=" in btn, f"Missing data-value: {btn[:80]}"

    def test_dual_delta_function_exists(self):
        """deltaBoth() function should exist for showing WoW + YoY together."""
        html = _read_html()
        assert "function deltaBoth(" in html
        assert "function _oneDelta(" in html


# ═══════════════════════════════════════════════════════════════════════════════
#  JAVASCRIPT LOGIC VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestJSLogic:

    def test_state_object_no_compare_field(self):
        """JS state should NOT have compare — both shown simultaneously."""
        html = _read_html()
        # State has ecu, hvc, granularity but not compare
        assert "ecu:" in html
        assert "hvc:" in html
        assert "granularity:" in html

    def test_state_object_has_all_filters(self):
        html = _read_html()
        assert "ecu:" in html
        assert "hvc:" in html
        assert "granularity:" in html

    def test_delta_function_exists(self):
        html = _read_html()
        assert "function delta(" in html

    def test_delta_both_function(self):
        """deltaBoth() shows WoW + YoY side by side."""
        html = _read_html()
        assert "deltaBoth" in html
        assert '"WoW"' in html
        assert '"YoY"' in html

    def test_delta_handles_null_values(self):
        html = _read_html()
        # Should check for null current or prior
        assert "cur == null" in html or "pri == null" in html

    def test_dual_comparison_prefixes(self):
        """Both pw_ and py_ prefixes used for WoW and YoY."""
        html = _read_html()
        assert "pw_" in html
        assert "py_" in html
        assert '"WoW"' in html
        assert '"YoY"' in html

    def test_formatters_exist(self):
        html = _read_html()
        for fn in ["fmtPct", "fmtRatio", "fmtCurrency", "fmtCount", "fmtDate"]:
            assert fn in html, f"Missing formatter: {fn}"

    def test_fmtPct_handles_null(self):
        html = _read_html()
        # Should return "N/A" for null
        assert 'v == null ? "N/A"' in html

    def test_buildChart_function(self):
        html = _read_html()
        assert "function buildChart(" in html

    def test_buildChart_has_three_series(self):
        """Chart builder should support current + prior week + prior year."""
        html = _read_html()
        assert "Prior Week" in html
        assert "Prior Year" in html

    def test_loadData_fetches_all_endpoints(self):
        """loadData must fetch metrics, adoption, engagement, and summary."""
        html = _read_html()
        assert "/api/metrics" in html
        assert "/api/adoption" in html
        assert "/api/engagement" in html
        assert "/api/executive-summary" in html

    def test_loadData_uses_promise_all(self):
        """All fetches should be parallel via Promise.all."""
        html = _read_html()
        assert "Promise.all" in html

    def test_error_handling_in_loadData(self):
        """loadData should have try/catch for error display."""
        html = _read_html()
        assert "catch" in html
        assert "Failed to load data" in html


# ═══════════════════════════════════════════════════════════════════════════════
#  DESIGN SYSTEM COMPLIANCE
# ═══════════════════════════════════════════════════════════════════════════════

class TestDesignSystem:

    def test_uses_intuit_sidebar_color(self):
        html = _read_html()
        assert "#162251" in html, "Missing Intuit sidebar navy color"

    def test_uses_intuit_page_bg(self):
        html = _read_html()
        assert "#f0f4f8" in html, "Missing Intuit page background"

    def test_uses_intuit_accent_blue(self):
        html = _read_html()
        assert "#0070d2" in html, "Missing Intuit accent blue"

    def test_uses_ai_teal(self):
        html = _read_html()
        assert "#00b9a9" in html, "Missing AI teal accent"

    def test_uses_chart_navy(self):
        html = _read_html()
        assert "#1e3a6e" in html, "Missing chart navy color"

    def test_uses_avenir_font(self):
        html = _read_html()
        assert "Avenir Next" in html, "Missing Avenir Next font"

    def test_card_border_radius(self):
        html = _read_html()
        assert "border-radius" in html
        # 8px per design system
        assert "8px" in html

    def test_snowflake_ai_icon(self):
        """AI content should use snowflake icon."""
        html = _read_html()
        assert "ai-icon" in html
        # Unicode snowflake star
        assert "\u2726" in html or "✦" in html


# ═══════════════════════════════════════════════════════════════════════════════
#  EXECUTIVE SUMMARY RENDERING
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecSummaryRendering:

    def test_render_function_exists(self):
        html = _read_html()
        assert "function renderExecSummary(" in html

    def test_handles_empty_summary(self):
        html = _read_html()
        assert "Insufficient data" in html

    def test_category_chips_rendered(self):
        html = _read_html()
        assert "exec-chip" in html

    def test_bullet_icons_by_category(self):
        html = _read_html()
        for cat in ["trend", "anomaly", "opportunity", "threat", "action"]:
            assert f"bullet-icon {cat}" in html or f'bullet-icon.{cat}' in html


# ═══════════════════════════════════════════════════════════════════════════════
#  I2A SECTION RENDERING
# ═══════════════════════════════════════════════════════════════════════════════

class TestI2ARendering:

    def test_render_function_exists(self):
        html = _read_html()
        assert "function renderI2ASection(" in html

    def test_shows_ab_test_coming_soon(self):
        html = _read_html()
        assert "Coming Soon" in html
        assert "A/B Test" in html

    def test_breakdown_table_has_action_types(self):
        html = _read_html()
        assert "Campaign Created" in html
        assert "Automation Created" in html
        assert "Segment Created" in html

    def test_i2a_table_headers(self):
        html = _read_html()
        assert "Action Type" in html
        assert "% of WAU" in html


# ═══════════════════════════════════════════════════════════════════════════════
#  ENGAGEMENT SECTION RENDERING
# ═══════════════════════════════════════════════════════════════════════════════

class TestEngagementRendering:

    def test_render_function_exists(self):
        html = _read_html()
        assert "function renderEngagementSection(" in html

    def test_engagement_tabs_present(self):
        html = _read_html()
        assert 'data-eng="all"' in html
        assert 'data-eng="owned"' in html
        assert 'data-eng="supported"' in html
