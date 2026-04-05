"""
Frontend contract checks — validate HTML structure, JS logic presence,
filter elements, design system compliance, and API contract alignment.
"""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import ENGAGEMENT_TAXONOMY, METRIC_DISPLAY

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "static", "index.html")


def _read_html():
    with open(HTML_PATH, "r") as f:
        return f.read()


def check_html_structure():
    """Verify required page containers and sections exist."""
    html = _read_html()
    required = [
        ("page-overview", "Overview page container"),
        ("page-details", "Details page container"),
        ("exec-summary-wow", "Executive summary WoW"),
        ("exec-summary-yoy", "Executive summary YoY"),
        ("kpi-grid-container", "KPI grid"),
        ("i2a-section", "Insight-to-action section"),
        ("engagement-section", "Engagement section"),
        ("page-funnel-container", "Page funnels"),
    ]
    missing = [(id_, desc) for id_, desc in required if f'id="{id_}"' not in html]
    if missing:
        return {"status": "FAIL", "details": f"Missing elements: {[m[1] for m in missing]}"}
    return {"status": "PASS", "details": f"All {len(required)} required elements present"}


def check_filter_elements():
    """Verify all filter buttons have data-filter and data-value attributes."""
    html = _read_html()
    expected_filters = {
        ("ecu", "all"), ("ecu", "ecu"), ("ecu", "non_ecu"),
        ("hvc", "all"), ("hvc", "hvc"), ("hvc", "non_hvc"),
        ("granularity", "weekly"), ("granularity", "monthly"),
    }
    found = set()
    for match in re.finditer(r'data-filter="(\w+)"\s+data-value="(\w+)"', html):
        found.add((match.group(1), match.group(2)))
    missing = expected_filters - found
    if missing:
        return {"status": "FAIL", "details": f"Missing filter buttons: {missing}"}
    return {"status": "PASS", "details": f"All {len(expected_filters)} filter buttons present"}


def check_api_endpoints_referenced():
    """Verify all 4 API endpoints are called in loadData."""
    html = _read_html()
    endpoints = ["/api/metrics", "/api/adoption", "/api/i2a", "/api/engagement", "/api/executive-summary"]
    missing = [ep for ep in endpoints if ep not in html]
    if missing:
        return {"status": "FAIL", "details": f"Missing API calls: {missing}"}
    return {"status": "PASS", "details": f"All {len(endpoints)} endpoints referenced"}


def check_comparison_logic():
    """Verify dual comparison logic exists (deltaBoth showing WoW + YoY)."""
    html = _read_html()
    checks = {
        "deltaBoth_fn": "function deltaBoth(" in html,
        "_oneDelta_fn": "function _oneDelta(" in html,
        "delta_null_check": "cur == null" in html,
        "pw_prefix": '"pw_"' in html or "pw_" in html,
        "py_prefix": '"py_"' in html or "py_" in html,
        "WoW_label": '"WoW"' in html,
        "YoY_label": '"YoY"' in html,
        "three_series_chart": "Prior Week" in html and "Prior Year" in html,
    }
    failures = [k for k, v in checks.items() if not v]
    if failures:
        return {"status": "FAIL", "details": f"Missing comparison logic: {failures}"}
    return {"status": "PASS", "details": "All comparison logic present"}


def check_wow_monthly_handling():
    """Verify WoW is only shown for weekly granularity (isWeekly check)."""
    html = _read_html()
    if "isWeekly" not in html and "state.granularity" not in html:
        return {"status": "FAIL", "details": "No granularity-aware WoW logic found"}
    return {"status": "PASS", "details": "WoW conditionally shown for weekly granularity"}


def check_fetch_timeout():
    """Verify fetch calls use timeout (Fix 7)."""
    html = _read_html()
    if "AbortController" not in html and "fetchWithTimeout" not in html:
        return {"status": "FAIL", "details": "No fetch timeout protection"}
    return {"status": "PASS", "details": "Fetch timeout via AbortController present"}


def check_freshness_display():
    """Verify data freshness indicator exists (Fix 4)."""
    html = _read_html()
    if "data-freshness" not in html:
        return {"status": "FAIL", "details": "Missing data-freshness element"}
    if "updateFreshness" not in html:
        return {"status": "FAIL", "details": "Missing updateFreshness function"}
    return {"status": "PASS", "details": "Data freshness indicator present"}


def check_error_handling():
    """Verify error handling in loadData."""
    html = _read_html()
    checks = {
        "try_catch": "catch" in html,
        "error_display": "Failed to load data" in html,
        "abort_error": "AbortError" in html,
    }
    failures = [k for k, v in checks.items() if not v]
    if failures:
        return {"status": "WARN", "details": f"Incomplete error handling: {failures}"}
    return {"status": "PASS", "details": "Error handling complete"}


def check_design_system():
    """Verify black/grey design system tokens are used."""
    html = _read_html()
    tokens = {
        "sidebar_black": "#111111",
        "page_bg": "#f5f5f5",
        "accent_dark": "#222222",
        "grey_mid": "#666666",
        "chart_black": "#111111",
        "avenir_font": "Avenir Next",
        "ai_icon": "ai-icon",
    }
    missing = [name for name, val in tokens.items() if val not in html]
    if missing:
        return {"status": "WARN", "details": f"Missing design tokens: {missing}"}
    return {"status": "PASS", "details": f"All {len(tokens)} design tokens present"}


def check_i2a_action_types():
    """Verify all I2A action types are referenced in HTML."""
    html = _read_html()
    types = ["Campaign Created", "Automation Created", "Segment Created", "A/B Test", "Coming Soon"]
    missing = [t for t in types if t not in html]
    if missing:
        return {"status": "FAIL", "details": f"Missing I2A types: {missing}"}
    return {"status": "PASS", "details": "All I2A action types present"}


def run_all():
    return {
        "frontend.html_structure": check_html_structure(),
        "frontend.filter_elements": check_filter_elements(),
        "frontend.api_endpoints": check_api_endpoints_referenced(),
        "frontend.comparison_logic": check_comparison_logic(),
        "frontend.wow_monthly_handling": check_wow_monthly_handling(),
        "frontend.fetch_timeout": check_fetch_timeout(),
        "frontend.freshness_display": check_freshness_display(),
        "frontend.error_handling": check_error_handling(),
        "frontend.design_system": check_design_system(),
        "frontend.i2a_action_types": check_i2a_action_types(),
    }
