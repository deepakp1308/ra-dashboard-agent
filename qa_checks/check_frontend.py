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
        ("exec-summary-container", "Executive summary"),
        ("kpi-grid-container", "KPI grid"),
        ("i2a-section", "Insight-to-action section"),
        ("engagement-section", "Engagement section"),
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
        ("compare", "none"), ("compare", "wow"), ("compare", "yoy"),
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
    endpoints = ["/api/metrics", "/api/adoption", "/api/engagement", "/api/executive-summary"]
    missing = [ep for ep in endpoints if ep not in html]
    if missing:
        return {"status": "FAIL", "details": f"Missing API calls: {missing}"}
    return {"status": "PASS", "details": f"All {len(endpoints)} endpoints referenced"}


def check_comparison_logic():
    """Verify comparison mode logic exists (getComparePrefix, delta with mode)."""
    html = _read_html()
    checks = {
        "getComparePrefix": "function getComparePrefix()" in html,
        "getCompareLabel": "function getCompareLabel()" in html,
        "delta_null_check": "cur == null" in html,
        "compare_none_check": 'state.compare === "none"' in html,
        "pw_prefix": '"pw_"' in html,
        "py_prefix": '"py_"' in html,
        "WoW_label": '"WoW"' in html,
        "YoY_label": '"YoY"' in html,
    }
    failures = [k for k, v in checks.items() if not v]
    if failures:
        return {"status": "FAIL", "details": f"Missing comparison logic: {failures}"}
    return {"status": "PASS", "details": "All comparison logic present"}


def check_wow_monthly_disable():
    """Verify WoW is disabled for monthly granularity (Fix 2)."""
    html = _read_html()
    if "updateWoWAvailability" not in html:
        return {"status": "FAIL", "details": "Missing updateWoWAvailability function"}
    if "monthly" not in html or "disabled" not in html:
        return {"status": "WARN", "details": "WoW monthly disable logic may be incomplete"}
    return {"status": "PASS", "details": "WoW monthly disable logic present"}


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
    """Verify Intuit/QBO FY27 design system tokens are used."""
    html = _read_html()
    tokens = {
        "sidebar_navy": "#162251",
        "page_bg": "#f0f4f8",
        "accent_blue": "#0070d2",
        "ai_teal": "#00b9a9",
        "chart_navy": "#1e3a6e",
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
        "frontend.wow_monthly_disable": check_wow_monthly_disable(),
        "frontend.fetch_timeout": check_fetch_timeout(),
        "frontend.freshness_display": check_freshness_display(),
        "frontend.error_handling": check_error_handling(),
        "frontend.design_system": check_design_system(),
        "frontend.i2a_action_types": check_i2a_action_types(),
    }
