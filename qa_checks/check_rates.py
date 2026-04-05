"""
Rate calculation checks — verify formulas, bounds, and numerator/denominator relationships.
"""

import requests

BASE_URL = "http://localhost:5050"


def _fetch_metrics():
    try:
        resp = requests.get(f"{BASE_URL}/api/metrics", timeout=30)
        return resp.json() if resp.ok else None
    except Exception:
        return None


def check_rate_bounds():
    """All pct_ fields should be 0-100 (flag > 100 as data quality issue)."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}
    violations = []
    for row in data:
        for key, val in row.items():
            if key.startswith("pct_") and val is not None:
                if val < 0 or val > 100.5:
                    violations.append(f"{row['period_start']}: {key}={val}")
    if violations:
        return {"status": "WARN", "details": f"{len(violations)} out-of-bounds rates: {violations[:3]}"}
    return {"status": "PASS", "details": "All rates within 0-100 bounds"}


def check_numerator_lte_denominator():
    """Campaign/automation/segment users should not exceed active_users."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}
    violations = []
    for row in data:
        au = row.get("active_users", 0)
        if au <= 0:
            continue
        for col in ["campaign_users", "automation_users", "segment_users", "actions_taken_users"]:
            val = row.get(col, 0)
            if val is not None and val > au * 1.01:
                violations.append(f"{row['period_start']}: {col}={val} > active_users={au}")
    if violations:
        return {"status": "WARN", "details": f"{len(violations)} numerator > denominator: {violations[:3]}"}
    return {"status": "PASS", "details": "All numerators <= denominators"}


def check_active_users_positive():
    """active_users should never be 0 (breaks all rate calculations)."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}
    zero_periods = [r["period_start"] for r in data if r.get("active_users", 0) == 0]
    if zero_periods:
        return {"status": "FAIL", "details": f"active_users=0 in: {zero_periods[:3]}"}
    return {"status": "PASS", "details": "All periods have positive active_users"}


def check_campaign_rate_formula():
    """Verify pct_campaigns_created ≈ campaign_users / active_users * 100."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}
    errors = []
    for row in data:
        cu = row.get("campaign_users", 0) or 0
        au = row.get("active_users", 0) or 0
        pct = row.get("pct_campaigns_created")
        if au > 0 and pct is not None:
            expected = round(cu / au * 100, 2)
            if abs(pct - expected) > 0.1:
                errors.append(f"{row['period_start']}: got {pct}, expected {expected}")
    if errors:
        return {"status": "FAIL", "details": f"Rate formula mismatch: {errors[:3]}"}
    return {"status": "PASS", "details": "Campaign rate formula verified"}


def check_no_negative_counts():
    """No user count or revenue field should be negative."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}
    count_cols = [
        "active_users", "campaign_users", "total_campaigns", "automation_users",
        "total_automations", "segment_users", "total_segments",
        "actions_taken_users", "total_actions", "ra_active_users", "ra_owned_users",
    ]
    violations = []
    for row in data:
        for col in count_cols:
            val = row.get(col)
            if val is not None and val < 0:
                violations.append(f"{row['period_start']}: {col}={val}")
    if violations:
        return {"status": "FAIL", "details": f"Negative counts: {violations[:3]}"}
    return {"status": "PASS", "details": "No negative counts"}


def run_all():
    return {
        "rates.bounds": check_rate_bounds(),
        "rates.numerator_lte_denominator": check_numerator_lte_denominator(),
        "rates.active_users_positive": check_active_users_positive(),
        "rates.campaign_formula": check_campaign_rate_formula(),
        "rates.no_negative_counts": check_no_negative_counts(),
    }
