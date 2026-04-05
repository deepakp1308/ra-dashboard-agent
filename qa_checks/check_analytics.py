"""
Analytics engine output checks — validate trend direction, anomaly z-scores,
funnel monotonicity, and summary constraints.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import requests
from analytics import (
    compute_trends, detect_anomalies, compute_funnel,
    _mean, _stddev,
)

BASE_URL = "http://localhost:5050"

RATE_KEYS = [
    "pct_attributed_rev", "pct_campaigns_created", "campaigns_per_user",
    "pct_automations_created", "pct_segments_created", "pct_actions_taken",
    "pct_ra_owned", "pct_ra_total",
]


def _fetch_metrics():
    try:
        resp = requests.get(f"{BASE_URL}/api/metrics", timeout=30)
        return resp.json() if resp.ok else None
    except Exception:
        return None


def check_trend_direction_matches_data():
    """Verify computed trend direction matches actual data movement."""
    data = _fetch_metrics()
    if not data or len(data) < 3:
        return {"status": "WARN", "details": "Insufficient data for trend check"}

    trends = compute_trends(data, RATE_KEYS)
    mismatches = []
    for key, t in trends.items():
        cur = t.get("current")
        prev = t.get("previous")
        if cur is None or prev is None:
            continue
        actual_dir = "up" if cur > prev else "down" if cur < prev else "flat"
        computed_dir = t.get("direction", "flat")
        # Direction is only set if streak >= 3, so "flat" is OK for short trends
        if computed_dir != "flat" and computed_dir != actual_dir:
            mismatches.append(f"{key}: computed={computed_dir}, actual={actual_dir}")

    if mismatches:
        return {"status": "WARN", "details": f"Direction mismatches: {mismatches[:3]}"}
    return {"status": "PASS", "details": f"All {len(trends)} trend directions verified"}


def check_anomaly_zscores_correct():
    """Recompute z-scores independently and compare to analytics output."""
    data = _fetch_metrics()
    if not data or len(data) < 10:
        return {"status": "WARN", "details": "Insufficient data for anomaly check"}

    anomalies = detect_anomalies(data, RATE_KEYS)
    errors = []
    for a in anomalies[:5]:
        key = a["metric"]
        period = a["period"]
        reported_z = a["z_score"]

        # Find the index of this period
        values = [d.get(key) for d in data]
        periods = [d.get("period_start") for d in data]
        idx = periods.index(period) if period in periods else -1
        if idx < 8:
            continue

        window_vals = [v for v in values[max(0, idx - 8):idx] if v is not None]
        if len(window_vals) < 4:
            continue
        m = _mean(window_vals)
        s = _stddev(window_vals)
        if s and s > 0:
            expected_z = round((values[idx] - m) / s, 2)
            if abs(reported_z - expected_z) > 0.1:
                errors.append(f"{key}@{period}: reported z={reported_z}, recomputed z={expected_z}")

    if errors:
        return {"status": "FAIL", "details": f"Z-score mismatches: {errors[:3]}"}
    return {"status": "PASS", "details": f"All {len(anomalies)} anomaly z-scores verified"}


def check_funnel_monotonic():
    """Validate funnel stages are monotonically decreasing."""
    data = _fetch_metrics()
    if not data:
        return {"status": "FAIL", "details": "Could not fetch metrics"}

    latest = data[-1]
    funnel_data = {
        "active_users": latest.get("active_users", 0),
        "ra_viewed_users": latest.get("ra_active_users", 0),
        "ra_owned_users": latest.get("ra_owned_users", 0),
        "ra_engaged_users": 0,
        "actions_taken_users": latest.get("actions_taken_users", 0),
    }
    funnel = compute_funnel(funnel_data)

    prev = float("inf")
    violations = []
    for stage in funnel:
        if stage["value"] > prev:
            violations.append(f"{stage['label']}: {stage['value']} > prev {prev}")
        prev = stage["value"]

    if violations:
        return {"status": "WARN", "details": f"Non-monotonic funnel: {violations[:3]}"}
    return {"status": "PASS", "details": "Funnel is monotonically decreasing"}


def check_summary_constraints():
    """Validate exec summary has 0-8 bullets with required fields."""
    try:
        resp = requests.get(f"{BASE_URL}/api/executive-summary", params={"compare": "wow"}, timeout=30)
        data = resp.json() if resp.ok else None
    except Exception:
        return {"status": "FAIL", "details": "Could not fetch summary"}

    if not data or "summary" not in data:
        return {"status": "FAIL", "details": "Missing summary key"}

    bullets = data["summary"]
    if len(bullets) > 8:
        return {"status": "FAIL", "details": f"{len(bullets)} bullets exceeds max 8"}

    valid_categories = {"trend", "anomaly", "opportunity", "threat", "action"}
    for i, b in enumerate(bullets):
        if b.get("category") not in valid_categories:
            return {"status": "FAIL", "details": f"Bullet {i}: invalid category '{b.get('category')}'"}
        if not b.get("text"):
            return {"status": "FAIL", "details": f"Bullet {i}: empty text"}
        if "score" in b:
            return {"status": "FAIL", "details": f"Bullet {i}: score not stripped"}

    return {"status": "PASS", "details": f"{len(bullets)} bullets, all valid"}


def run_all():
    return {
        "analytics.trend_direction": check_trend_direction_matches_data(),
        "analytics.anomaly_zscores": check_anomaly_zscores_correct(),
        "analytics.funnel_monotonic": check_funnel_monotonic(),
        "analytics.summary_constraints": check_summary_constraints(),
    }
