"""
API response checks — validate endpoint response shape, column presence,
data types, sorting, and response time.
"""

import time
import requests


BASE_URL = "http://localhost:5050"
TIMEOUT = 30

REQUIRED_METRICS_COLS = [
    "period_start", "active_users", "campaign_users", "total_campaigns",
    "attributed_rev", "total_rev", "ra_active_users",
    "automation_users", "total_automations", "segment_users", "total_segments",
    "actions_taken_users", "total_actions",
    "pct_attributed_rev", "pct_campaigns_created", "campaigns_per_user",
    "pct_automations_created", "pct_segments_created", "pct_actions_taken",
    "pct_ra_owned", "pct_ra_total",
    "py_pct_attributed_rev", "py_pct_campaigns_created",
]

REQUIRED_ADOPTION_COLS = ["period_start", "active_users", "pct_ra_owned", "pct_ra_total"]


def _fetch(endpoint, params=None):
    """Fetch an endpoint and return (data, elapsed_seconds, error)."""
    url = f"{BASE_URL}{endpoint}"
    start = time.time()
    try:
        resp = requests.get(url, params=params, timeout=TIMEOUT)
        elapsed = time.time() - start
        if resp.status_code != 200:
            return None, elapsed, f"HTTP {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        return data, elapsed, None
    except requests.exceptions.ConnectionError:
        return None, 0, "Connection refused — is server running on port 5050?"
    except Exception as e:
        return None, time.time() - start, str(e)


def check_metrics_response_shape():
    """Validate /api/metrics returns expected columns."""
    data, elapsed, err = _fetch("/api/metrics")
    if err:
        return {"status": "FAIL", "details": err}
    if not isinstance(data, list) or len(data) == 0:
        return {"status": "WARN", "details": "Empty response"}
    row = data[0]
    missing = [c for c in REQUIRED_METRICS_COLS if c not in row]
    if missing:
        return {"status": "FAIL", "details": f"Missing columns: {missing}"}
    return {"status": "PASS", "details": f"{len(row)} columns, {len(data)} rows, {elapsed:.1f}s"}


def check_metrics_wow_columns():
    """Validate pw_ (WoW) columns present for weekly granularity."""
    data, _, err = _fetch("/api/metrics", {"granularity": "weekly"})
    if err or not data:
        return {"status": "FAIL", "details": err or "No data"}
    row = data[0]
    pw_cols = [c for c in row.keys() if c.startswith("pw_")]
    if len(pw_cols) == 0:
        return {"status": "FAIL", "details": "No pw_ columns in weekly response"}
    return {"status": "PASS", "details": f"{len(pw_cols)} pw_ columns present"}


def check_metrics_monthly_no_wow():
    """Validate pw_ columns absent for monthly granularity (Fix 2)."""
    data, _, err = _fetch("/api/metrics", {"granularity": "monthly"})
    if err or not data:
        return {"status": "FAIL", "details": err or "No data"}
    row = data[0]
    pw_cols = [c for c in row.keys() if c.startswith("pw_")]
    if pw_cols:
        return {"status": "WARN", "details": f"Monthly still has pw_ columns: {pw_cols[:3]}"}
    return {"status": "PASS", "details": "No pw_ columns in monthly (correct)"}


def check_metrics_sorted():
    """Validate periods sorted ascending."""
    data, _, err = _fetch("/api/metrics")
    if err or not data:
        return {"status": "FAIL", "details": err or "No data"}
    dates = [r["period_start"] for r in data]
    if dates != sorted(dates):
        return {"status": "FAIL", "details": "Periods not sorted ascending"}
    if len(dates) != len(set(dates)):
        return {"status": "FAIL", "details": "Duplicate period_start values"}
    return {"status": "PASS", "details": f"{len(dates)} periods, sorted, no duplicates"}


def check_metrics_response_time():
    """Validate response time < 5s."""
    _, elapsed, err = _fetch("/api/metrics")
    if err:
        return {"status": "FAIL", "details": err}
    if elapsed > 5:
        return {"status": "WARN", "details": f"Slow: {elapsed:.1f}s (>5s threshold)"}
    return {"status": "PASS", "details": f"{elapsed:.1f}s"}


def check_adoption_response_shape():
    """Validate /api/adoption response shape."""
    data, _, err = _fetch("/api/adoption")
    if err:
        return {"status": "FAIL", "details": err}
    # May be wrapped in {data, meta} or flat list
    rows = data.get("data", data) if isinstance(data, dict) else data
    if not isinstance(rows, list) or len(rows) == 0:
        return {"status": "WARN", "details": "Empty response"}
    row = rows[0]
    missing = [c for c in REQUIRED_ADOPTION_COLS if c not in row]
    if missing:
        return {"status": "FAIL", "details": f"Missing columns: {missing}"}
    return {"status": "PASS", "details": f"{len(rows)} rows"}


def check_adoption_dedup():
    """Validate pct_ra_total >= pct_ra_owned (de-duplication)."""
    data, _, err = _fetch("/api/adoption")
    if err:
        return {"status": "FAIL", "details": err}
    rows = data.get("data", data) if isinstance(data, dict) else data
    if not rows:
        return {"status": "WARN", "details": "No data"}
    violations = []
    for r in rows:
        owned = r.get("pct_ra_owned", 0)
        total = r.get("pct_ra_total", 0)
        if total < owned - 0.1:
            violations.append(f"{r['period_start']}: total={total} < owned={owned}")
    if violations:
        return {"status": "FAIL", "details": f"De-dup violations: {violations[:3]}"}
    return {"status": "PASS", "details": f"{len(rows)} periods, all total >= owned"}


def check_engagement_response_shape():
    """Validate /api/engagement response has taxonomy groups."""
    data, _, err = _fetch("/api/engagement")
    if err:
        return {"status": "FAIL", "details": err}
    rows = data.get("data", data) if isinstance(data, dict) else data
    if not rows:
        return {"status": "WARN", "details": "No data"}
    period = rows[0]
    expected = {"owned", "supported", "owned_engaged", "supported_engaged", "combined"}
    missing = expected - set(period.keys())
    if missing:
        return {"status": "FAIL", "details": f"Missing groups: {missing}"}
    return {"status": "PASS", "details": f"{len(rows)} periods, all groups present"}


def check_exec_summary_shape():
    """Validate /api/executive-summary response structure."""
    data, _, err = _fetch("/api/executive-summary", {"compare": "wow"})
    if err:
        return {"status": "FAIL", "details": err}
    for key in ["period", "comparison_mode", "summary", "analytics"]:
        if key not in data:
            return {"status": "FAIL", "details": f"Missing key: {key}"}
    bullets = data["summary"]
    if len(bullets) > 8:
        return {"status": "WARN", "details": f"{len(bullets)} bullets (max 8)"}
    for b in bullets:
        if "score" in b:
            return {"status": "FAIL", "details": "Score not stripped from output"}
        for field in ["category", "severity", "text", "metric"]:
            if field not in b:
                return {"status": "FAIL", "details": f"Bullet missing '{field}'"}
    return {"status": "PASS", "details": f"{len(bullets)} bullets, structure valid"}


def check_invalid_filter_rejected():
    """Validate server rejects invalid filter values (Fix 1)."""
    data, _, err = _fetch("/api/metrics", {"ecu": "'; DROP TABLE x; --"})
    if err and "400" not in err:
        return {"status": "WARN", "details": f"Unexpected error: {err}"}
    if isinstance(data, dict) and "error" in data:
        return {"status": "PASS", "details": "Invalid filter rejected with error"}
    return {"status": "FAIL", "details": "Invalid filter was accepted — SQL injection risk"}


def run_all():
    return {
        "api.metrics_response_shape": check_metrics_response_shape(),
        "api.metrics_wow_columns": check_metrics_wow_columns(),
        "api.metrics_monthly_no_wow": check_metrics_monthly_no_wow(),
        "api.metrics_sorted": check_metrics_sorted(),
        "api.metrics_response_time": check_metrics_response_time(),
        "api.adoption_response_shape": check_adoption_response_shape(),
        "api.adoption_dedup": check_adoption_dedup(),
        "api.engagement_response_shape": check_engagement_response_shape(),
        "api.exec_summary_shape": check_exec_summary_shape(),
        "api.invalid_filter_rejected": check_invalid_filter_rejected(),
    }
