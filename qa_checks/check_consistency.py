"""
Cross-endpoint consistency checks — validate filter additivity,
active_users alignment, and comparison mode behavior.
"""

import requests

BASE_URL = "http://localhost:5050"


def _fetch(endpoint, params=None):
    try:
        resp = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=30)
        if resp.ok:
            data = resp.json()
            return data.get("data", data) if isinstance(data, dict) else data
        return None
    except Exception:
        return None


def check_ecu_additivity():
    """ECU + Non-ECU active_users should ≈ All active_users (within 5%)."""
    all_data = _fetch("/api/metrics", {"ecu": "all"})
    ecu_data = _fetch("/api/metrics", {"ecu": "ecu"})
    nonecu_data = _fetch("/api/metrics", {"ecu": "non_ecu"})
    if not all_data or not ecu_data or not nonecu_data:
        return {"status": "FAIL", "details": "Could not fetch all 3 slices"}

    # Compare latest period
    all_au = all_data[-1].get("active_users", 0) if all_data else 0
    ecu_au = ecu_data[-1].get("active_users", 0) if ecu_data else 0
    nonecu_au = nonecu_data[-1].get("active_users", 0) if nonecu_data else 0
    combined = ecu_au + nonecu_au

    if all_au == 0:
        return {"status": "WARN", "details": "All active_users = 0"}

    diff_pct = abs(combined - all_au) / all_au * 100
    if diff_pct > 5:
        return {"status": "WARN", "details": f"ECU({ecu_au})+NonECU({nonecu_au})={combined} vs All={all_au} ({diff_pct:.1f}% diff)"}
    return {"status": "PASS", "details": f"ECU+NonECU={combined}, All={all_au} ({diff_pct:.1f}% diff)"}


def check_hvc_additivity():
    """HVC + Non-HVC should ≈ All."""
    all_data = _fetch("/api/metrics", {"hvc": "all"})
    hvc_data = _fetch("/api/metrics", {"hvc": "hvc"})
    nonhvc_data = _fetch("/api/metrics", {"hvc": "non_hvc"})
    if not all_data or not hvc_data or not nonhvc_data:
        return {"status": "FAIL", "details": "Could not fetch all 3 slices"}

    all_au = all_data[-1].get("active_users", 0) if all_data else 0
    hvc_au = hvc_data[-1].get("active_users", 0) if hvc_data else 0
    nonhvc_au = nonhvc_data[-1].get("active_users", 0) if nonhvc_data else 0
    combined = hvc_au + nonhvc_au

    if all_au == 0:
        return {"status": "WARN", "details": "All active_users = 0"}

    diff_pct = abs(combined - all_au) / all_au * 100
    if diff_pct > 5:
        return {"status": "WARN", "details": f"HVC({hvc_au})+NonHVC({nonhvc_au})={combined} vs All={all_au} ({diff_pct:.1f}% diff)"}
    return {"status": "PASS", "details": f"HVC+NonHVC={combined}, All={all_au} ({diff_pct:.1f}% diff)"}


def check_exec_summary_both_modes():
    """Executive summary should work for both WoW and YoY."""
    for mode in ["wow", "yoy"]:
        data = _fetch("/api/executive-summary", {"compare": mode})
        if data is None:
            return {"status": "FAIL", "details": f"Failed for compare={mode}"}
        if not isinstance(data, dict) or "summary" not in data:
            return {"status": "FAIL", "details": f"Bad shape for compare={mode}"}
        if data.get("comparison_mode") != mode:
            return {"status": "FAIL", "details": f"Mode mismatch: expected {mode}, got {data.get('comparison_mode')}"}
    return {"status": "PASS", "details": "Both WoW and YoY modes work"}


def check_monthly_fewer_periods():
    """Monthly granularity should have fewer periods than weekly."""
    weekly = _fetch("/api/metrics", {"granularity": "weekly"})
    monthly = _fetch("/api/metrics", {"granularity": "monthly"})
    if not weekly or not monthly:
        return {"status": "FAIL", "details": "Could not fetch both granularities"}
    if len(monthly) >= len(weekly):
        return {"status": "WARN", "details": f"Monthly({len(monthly)}) >= Weekly({len(weekly)})"}
    return {"status": "PASS", "details": f"Weekly={len(weekly)} periods, Monthly={len(monthly)} periods"}


def run_all():
    return {
        "consistency.ecu_additivity": check_ecu_additivity(),
        "consistency.hvc_additivity": check_hvc_additivity(),
        "consistency.exec_summary_modes": check_exec_summary_both_modes(),
        "consistency.monthly_fewer_periods": check_monthly_fewer_periods(),
    }
