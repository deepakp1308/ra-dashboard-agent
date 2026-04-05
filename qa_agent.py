#!/usr/bin/env python3
"""
R&A Executive Dashboard — Companion QA Agent

Validates all aspects of dashboard output quality:
  - Schema: config vs BigQuery metric names
  - API: endpoint response shape, timing, structure
  - Rates: calculation correctness, bounds, formulas
  - Consistency: filter additivity, cross-endpoint alignment
  - Analytics: trend direction, z-scores, funnel, summary constraints
  - Frontend: HTML structure, JS logic, design system compliance

Usage:
  python3 qa_agent.py                    # Offline mode (no server needed)
  python3 qa_agent.py --mode full        # Full mode (server must be running)
  python3 qa_agent.py --mode offline     # Offline only (schema + frontend)
  python3 qa_agent.py --check schema,frontend   # Specific check categories
  python3 qa_agent.py --json             # JSON output
"""

import argparse
import datetime
import json
import sys
import time

from qa_checks import check_schema, check_frontend

# These require a running server
ONLINE_MODULES = {}
try:
    from qa_checks import check_api, check_rates, check_consistency, check_analytics
    ONLINE_MODULES = {
        "api": check_api,
        "rates": check_rates,
        "consistency": check_consistency,
        "analytics": check_analytics,
    }
except ImportError:
    pass  # requests not installed — online checks unavailable

OFFLINE_MODULES = {
    "schema": check_schema,
    "frontend": check_frontend,
}

ALL_MODULES = {**OFFLINE_MODULES, **ONLINE_MODULES}


def run_checks(mode="offline", categories=None):
    """Run QA checks and return structured results."""
    start = time.time()
    results = {}

    if mode == "offline":
        modules = OFFLINE_MODULES
    elif mode == "full":
        modules = ALL_MODULES
    else:
        modules = ALL_MODULES

    if categories:
        modules = {k: v for k, v in modules.items() if k in categories}

    for name, module in modules.items():
        try:
            module_results = module.run_all()
            results.update(module_results)
        except Exception as e:
            results[f"{name}.module_error"] = {
                "status": "FAIL",
                "details": f"Module crashed: {e}",
            }

    duration = time.time() - start

    # Compute summary
    statuses = [r["status"] for r in results.values()]
    passed = statuses.count("PASS")
    warned = statuses.count("WARN")
    failed = statuses.count("FAIL")

    if failed > 0:
        overall = "FAIL"
    elif warned > 0:
        overall = "WARN"
    else:
        overall = "PASS"

    return {
        "run_at": datetime.datetime.utcnow().isoformat() + "Z",
        "mode": mode,
        "duration_seconds": round(duration, 2),
        "overall_status": overall,
        "summary": f"{passed} passed, {warned} warnings, {failed} failures",
        "checks": results,
    }


def print_results(report, as_json=False):
    """Print results in human-readable or JSON format."""
    if as_json:
        print(json.dumps(report, indent=2))
        return

    print()
    print("=" * 72)
    print(f"  R&A Dashboard QA Report — {report['run_at']}")
    print(f"  Mode: {report['mode']}  |  Duration: {report['duration_seconds']}s")
    print("=" * 72)
    print()

    # Status emoji
    icons = {"PASS": " PASS", "WARN": " WARN", "FAIL": " FAIL"}

    for name, check in sorted(report["checks"].items()):
        status = check["status"]
        icon = icons.get(status, "  ???")
        details = check.get("details", "")
        # Color: green for pass, yellow for warn, red for fail
        if status == "PASS":
            color = "\033[92m"
        elif status == "WARN":
            color = "\033[93m"
        else:
            color = "\033[91m"
        reset = "\033[0m"
        print(f"  {color}{icon}{reset}  {name}")
        if details and status != "PASS":
            print(f"         {details}")

    print()
    print("-" * 72)

    overall = report["overall_status"]
    if overall == "PASS":
        color = "\033[92m"
    elif overall == "WARN":
        color = "\033[93m"
    else:
        color = "\033[91m"
    reset = "\033[0m"
    print(f"  {color}Overall: {report['summary']}{reset}")
    print()


def main():
    parser = argparse.ArgumentParser(description="R&A Dashboard QA Agent")
    parser.add_argument(
        "--mode", choices=["offline", "full"], default="offline",
        help="offline: schema+frontend only. full: all checks (server must be running)"
    )
    parser.add_argument(
        "--check", type=str, default=None,
        help="Comma-separated check categories: schema,api,rates,consistency,analytics,frontend"
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output as JSON instead of human-readable"
    )
    args = parser.parse_args()

    categories = args.check.split(",") if args.check else None

    if args.mode == "full" and not ONLINE_MODULES:
        print("ERROR: 'requests' package required for full mode. Install with: pip3 install requests")
        sys.exit(1)

    report = run_checks(mode=args.mode, categories=categories)
    print_results(report, as_json=args.json)

    # Exit code: 0 for pass/warn, 1 for fail
    sys.exit(1 if report["overall_status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
