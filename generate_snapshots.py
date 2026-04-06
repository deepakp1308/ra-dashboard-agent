#!/usr/bin/env python3
"""
Generate snapshot matrix for static GitHub Pages deployment.
Pre-computes data for key filter combos so filters work without a live server.
"""

import json
import requests
import sys
import time

BASE = "http://localhost:5050"
ENDPOINTS = [
    ("metrics", "/api/metrics"),
    ("adoption", "/api/adoption"),
    ("i2a", "/api/i2a"),
    ("engagement", "/api/engagement"),
    ("summary_wow", "/api/executive-summary", {"compare": "wow"}),
    ("summary_yoy", "/api/executive-summary", {"compare": "yoy"}),
    ("page_funnel", "/api/page-funnel"),
]

COMBOS = [
    {"ecu": ecu, "hvc": hvc, "granularity": gran}
    for ecu in ["all", "ecu", "non_ecu"]
    for hvc in ["all", "hvc", "non_hvc"]
    for gran in ["monthly", "weekly"]
]


def make_key(combo):
    return f"{combo['ecu']}|{combo['hvc']}|{combo['granularity']}"


def fetch_combo(combo):
    params = {**combo, "tenure": "all", "date_start": "2024-01-01"}
    data = {}
    for entry in ENDPOINTS:
        name = entry[0]
        path = entry[1]
        extra = entry[2] if len(entry) > 2 else {}
        url = f"{BASE}{path}"
        p = {**params, **extra}
        try:
            r = requests.get(url, params=p, timeout=180)
            d = r.json()
            data[name] = d.get("data", d) if isinstance(d, dict) and "data" in d else d
        except Exception as e:
            print(f"  ERROR {name}: {e}")
            data[name] = []
    return data


def main():
    print("Generating snapshot matrix...")
    print(f"Server: {BASE}")
    print(f"Combos: {len(COMBOS)}")
    print()

    # Check server is running
    try:
        r = requests.get(f"{BASE}/", timeout=5)
        assert r.ok
    except Exception:
        print("ERROR: Flask server not running at", BASE)
        sys.exit(1)

    matrix = {}
    for i, combo in enumerate(COMBOS):
        key = make_key(combo)
        print(f"[{i+1}/{len(COMBOS)}] {key}...", flush=True)
        start = time.time()
        matrix[key] = fetch_combo(combo)
        dur = time.time() - start
        print(f"  Done in {dur:.1f}s")

    out = "static/snapshot_matrix.json"
    with open(out, "w") as f:
        json.dump(matrix, f)

    size_kb = len(json.dumps(matrix)) // 1024
    print(f"\nSaved {out}: {size_kb}KB ({len(matrix)} combos)")


if __name__ == "__main__":
    main()
