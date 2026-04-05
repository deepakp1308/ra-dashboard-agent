#!/usr/bin/env python3
"""
I2A Attribution Window Analysis
Compares Insight-to-Action rates across 1, 7, 14, and 30 day windows.

Question: How do I2A metrics change when we expand the attribution window
from 1 day to longer periods? Which window shows the most positive outlook?

The current dashboard uses a 1-day window (from RA_L1L3_Insight_to_action.sqlx).
This script tests whether expanding that window captures more meaningful actions.
"""

import datetime
import json
from google.cloud import bigquery

client = bigquery.Client(project="mc-analytics-devel")

WINDOWS = [1, 7, 14, 30]

# We look at the most recent 12 weeks for trend analysis
QUERY_TEMPLATE = """
WITH base_user AS (
    SELECT w.user_id, w.week,
           ecomm_level_end,
           CAST(is_high_value AS STRING) AS is_high_value
    FROM `mc-analytics-devel.bi_product.user_dimensions_weekly_rollup` w
    WHERE w.week >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, WEEK), INTERVAL 14 WEEK)
      AND w.week < DATE_TRUNC(CURRENT_DATE, WEEK)
      AND w.wau
      AND w.is_high_value IS NOT NULL
      AND ecomm_level_end IN ('ecu', 'non', 'ecomm')
    GROUP BY ALL
),

-- R&A page views (owned + supported union, same as dashboard)
ra_views AS (
    SELECT user_id,
           DATE_TRUNC(DATE(timestamp), WEEK) AS week,
           MIN(DATE(timestamp)) AS first_viewed_date
    FROM `mc-business-intelligence.bi_activities.ecs_users_activities`
    WHERE DATE(timestamp) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, WEEK), INTERVAL 14 WEEK)
      AND (
        -- Owned pages (reporting:viewed)
        (event = 'reporting:viewed' AND screen NOT IN ('/') AND (
            (scope_area = 'analytics' AND object_detail IN ('marketing_dashboard','email_dashboard','audience_analytics','custom_reports','sms_report'))
            OR (scope_area IN ('business_analytics','subscription_management') AND object_detail IN ('conversion_insights','revenue_plans'))
            OR (initiative_name = 'marketing_crm_analytics' AND scope_area = 'campaign_analytics' AND object_detail LIKE 'email_%')
            OR (initiative_name = 'core_offerings' AND object_detail IN ('cjb_original_report','cjb_overview_report','customer_journey_builder_report'))
            OR screen IN ('/reports/','/reports','reports','/analytics/marketing-dashboard','/analytics/audience-analytics','/analytics/sms')
            OR screen LIKE '%/reports/#f_list%'
            OR screen LIKE '%analytics/conversion-insights%'
            OR screen LIKE '%analytics/custom-reports%'
        ))
        -- Supported page views
        OR screen IN ('/','/campaigns/','/campaigns','/customer-journey/','/audience/','/audience/contacts/','/audience/segments/','/sms/')
      )
    GROUP BY user_id, week
),

-- Campaigns created (with variable window)
campaigns AS (
    SELECT a.user_id,
           DATE_TRUNC(DATE(a.created_at), WEEK) AS week,
           COUNT(DISTINCT CONCAT(a.user_id, a.campaign_id)) AS campaigns_created
    FROM `mc-business-intelligence.bi_reporting.emails_bulk` a
    INNER JOIN ra_views v
        ON a.user_id = v.user_id
        AND DATE_TRUNC(DATE(a.created_at), WEEK) = v.week
        AND DATE(a.created_at) >= v.first_viewed_date
        AND DATE_DIFF(DATE(a.created_at), v.first_viewed_date, DAY) <= {window}
    WHERE DATE(a.created_at) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, WEEK), INTERVAL 14 WEEK)
    GROUP BY ALL
),

-- Automations created (with variable window)
automations AS (
    SELECT a.user_id,
           DATE_TRUNC(DATE(a.created_at), WEEK) AS week,
           COUNT(DISTINCT CONCAT(a.user_id, a.workflow_id)) AS automations_created
    FROM `mc-business-intelligence.bi_reporting.customer_journey_workflows` a
    INNER JOIN ra_views v
        ON a.user_id = v.user_id
        AND DATE_TRUNC(DATE(a.created_at), WEEK) = v.week
        AND DATE(a.created_at) >= v.first_viewed_date
        AND DATE_DIFF(DATE(a.created_at), v.first_viewed_date, DAY) <= {window}
    WHERE DATE(a.created_at) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, WEEK), INTERVAL 14 WEEK)
    GROUP BY ALL
),

-- Segments created (with variable window)
segments AS (
    SELECT a.user_id,
           DATE_TRUNC(a.action_date, WEEK) AS week,
           SUM(CASE WHEN action_type LIKE '%segment created%' AND finished_action_count > 0
                    THEN finished_action_count END) AS segments_created
    FROM `mc-business-intelligence.bi_reporting.tags_segments_daily_rollup` a
    INNER JOIN ra_views v
        ON a.user_id = v.user_id
        AND DATE_TRUNC(a.action_date, WEEK) = v.week
        AND a.action_date >= v.first_viewed_date
        AND DATE_DIFF(a.action_date, v.first_viewed_date, DAY) <= {window}
    WHERE a.action_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, WEEK), INTERVAL 14 WEEK)
    GROUP BY ALL
),

-- Join everything
weekly AS (
    SELECT
        b.week,
        COUNT(DISTINCT b.user_id) AS active_users,
        COUNT(DISTINCT CASE WHEN c.campaigns_created > 0 THEN b.user_id END) AS campaign_users,
        COUNT(DISTINCT CASE WHEN a.automations_created > 0 THEN b.user_id END) AS automation_users,
        COUNT(DISTINCT CASE WHEN s.segments_created > 0 THEN b.user_id END) AS segment_users,
        COUNT(DISTINCT CASE WHEN c.campaigns_created > 0
                              OR a.automations_created > 0
                              OR s.segments_created > 0
                           THEN b.user_id END) AS any_action_users,
        SUM(COALESCE(c.campaigns_created, 0)) AS total_campaigns,
        SUM(COALESCE(a.automations_created, 0)) AS total_automations,
        SUM(COALESCE(s.segments_created, 0)) AS total_segments
    FROM base_user b
    LEFT JOIN campaigns c ON b.user_id = c.user_id AND b.week = c.week
    LEFT JOIN automations a ON b.user_id = a.user_id AND b.week = a.week
    LEFT JOIN segments s ON b.user_id = s.user_id AND b.week = s.week
    GROUP BY 1
)

SELECT
    week,
    active_users,
    campaign_users,
    automation_users,
    segment_users,
    any_action_users,
    total_campaigns,
    total_automations,
    total_segments,
    ROUND(SAFE_DIVIDE(campaign_users, active_users) * 100, 2) AS pct_campaign,
    ROUND(SAFE_DIVIDE(automation_users, active_users) * 100, 2) AS pct_automation,
    ROUND(SAFE_DIVIDE(segment_users, active_users) * 100, 2) AS pct_segment,
    ROUND(SAFE_DIVIDE(any_action_users, active_users) * 100, 2) AS pct_any_action,
    ROUND(SAFE_DIVIDE(total_campaigns, active_users), 3) AS campaigns_per_user,
    ROUND(SAFE_DIVIDE(total_automations, active_users), 4) AS automations_per_user,
    ROUND(SAFE_DIVIDE(total_segments, active_users), 4) AS segments_per_user
FROM weekly
WHERE week IS NOT NULL
ORDER BY week ASC
"""


def run_analysis():
    print("=" * 80)
    print("  I2A Attribution Window Analysis")
    print(f"  Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)
    print()

    all_results = {}

    for window in WINDOWS:
        print(f"  Querying {window}-day window...", end=" ", flush=True)
        query = QUERY_TEMPLATE.format(window=window)
        rows = []
        try:
            for row in client.query(query).result():
                r = dict(row)
                if hasattr(r.get("week"), "isoformat"):
                    r["week"] = r["week"].isoformat()
                rows.append(r)
            print(f"{len(rows)} weeks returned")
        except Exception as e:
            print(f"FAILED: {e}")
            continue
        all_results[window] = rows

    if not all_results:
        print("\n  ERROR: No data returned from any query.")
        return

    # ── Latest Week Comparison ────────────────────────────────────────────
    print()
    print("-" * 80)
    print("  LATEST WEEK COMPARISON")
    print("-" * 80)
    print()
    print(f"  {'Metric':<35} {'1-day':>10} {'7-day':>10} {'14-day':>10} {'30-day':>10}")
    print(f"  {'─' * 35} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")

    metrics = [
        ("% Campaign Created", "pct_campaign"),
        ("% Automation Created", "pct_automation"),
        ("% Segment Created", "pct_segment"),
        ("% Any Action (composite)", "pct_any_action"),
        ("Campaigns per User", "campaigns_per_user"),
        ("Automations per User", "automations_per_user"),
        ("Segments per User", "segments_per_user"),
        ("Campaign Users", "campaign_users"),
        ("Automation Users", "automation_users"),
        ("Segment Users", "segment_users"),
        ("Any Action Users", "any_action_users"),
        ("Active Users (WAU)", "active_users"),
    ]

    latest = {}
    for w in WINDOWS:
        if w in all_results and all_results[w]:
            latest[w] = all_results[w][-1]

    for label, key in metrics:
        vals = []
        for w in WINDOWS:
            v = latest.get(w, {}).get(key)
            if v is None:
                vals.append("N/A")
            elif isinstance(v, float):
                vals.append(f"{v:.2f}" if v < 100 else f"{v:,.0f}")
            else:
                vals.append(f"{v:,}")
        print(f"  {label:<35} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10} {vals[3]:>10}")

    # ── Lift from expanding window ────────────────────────────────────────
    print()
    print("-" * 80)
    print("  LIFT FROM EXPANDING WINDOW (vs 1-day baseline)")
    print("-" * 80)
    print()

    rate_metrics = [
        ("% Campaign Created", "pct_campaign"),
        ("% Automation Created", "pct_automation"),
        ("% Segment Created", "pct_segment"),
        ("% Any Action", "pct_any_action"),
    ]

    print(f"  {'Metric':<35} {'1→7 day':>10} {'1→14 day':>10} {'1→30 day':>10}")
    print(f"  {'─' * 35} {'─' * 10} {'─' * 10} {'─' * 10}")

    for label, key in rate_metrics:
        base = latest.get(1, {}).get(key, 0) or 0
        lifts = []
        for w in [7, 14, 30]:
            v = latest.get(w, {}).get(key, 0) or 0
            if base > 0:
                lift = v - base
                lifts.append(f"+{lift:.1f}pp" if lift >= 0 else f"{lift:.1f}pp")
            else:
                lifts.append("N/A")
        print(f"  {label:<35} {lifts[0]:>10} {lifts[1]:>10} {lifts[2]:>10}")

    # ── Trend Analysis (last 12 weeks average) ────────────────────────────
    print()
    print("-" * 80)
    print("  12-WEEK AVERAGE RATES BY WINDOW")
    print("-" * 80)
    print()

    print(f"  {'Metric':<35} {'1-day':>10} {'7-day':>10} {'14-day':>10} {'30-day':>10}")
    print(f"  {'─' * 35} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")

    for label, key in rate_metrics:
        avgs = []
        for w in WINDOWS:
            rows = all_results.get(w, [])
            vals = [r.get(key) for r in rows if r.get(key) is not None]
            avg = sum(vals) / len(vals) if vals else 0
            avgs.append(f"{avg:.2f}%")
        print(f"  {label:<35} {avgs[0]:>10} {avgs[1]:>10} {avgs[2]:>10} {avgs[3]:>10}")

    # ── Week-over-Week Trend Direction ────────────────────────────────────
    print()
    print("-" * 80)
    print("  RECENT TREND DIRECTION (last 4 weeks)")
    print("-" * 80)
    print()

    for label, key in rate_metrics:
        print(f"  {label}:")
        for w in WINDOWS:
            rows = all_results.get(w, [])
            if len(rows) < 4:
                print(f"    {w:>2}-day: insufficient data")
                continue
            recent = [r.get(key, 0) for r in rows[-4:]]
            deltas = [recent[i] - recent[i-1] for i in range(1, len(recent)) if recent[i] is not None and recent[i-1] is not None]
            up = sum(1 for d in deltas if d > 0)
            down = sum(1 for d in deltas if d < 0)
            direction = "↑ improving" if up > down else "↓ declining" if down > up else "→ flat"
            avg_delta = sum(deltas) / len(deltas) if deltas else 0
            print(f"    {w:>2}-day: {direction}  (avg WoW Δ: {avg_delta:+.2f}pp)")
        print()

    # ── Recommendation ────────────────────────────────────────────────────
    print("-" * 80)
    print("  RECOMMENDATION")
    print("-" * 80)
    print()

    # Find which window shows best improvement in "any action" rate
    base_any = latest.get(1, {}).get("pct_any_action", 0) or 0
    best_window = 1
    best_lift = 0
    for w in [7, 14, 30]:
        v = latest.get(w, {}).get("pct_any_action", 0) or 0
        lift = v - base_any
        if lift > best_lift:
            best_lift = lift
            best_window = w

    # Check diminishing returns
    lifts = {}
    for w in [7, 14, 30]:
        v = latest.get(w, {}).get("pct_any_action", 0) or 0
        lifts[w] = v - base_any

    marginal_7_to_14 = lifts.get(14, 0) - lifts.get(7, 0)
    marginal_14_to_30 = lifts.get(30, 0) - lifts.get(14, 0)

    print(f"  Baseline (1-day window): {base_any:.1f}% of WAU took any action")
    print()
    for w in [7, 14, 30]:
        v = latest.get(w, {}).get("pct_any_action", 0) or 0
        lift = lifts.get(w, 0)
        print(f"  {w:>2}-day window: {v:.1f}% (+{lift:.1f}pp lift from 1-day)")
    print()
    print(f"  Marginal gain 7→14 day: +{marginal_7_to_14:.1f}pp")
    print(f"  Marginal gain 14→30 day: +{marginal_14_to_30:.1f}pp")
    print()

    if best_lift > 0:
        print(f"  >>> Best window: {best_window}-day (+{best_lift:.1f}pp over 1-day)")
        if marginal_14_to_30 < 0.5 and lifts.get(14, 0) > 0:
            print(f"  >>> Diminishing returns after 14 days — consider 7 or 14 day window")
        elif marginal_7_to_14 < 0.5 and lifts.get(7, 0) > 0:
            print(f"  >>> Diminishing returns after 7 days — 7-day window is the sweet spot")
    else:
        print(f"  >>> 1-day window already captures most actions — longer windows add minimal lift")

    print()
    print("=" * 80)


if __name__ == "__main__":
    run_analysis()
