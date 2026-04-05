"""
R&A Executive Dashboard — Flask Backend.
Serves metrics, engagement, insight-to-action, and executive summary APIs from BigQuery.
"""

import datetime
from flask import Flask, jsonify, request, send_from_directory
from google.cloud import bigquery
from config import (
    CORE_METRICS,
    ALL_ENGAGEMENT_METRICS,
    ENGAGEMENT_TAXONOMY,
    METRIC_DISPLAY,
)
from analytics import (
    compute_trends,
    detect_anomalies,
    analyze_segments,
    compute_funnel,
    compute_correlations,
    generate_executive_summary,
)

app = Flask(__name__, static_folder="static")
client = bigquery.Client(project="mc-analytics-devel")

# ── In-memory cache (24h TTL) ────────────────────────────────────────────────
_cache: dict = {}
_CACHE_TTL = 86400  # seconds


def _cache_get(key):
    entry = _cache.get(key)
    if entry:
        data, ts = entry
        if (datetime.datetime.utcnow() - ts).total_seconds() < _CACHE_TTL:
            return data
    return None


def _cache_set(key, data):
    _cache[key] = (data, datetime.datetime.utcnow())


TABLE = "mc-analytics-devel.bi_product.rpt_RA_L1L3_Test_03_04"


# ── Filter helpers ────────────────────────────────────────────────────────────

def _ecu_clause(ecu, prefix=""):
    p = f"{prefix}." if prefix else ""
    if ecu == "ecu":
        return f"{p}ecomm_level_end = 'ecu'"
    elif ecu == "non_ecu":
        return f"{p}ecomm_level_end IN ('non', 'ecomm')"
    return f"{p}ecomm_level_end IN ('ecu', 'non', 'ecomm')"


def _hvc_clause(hvc, prefix="", string_type=True):
    p = f"{prefix}." if prefix else ""
    if string_type:
        if hvc == "hvc":
            return f"{p}is_high_value = 'true'"
        elif hvc == "non_hvc":
            return f"{p}is_high_value = 'false'"
        return f"{p}is_high_value IN ('true', 'false')"
    else:
        if hvc == "hvc":
            return f"{p}is_high_value = TRUE"
        elif hvc == "non_hvc":
            return f"{p}is_high_value = FALSE"
        return f"{p}is_high_value IN (TRUE, FALSE)"


def _period_expr(granularity, col="week"):
    if granularity == "monthly":
        return f"DATE_TRUNC({col}, MONTH)"
    return col


def _get_filters():
    return (
        request.args.get("ecu", "all"),
        request.args.get("hvc", "all"),
        request.args.get("granularity", "weekly"),
    )


def _metrics_in_clause(metric_list):
    return ", ".join(f"'{m}'" for m in metric_list)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/metrics")
def get_metrics():
    """
    Core metrics endpoint — returns all KPI data with WoW and YoY comparisons.
    Expanded to include I2A metrics (automations, segments) and WoW data.
    """
    ecu, hvc, granularity = _get_filters()
    metrics_sql = _metrics_in_clause(CORE_METRICS)

    query = f"""
    WITH filtered AS (
        SELECT
            {_period_expr(granularity)} AS period_start,
            metric_name,
            metric_value,
            denominator,
            COALESCE(py, 0)            AS py,
            COALESCE(py_denominator, 0) AS py_denominator,
            COALESCE(pw, 0)            AS pw,
            COALESCE(pw_denominator, 0) AS pw_denominator
        FROM `{TABLE}`
        WHERE metric_name IN ({metrics_sql})
          AND {_ecu_clause(ecu)}
          AND {_hvc_clause(hvc)}
    ),
    pivoted AS (
        SELECT
            period_start,

            -- ── Current year numerators ──────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN metric_value ELSE 0 END) AS campaign_users,
            SUM(CASE WHEN metric_name = 'total_campaigns_created'    THEN metric_value ELSE 0 END) AS total_campaigns,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN metric_value ELSE 0 END) AS attributed_rev,
            SUM(CASE WHEN metric_name = 'RA_Viewed_Users'            THEN metric_value ELSE 0 END) AS ra_active_users,

            -- NEW: I2A expansion numerators
            SUM(CASE WHEN metric_name = 'automation_created_users'   THEN metric_value ELSE 0 END) AS automation_users,
            SUM(CASE WHEN metric_name = 'total_automations_created'  THEN metric_value ELSE 0 END) AS total_automations,
            SUM(CASE WHEN metric_name = 'segment_created_users'      THEN metric_value ELSE 0 END) AS segment_users,
            SUM(CASE WHEN metric_name = 'total_segments_created'     THEN metric_value ELSE 0 END) AS total_segments,
            SUM(CASE WHEN metric_name = 'actions_taken_users'        THEN metric_value ELSE 0 END) AS actions_taken_users,
            SUM(CASE WHEN metric_name = 'total_actions'              THEN metric_value ELSE 0 END) AS total_actions,
            SUM(CASE WHEN metric_name = 'campaign_sent_users'        THEN metric_value ELSE 0 END) AS campaign_sent_users,
            SUM(CASE WHEN metric_name = 'total_campaigns_sent'       THEN metric_value ELSE 0 END) AS total_campaigns_sent,

            -- ── Current year denominators ────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN denominator  ELSE 0 END) AS active_users,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN denominator  ELSE 0 END) AS total_rev,
            SUM(CASE WHEN metric_name = 'c2_users'                   THEN denominator  ELSE 0 END) AS c1s_users,

            -- ── Prior year numerators ────────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN py ELSE 0 END) AS py_campaign_users,
            SUM(CASE WHEN metric_name = 'total_campaigns_created'    THEN py ELSE 0 END) AS py_total_campaigns,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN py ELSE 0 END) AS py_attributed_rev,
            SUM(CASE WHEN metric_name = 'RA_Viewed_Users'            THEN py ELSE 0 END) AS py_ra_active_users,
            SUM(CASE WHEN metric_name = 'automation_created_users'   THEN py ELSE 0 END) AS py_automation_users,
            SUM(CASE WHEN metric_name = 'total_automations_created'  THEN py ELSE 0 END) AS py_total_automations,
            SUM(CASE WHEN metric_name = 'segment_created_users'      THEN py ELSE 0 END) AS py_segment_users,
            SUM(CASE WHEN metric_name = 'total_segments_created'     THEN py ELSE 0 END) AS py_total_segments,
            SUM(CASE WHEN metric_name = 'actions_taken_users'        THEN py ELSE 0 END) AS py_actions_taken_users,
            SUM(CASE WHEN metric_name = 'total_actions'              THEN py ELSE 0 END) AS py_total_actions,

            -- ── Prior year denominators ──────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN py_denominator ELSE 0 END) AS py_active_users,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN py_denominator ELSE 0 END) AS py_total_rev,

            -- ── Prior WEEK numerators (WoW comparison) ──────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN pw ELSE 0 END) AS pw_campaign_users,
            SUM(CASE WHEN metric_name = 'total_campaigns_created'    THEN pw ELSE 0 END) AS pw_total_campaigns,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN pw ELSE 0 END) AS pw_attributed_rev,
            SUM(CASE WHEN metric_name = 'RA_Viewed_Users'            THEN pw ELSE 0 END) AS pw_ra_active_users,
            SUM(CASE WHEN metric_name = 'automation_created_users'   THEN pw ELSE 0 END) AS pw_automation_users,
            SUM(CASE WHEN metric_name = 'total_automations_created'  THEN pw ELSE 0 END) AS pw_total_automations,
            SUM(CASE WHEN metric_name = 'segment_created_users'      THEN pw ELSE 0 END) AS pw_segment_users,
            SUM(CASE WHEN metric_name = 'total_segments_created'     THEN pw ELSE 0 END) AS pw_total_segments,
            SUM(CASE WHEN metric_name = 'actions_taken_users'        THEN pw ELSE 0 END) AS pw_actions_taken_users,
            SUM(CASE WHEN metric_name = 'total_actions'              THEN pw ELSE 0 END) AS pw_total_actions,

            -- ── Prior WEEK denominators ──────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN pw_denominator ELSE 0 END) AS pw_active_users,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN pw_denominator ELSE 0 END) AS pw_total_rev,

            -- R&A Adoption
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN metric_value ELSE 0 END) AS ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN metric_value ELSE 0 END) AS ra_supported_users,
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN py           ELSE 0 END) AS py_ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN py           ELSE 0 END) AS py_ra_supported_users,
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN pw           ELSE 0 END) AS pw_ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN pw           ELSE 0 END) AS pw_ra_supported_users

        FROM filtered
        GROUP BY 1
    )
    SELECT
        period_start,
        active_users,
        c1s_users,
        attributed_rev,
        total_rev,
        total_campaigns,
        campaign_users,
        ra_active_users,

        -- ── I2A expansion raw counts ────────────────────────────────────
        automation_users,
        total_automations,
        segment_users,
        total_segments,
        actions_taken_users,
        total_actions,
        campaign_sent_users,
        total_campaigns_sent,

        -- ── Metric 1: % Attributed Revenue ──────────────────────────────
        ROUND(SAFE_DIVIDE(attributed_rev,    total_rev)    * 100, 2) AS pct_attributed_rev,
        ROUND(SAFE_DIVIDE(py_attributed_rev, py_total_rev) * 100, 2) AS py_pct_attributed_rev,
        ROUND(SAFE_DIVIDE(pw_attributed_rev, pw_total_rev) * 100, 2) AS pw_pct_attributed_rev,

        -- ── Metric 2: % Created Campaign (I2A) ─────────────────────────
        ROUND(SAFE_DIVIDE(campaign_users,    active_users)    * 100, 2) AS pct_campaigns_created,
        ROUND(SAFE_DIVIDE(py_campaign_users, py_active_users) * 100, 2) AS py_pct_campaigns_created,
        ROUND(SAFE_DIVIDE(pw_campaign_users, pw_active_users) * 100, 2) AS pw_pct_campaigns_created,

        -- ── Metric 3: Campaigns per User ────────────────────────────────
        ROUND(SAFE_DIVIDE(total_campaigns,    active_users),    2) AS campaigns_per_user,
        ROUND(SAFE_DIVIDE(py_total_campaigns, py_active_users), 2) AS py_campaigns_per_user,
        ROUND(SAFE_DIVIDE(pw_total_campaigns, pw_active_users), 2) AS pw_campaigns_per_user,

        -- ── Metric 4: % Created Automation (I2A) ────────────────────────
        ROUND(SAFE_DIVIDE(automation_users,    active_users)    * 100, 2) AS pct_automations_created,
        ROUND(SAFE_DIVIDE(py_automation_users, py_active_users) * 100, 2) AS py_pct_automations_created,
        ROUND(SAFE_DIVIDE(pw_automation_users, pw_active_users) * 100, 2) AS pw_pct_automations_created,

        -- ── Metric 5: Automations per User ──────────────────────────────
        ROUND(SAFE_DIVIDE(total_automations,    active_users),    2) AS automations_per_user,
        ROUND(SAFE_DIVIDE(py_total_automations, py_active_users), 2) AS py_automations_per_user,
        ROUND(SAFE_DIVIDE(pw_total_automations, pw_active_users), 2) AS pw_automations_per_user,

        -- ── Metric 6: % Created Segment (I2A) ──────────────────────────
        ROUND(SAFE_DIVIDE(segment_users,    active_users)    * 100, 2) AS pct_segments_created,
        ROUND(SAFE_DIVIDE(py_segment_users, py_active_users) * 100, 2) AS py_pct_segments_created,
        ROUND(SAFE_DIVIDE(pw_segment_users, pw_active_users) * 100, 2) AS pw_pct_segments_created,

        -- ── Metric 7: Segments per User ─────────────────────────────────
        ROUND(SAFE_DIVIDE(total_segments,    active_users),    2) AS segments_per_user,
        ROUND(SAFE_DIVIDE(py_total_segments, py_active_users), 2) AS py_segments_per_user,
        ROUND(SAFE_DIVIDE(pw_total_segments, pw_active_users), 2) AS pw_segments_per_user,

        -- ── Metric 8: % Took Any Action (I2A composite) ────────────────
        ROUND(SAFE_DIVIDE(actions_taken_users,    active_users)    * 100, 2) AS pct_actions_taken,
        ROUND(SAFE_DIVIDE(py_actions_taken_users, py_active_users) * 100, 2) AS py_pct_actions_taken,
        ROUND(SAFE_DIVIDE(pw_actions_taken_users, pw_active_users) * 100, 2) AS pw_pct_actions_taken,

        -- ── R&A Adoption rates ──────────────────────────────────────────
        ra_owned_users,
        ROUND(SAFE_DIVIDE(ra_owned_users,     active_users) * 100, 2) AS pct_ra_owned,
        ROUND(SAFE_DIVIDE(ra_active_users,    active_users) * 100, 2) AS pct_ra_total,
        ROUND(SAFE_DIVIDE(py_ra_owned_users,  py_active_users) * 100, 2) AS py_pct_ra_owned,
        ROUND(SAFE_DIVIDE(py_ra_active_users, py_active_users) * 100, 2) AS py_pct_ra_total,
        ROUND(SAFE_DIVIDE(pw_ra_owned_users,  pw_active_users) * 100, 2) AS pw_pct_ra_owned,
        ROUND(SAFE_DIVIDE(pw_ra_active_users, pw_active_users) * 100, 2) AS pw_pct_ra_total

    FROM pivoted
    WHERE period_start IS NOT NULL
    ORDER BY period_start ASC
    """

    rows = []
    for row in client.query(query).result():
        r = dict(row)
        if hasattr(r.get("period_start"), "isoformat"):
            r["period_start"] = r["period_start"].isoformat()
        rows.append(r)

    return jsonify(rows)


@app.route("/api/adoption")
def get_adoption():
    """
    R&A Adoption — de-duplicated union from ECS (bypasses Dataform double-counting).
    Includes owned pages + supported CTA clicks.
    """
    ecu, hvc, granularity = _get_filters()

    cache_key = f"adoption|{ecu}|{hvc}|{granularity}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    period_expr = "DATE_TRUNC(b.week, MONTH)" if granularity == "monthly" else "b.week"

    query = f"""
    WITH base_user AS (
        SELECT w.user_id, w.week
        FROM `mc-analytics-devel.bi_product.user_dimensions_weekly_rollup` w
        WHERE w.week >= '2024-01-01'
          AND w.week < DATE_TRUNC(CURRENT_DATE, WEEK)
          AND w.wau
          AND w.is_high_value IS NOT NULL
          AND {_ecu_clause(ecu, 'w')}
          AND {_hvc_clause(hvc, 'w', string_type=False)}
        GROUP BY ALL
    ),
    owned_pages AS (
        SELECT DISTINCT DATE_TRUNC(DATE(timestamp), WEEK) AS week, user_id
        FROM `mc-business-intelligence.bi_activities.ecs_users_activities`
        WHERE DATE(timestamp) >= '2024-01-01'
          AND event = 'reporting:viewed'
          AND screen NOT IN ('/')
          AND (
            (scope_area = 'analytics' AND object_detail IN ('marketing_dashboard','email_dashboard'))
            OR screen IN ('/analytics/marketing-dashboard','/analytics/marketing-dashboard/',
                          'analytics/marketing-dashboard','analytics/marketing-dashboard/',
                          '/analytics/email-dashboard/','analytics/email-dashboard',
                          '/analytics/email-dashboard')
            OR (scope_area = 'analytics' AND object_detail = 'audience_analytics')
            OR screen IN ('/analytics/audience-analytics/','/analytics/audience-analytics',
                          'analytics/audience-analytics')
            OR (scope_area IN ('business_analytics','subscription_management')
                AND object_detail IN ('conversion_insights','revenue_plans'))
            OR screen LIKE '%analytics/conversion-insights%'
            OR (scope_area = 'analytics' AND object_detail = 'custom_reports')
            OR screen IN ('analytics/custom-reports','/analytics/custom-reports',
                          '/analytics/custom-reports/','/analytics/reports/custom-reports/builder',
                          '/analytics/reports/custom-reports/','analytics/custom-reports/details/',
                          'analytics/custom-reports/builder','analytics/custom-reports/graph/',
                          '/analytics/custom-reports/builder','/analytics/custom-reports/graph',
                          '/analytics/custom-reports/details','/analytics/reports/custom-reports',
                          '/analytics/reports/custom-reports/builder/',
                          '/analytics/custom-reports/details','/analytics/reports/custom-reports/')
            OR screen IN ('/reports/','/reports','reports')
            OR screen LIKE '%/reports/#f_list%'
            OR (scope_area = 'analytics' AND object_detail = 'sms_report')
            OR screen IN ('analytics/sms','/analytics/sms')
            OR (initiative_name = 'marketing_crm_analytics' AND scope_area = 'campaign_analytics'
                AND object_detail IN ('email_overview_report','email_activity_bounced_report',
                                      'email_ecomm_order_history_report','email_activity_sent_report',
                                      'email_activity_unsubscribed_report','email_social_report',
                                      'email_activity_opened_report','email_activity_clicked_report',
                                      'email_ecomm_product_activity_report','email_click_performance_report',
                                      'email_activity_complained_report','email_activity_not_opened_report'))
            OR screen IN ('/analytics/report/overview','/reports/clicks','/reports/activity/open',
                          '/reports/activity/bounced','/reports/activity/unsubscribed',
                          '/reports/activity/sent','/reports/activity/clicked',
                          '/reports/activity/not-opened','reports/social-stats',
                          '/reports/ecommerce/history','/reports/activity/abuse',
                          '/reports/ecommerce/activity','/reports/summary',
                          'analytics/report/recipient-activity/','analytics/reports/click-performance',
                          '/analytics/reports/overview','/reports/summary/',
                          'analytics/reports/overview','/reports/clickmap','/reports/clicks/',
                          '/i/reports/clicks/','/reports/activity/opened',
                          'reports/activity/unsubscribed','/reports/activity/click-activity',
                          '/reports/social-stats','reports/summary','/i/reports/summary/',
                          '/analytics/reports/click-performance','/analytics/reports/recipient-activity')
            OR (initiative_name = 'core_offerings'
                AND object_detail IN ('cjb_original_report','cjb_overview_report',
                                      'customer_journey_builder_report'))
            OR screen IN ('analytics/cjb_report','/customer-journey/report',
                          'customer_journey/report','customer-journey/report')
            OR (scope_area = 'campaign_analytics' AND object_detail = 'marketing_insights'
                AND screen IN ('/insights-agent','/insights-agent/'))
          )
    ),
    supported_cta AS (
        SELECT DISTINCT DATE_TRUNC(DATE(timestamp), WEEK) AS week, user_id
        FROM `mc-business-intelligence.bi_activities.ecs_users_activities`
        WHERE DATE(timestamp) >= '2024-01-01'
          AND (
            (event = 'reporting:engaged' AND ui_object_detail = 'audience_analytics' AND screen = '/')
            OR (event = 'reporting:engaged' AND ui_object_detail = 'marketing_dashboard' AND screen = '/')
            OR (event = 'automations:engaged' AND ui_object_detail = 'view_analytics')
            OR (event = 'campaign:engaged' AND ui_object_detail = 'view_report'
                AND screen LIKE '%/campaigns%')
          )
    ),
    user_period AS (
        SELECT
            {period_expr} AS period_start,
            b.user_id,
            MAX(CASE WHEN op.user_id IS NOT NULL THEN 1 ELSE 0 END) AS is_owned,
            MAX(CASE WHEN sc.user_id IS NOT NULL THEN 1 ELSE 0 END) AS is_cta
        FROM base_user b
        LEFT JOIN owned_pages op ON b.user_id = op.user_id AND b.week = op.week
        LEFT JOIN supported_cta sc ON b.user_id = sc.user_id AND b.week = sc.week
        GROUP BY 1, 2
    )
    SELECT
        period_start,
        COUNT(DISTINCT user_id) AS active_users,
        ROUND(SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN is_owned = 1 THEN user_id END),
            COUNT(DISTINCT user_id)) * 100, 2) AS pct_ra_owned,
        ROUND(SAFE_DIVIDE(
            COUNT(DISTINCT CASE WHEN is_owned = 1 OR is_cta = 1 THEN user_id END),
            COUNT(DISTINCT user_id)) * 100, 2) AS pct_ra_total
    FROM user_period
    WHERE period_start IS NOT NULL
    GROUP BY 1
    ORDER BY 1 ASC
    """

    rows = []
    for row in client.query(query).result():
        r = dict(row)
        if hasattr(r.get("period_start"), "isoformat"):
            r["period_start"] = r["period_start"].isoformat()
        rows.append(r)

    _cache_set(cache_key, rows)
    return jsonify(rows)


@app.route("/api/engagement")
def get_engagement():
    """
    Engagement breakdown by page — owned, supported, and combined.
    Reads L2/L3 metrics from the report table.
    """
    ecu, hvc, granularity = _get_filters()

    cache_key = f"engagement|{ecu}|{hvc}|{granularity}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    all_metrics = ALL_ENGAGEMENT_METRICS + ["RA_Viewed_Users", "RA_Engaged_Users"]
    metrics_sql = _metrics_in_clause(all_metrics)

    query = f"""
    SELECT
        {_period_expr(granularity)} AS period_start,
        metric_name,
        SUM(metric_value) AS metric_value,
        SUM(denominator)  AS denominator,
        SUM(COALESCE(py, 0)) AS py,
        SUM(COALESCE(py_denominator, 0)) AS py_denominator,
        SUM(COALESCE(pw, 0)) AS pw,
        SUM(COALESCE(pw_denominator, 0)) AS pw_denominator
    FROM `{TABLE}`
    WHERE metric_name IN ({metrics_sql})
      AND {_ecu_clause(ecu)}
      AND {_hvc_clause(hvc)}
    GROUP BY 1, 2
    ORDER BY 1 ASC, 2
    """

    # Pivot into per-period dicts with page-level counts
    period_map = {}
    for row in client.query(query).result():
        r = dict(row)
        ps = r["period_start"]
        if hasattr(ps, "isoformat"):
            ps = ps.isoformat()

        if ps not in period_map:
            period_map[ps] = {
                "period_start": ps,
                "owned": {},
                "supported": {},
                "owned_engaged": {},
                "supported_engaged": {},
                "combined": {},
            }

        mn = r["metric_name"]
        val = r["metric_value"] or 0
        denom = r["denominator"] or 0
        py_val = r["py"] or 0
        pw_val = r["pw"] or 0

        entry = {
            "users": val,
            "rate": round((val / denom * 100) if denom > 0 else 0, 2),
            "py_users": py_val,
            "pw_users": pw_val,
            "denominator": denom,
        }

        # Classify into categories using taxonomy
        for display_name, metric_name in ENGAGEMENT_TAXONOMY.get("owned_viewers", {}).items():
            if mn == metric_name:
                period_map[ps]["owned"][display_name] = entry
        for display_name, metric_name in ENGAGEMENT_TAXONOMY.get("supported_viewers", {}).items():
            if mn == metric_name:
                period_map[ps]["supported"][display_name] = entry
        for display_name, metric_name in ENGAGEMENT_TAXONOMY.get("owned_engaged", {}).items():
            if mn == metric_name:
                period_map[ps]["owned_engaged"][display_name] = entry
        for display_name, metric_name in ENGAGEMENT_TAXONOMY.get("supported_engaged", {}).items():
            if mn == metric_name:
                period_map[ps]["supported_engaged"][display_name] = entry
        for display_name, metric_name in ENGAGEMENT_TAXONOMY.get("combined", {}).items():
            if mn == metric_name:
                period_map[ps]["combined"][display_name] = entry

    rows = list(period_map.values())
    rows.sort(key=lambda x: x["period_start"])

    _cache_set(cache_key, rows)
    return jsonify(rows)


@app.route("/api/executive-summary")
def get_executive_summary():
    """
    Executive summary — runs analytics engine on metrics data and returns
    6-8 prioritized insight bullets.
    """
    ecu, hvc, granularity = _get_filters()
    compare = request.args.get("compare", "wow")

    cache_key = f"exec_summary|{ecu}|{hvc}|{granularity}|{compare}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    # Fetch metrics data (reuse the metrics endpoint logic)
    import json
    with app.test_request_context(f"/api/metrics?ecu={ecu}&hvc={hvc}&granularity={granularity}"):
        metrics_response = get_metrics()
        metrics_data = json.loads(metrics_response.get_data())

    if not metrics_data:
        return jsonify({"period": None, "summary": []})

    # Define metric keys for analysis
    rate_keys = [
        "pct_attributed_rev", "pct_campaigns_created", "campaigns_per_user",
        "pct_automations_created", "automations_per_user",
        "pct_segments_created", "segments_per_user",
        "pct_actions_taken", "pct_ra_owned", "pct_ra_total",
    ]

    # Compute analytics
    trends = compute_trends(metrics_data, rate_keys)
    anomalies = detect_anomalies(metrics_data, rate_keys)

    # Segment analysis — fetch ECU and HVC slices if currently on "all"
    segment_data = {}
    if ecu == "all":
        for seg_val, seg_name in [("ecu", "ECU"), ("non_ecu", "Non-ECU")]:
            with app.test_request_context(
                f"/api/metrics?ecu={seg_val}&hvc={hvc}&granularity={granularity}"
            ):
                resp = get_metrics()
                seg_rows = json.loads(resp.get_data())
                if seg_rows:
                    segment_data[seg_name] = seg_rows

    segments = analyze_segments(segment_data, rate_keys) if segment_data else []

    # Funnel analysis — use latest period
    latest = metrics_data[-1] if metrics_data else {}
    funnel_data = {
        "active_users": latest.get("active_users", 0),
        "ra_viewed_users": latest.get("ra_active_users", 0),
        "ra_owned_users": latest.get("ra_owned_users", 0),
        "ra_engaged_users": 0,  # Not directly in metrics endpoint
        "actions_taken_users": latest.get("actions_taken_users", 0),
    }
    funnel = compute_funnel(funnel_data)

    # Correlation analysis
    engagement_keys = ["pct_ra_owned", "pct_ra_total"]
    outcome_keys = ["pct_campaigns_created", "pct_actions_taken"]
    correlations = compute_correlations(metrics_data, engagement_keys, outcome_keys)

    # Generate summary
    summary = generate_executive_summary(
        trends, anomalies, segments, funnel, correlations,
        comparison_mode=compare,
    )

    result = {
        "period": metrics_data[-1].get("period_start") if metrics_data else None,
        "comparison_mode": compare,
        "summary": summary,
        "analytics": {
            "trends": {k: {kk: vv for kk, vv in v.items() if kk != "moving_avg"}
                       for k, v in trends.items()},
            "anomalies": anomalies[:5],
            "funnel": funnel,
            "top_correlations": correlations[:3],
        },
    }

    _cache_set(cache_key, result)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5050)
