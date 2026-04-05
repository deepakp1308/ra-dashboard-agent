"""
R&A Executive Dashboard — Flask Backend.
Serves metrics, engagement, insight-to-action, and executive summary APIs from BigQuery.
"""

import datetime
import json
import logging
import time

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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ra_dashboard")

app = Flask(__name__, static_folder="static")
client = bigquery.Client(project="mc-analytics-devel")

# ── In-memory cache (24h TTL) ────────────────────────────────────────────────
_cache: dict = {}
_CACHE_TTL = 86400  # seconds


def _cache_get(key):
    entry = _cache.get(key)
    if entry:
        data, ts = entry
        age = (datetime.datetime.utcnow() - ts).total_seconds()
        if age < _CACHE_TTL:
            return data, ts, True  # data, timestamp, is_cache_hit
    return None, None, False


def _cache_set(key, data):
    _cache[key] = (data, datetime.datetime.utcnow())


TABLE = "mc-analytics-devel.bi_product.rpt_RA_L1L3_Test_03_04"


# ── Input validation (Fix 1 — SQL injection prevention) ──────────────────────
VALID_ECU = {"all", "ecu", "non_ecu"}
VALID_HVC = {"all", "hvc", "non_hvc"}
VALID_GRANULARITY = {"weekly", "monthly"}
VALID_COMPARE = {"none", "wow", "yoy"}


def _validate_filters(ecu, hvc, granularity):
    """Reject invalid filter values before they reach SQL."""
    errors = []
    if ecu not in VALID_ECU:
        errors.append(f"Invalid ecu value: '{ecu}'. Must be one of {VALID_ECU}")
    if hvc not in VALID_HVC:
        errors.append(f"Invalid hvc value: '{hvc}'. Must be one of {VALID_HVC}")
    if granularity not in VALID_GRANULARITY:
        errors.append(f"Invalid granularity: '{granularity}'. Must be one of {VALID_GRANULARITY}")
    return errors


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
    ecu = request.args.get("ecu", "all")
    hvc = request.args.get("hvc", "all")
    granularity = request.args.get("granularity", "weekly")
    errors = _validate_filters(ecu, hvc, granularity)
    if errors:
        return None, None, None, errors
    return ecu, hvc, granularity, None


def _metrics_in_clause(metric_list):
    return ", ".join(f"'{m}'" for m in metric_list)


def _run_query(query, description="query"):
    """Execute a BigQuery query with error handling and timing."""
    start = time.time()
    try:
        result = client.query(query).result()
        rows = []
        for row in result:
            r = dict(row)
            if hasattr(r.get("period_start"), "isoformat"):
                r["period_start"] = r["period_start"].isoformat()
            rows.append(r)
        duration = time.time() - start
        logger.info(f"{description}: {len(rows)} rows in {duration:.2f}s")
        return rows, None
    except Exception as e:
        duration = time.time() - start
        logger.error(f"{description} FAILED after {duration:.2f}s: {e}")
        return None, str(e)


def _make_response(data, cache_ts=None, cache_hit=False):
    """Wrap data with metadata for freshness tracking (Fix 4)."""
    now = datetime.datetime.utcnow().isoformat() + "Z"
    extracted = cache_ts.isoformat() + "Z" if cache_ts else now
    return jsonify({
        "data": data,
        "meta": {
            "extracted_at": extracted,
            "served_at": now,
            "cache_hit": cache_hit,
        },
    })


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/metrics")
def get_metrics():
    """
    Core metrics endpoint — returns all KPI data with WoW and YoY comparisons.
    Expanded to include I2A metrics (automations, segments) and WoW data.

    Fix 2: WoW (pw_) columns excluded for monthly granularity.
    Fix 5: category = 'L1' filter prevents double-counting across L1/L2/L3.
    Fix 6: pw_has_data / py_has_data flags for null-vs-zero distinction.
    """
    ecu, hvc, granularity, errors = _get_filters()
    if errors:
        return jsonify({"error": errors}), 400

    metrics_sql = _metrics_in_clause(CORE_METRICS)
    is_weekly = granularity == "weekly"

    # Fix 2: only include pw_ columns for weekly granularity
    pw_select = ""
    pw_pivot = ""
    if is_weekly:
        pw_pivot = """
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
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN pw_denominator ELSE 0 END) AS pw_active_users,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN pw_denominator ELSE 0 END) AS pw_total_rev,
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN pw           ELSE 0 END) AS pw_ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN pw           ELSE 0 END) AS pw_ra_supported_users,
            -- Fix 6: null-vs-zero flag
            COUNTIF(metric_name = 'campaign_created_users' AND pw IS NOT NULL) > 0 AS pw_has_data,
        """
        pw_select = """
        ROUND(SAFE_DIVIDE(pw_attributed_rev, pw_total_rev) * 100, 2) AS pw_pct_attributed_rev,
        ROUND(SAFE_DIVIDE(pw_campaign_users, pw_active_users) * 100, 2) AS pw_pct_campaigns_created,
        ROUND(SAFE_DIVIDE(pw_total_campaigns, pw_active_users), 2) AS pw_campaigns_per_user,
        ROUND(SAFE_DIVIDE(pw_automation_users, pw_active_users) * 100, 2) AS pw_pct_automations_created,
        ROUND(SAFE_DIVIDE(pw_total_automations, pw_active_users), 2) AS pw_automations_per_user,
        ROUND(SAFE_DIVIDE(pw_segment_users, pw_active_users) * 100, 2) AS pw_pct_segments_created,
        ROUND(SAFE_DIVIDE(pw_total_segments, pw_active_users), 2) AS pw_segments_per_user,
        ROUND(SAFE_DIVIDE(pw_actions_taken_users, pw_active_users) * 100, 2) AS pw_pct_actions_taken,
        ROUND(SAFE_DIVIDE(pw_ra_owned_users, pw_active_users) * 100, 2) AS pw_pct_ra_owned,
        ROUND(SAFE_DIVIDE(pw_ra_active_users, pw_active_users) * 100, 2) AS pw_pct_ra_total,
        pw_has_data,
        """

    query = f"""
    WITH filtered AS (
        SELECT
            {_period_expr(granularity)} AS period_start,
            metric_name,
            metric_value,
            denominator,
            py,
            COALESCE(py, 0)            AS py_val,
            COALESCE(py_denominator, 0) AS py_denominator,
            COALESCE(pw, 0)            AS pw,
            COALESCE(pw_denominator, 0) AS pw_denominator
        FROM `{TABLE}`
        WHERE metric_name IN ({metrics_sql})
          AND category = 'L1'
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
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN py_val ELSE 0 END) AS py_campaign_users,
            SUM(CASE WHEN metric_name = 'total_campaigns_created'    THEN py_val ELSE 0 END) AS py_total_campaigns,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN py_val ELSE 0 END) AS py_attributed_rev,
            SUM(CASE WHEN metric_name = 'RA_Viewed_Users'            THEN py_val ELSE 0 END) AS py_ra_active_users,
            SUM(CASE WHEN metric_name = 'automation_created_users'   THEN py_val ELSE 0 END) AS py_automation_users,
            SUM(CASE WHEN metric_name = 'total_automations_created'  THEN py_val ELSE 0 END) AS py_total_automations,
            SUM(CASE WHEN metric_name = 'segment_created_users'      THEN py_val ELSE 0 END) AS py_segment_users,
            SUM(CASE WHEN metric_name = 'total_segments_created'     THEN py_val ELSE 0 END) AS py_total_segments,
            SUM(CASE WHEN metric_name = 'actions_taken_users'        THEN py_val ELSE 0 END) AS py_actions_taken_users,
            SUM(CASE WHEN metric_name = 'total_actions'              THEN py_val ELSE 0 END) AS py_total_actions,
            -- Fix 6: null-vs-zero flag for prior year
            COUNTIF(metric_name = 'campaign_created_users' AND py IS NOT NULL) > 0 AS py_has_data,

            -- ── Prior year denominators ──────────────────────────────────
            SUM(CASE WHEN metric_name = 'campaign_created_users'     THEN py_denominator ELSE 0 END) AS py_active_users,
            SUM(CASE WHEN metric_name = 'total_attributable_revenue' THEN py_denominator ELSE 0 END) AS py_total_rev,

            {pw_pivot}

            -- R&A Adoption
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN metric_value ELSE 0 END) AS ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN metric_value ELSE 0 END) AS ra_supported_users,
            SUM(CASE WHEN metric_name = 'RA_Owned_Viewed_Users'      THEN py_val        ELSE 0 END) AS py_ra_owned_users,
            SUM(CASE WHEN metric_name = 'RA_Supported_Engaged_Users' THEN py_val        ELSE 0 END) AS py_ra_supported_users

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
        automation_users,
        total_automations,
        segment_users,
        total_segments,
        actions_taken_users,
        total_actions,
        campaign_sent_users,
        total_campaigns_sent,

        -- ── Rates: current ──────────────────────────────────────────────
        ROUND(SAFE_DIVIDE(attributed_rev,    total_rev)    * 100, 2) AS pct_attributed_rev,
        ROUND(SAFE_DIVIDE(campaign_users,    active_users)    * 100, 2) AS pct_campaigns_created,
        ROUND(SAFE_DIVIDE(total_campaigns,    active_users),    2) AS campaigns_per_user,
        ROUND(SAFE_DIVIDE(automation_users,    active_users)    * 100, 2) AS pct_automations_created,
        ROUND(SAFE_DIVIDE(total_automations,    active_users),    2) AS automations_per_user,
        ROUND(SAFE_DIVIDE(segment_users,    active_users)    * 100, 2) AS pct_segments_created,
        ROUND(SAFE_DIVIDE(total_segments,    active_users),    2) AS segments_per_user,
        ROUND(SAFE_DIVIDE(actions_taken_users,    active_users)    * 100, 2) AS pct_actions_taken,

        -- ── Rates: prior year ───────────────────────────────────────────
        ROUND(SAFE_DIVIDE(py_attributed_rev, py_total_rev) * 100, 2) AS py_pct_attributed_rev,
        ROUND(SAFE_DIVIDE(py_campaign_users, py_active_users) * 100, 2) AS py_pct_campaigns_created,
        ROUND(SAFE_DIVIDE(py_total_campaigns, py_active_users), 2) AS py_campaigns_per_user,
        ROUND(SAFE_DIVIDE(py_automation_users, py_active_users) * 100, 2) AS py_pct_automations_created,
        ROUND(SAFE_DIVIDE(py_total_automations, py_active_users), 2) AS py_automations_per_user,
        ROUND(SAFE_DIVIDE(py_segment_users, py_active_users) * 100, 2) AS py_pct_segments_created,
        ROUND(SAFE_DIVIDE(py_total_segments, py_active_users), 2) AS py_segments_per_user,
        ROUND(SAFE_DIVIDE(py_actions_taken_users, py_active_users) * 100, 2) AS py_pct_actions_taken,
        py_has_data,

        -- ── Rates: prior week (only for weekly granularity) ─────────────
        {pw_select}

        -- ── R&A Adoption rates ──────────────────────────────────────────
        ra_owned_users,
        ROUND(SAFE_DIVIDE(ra_owned_users,     active_users) * 100, 2) AS pct_ra_owned,
        ROUND(SAFE_DIVIDE(ra_active_users,    active_users) * 100, 2) AS pct_ra_total,
        ROUND(SAFE_DIVIDE(py_ra_owned_users,  py_active_users) * 100, 2) AS py_pct_ra_owned,
        ROUND(SAFE_DIVIDE(py_ra_active_users, py_active_users) * 100, 2) AS py_pct_ra_total

    FROM pivoted
    WHERE period_start IS NOT NULL
    ORDER BY period_start ASC
    """

    rows, err = _run_query(query, f"metrics(ecu={ecu},hvc={hvc},gran={granularity})")
    if err:
        return jsonify({"error": f"BigQuery error: {err}"}), 503
    return jsonify(rows)


@app.route("/api/adoption")
def get_adoption():
    """
    R&A Adoption — de-duplicated union from ECS (bypasses Dataform double-counting).
    """
    ecu, hvc, granularity, errors = _get_filters()
    if errors:
        return jsonify({"error": errors}), 400

    cache_key = f"adoption|{ecu}|{hvc}|{granularity}"
    cached, cache_ts, hit = _cache_get(cache_key)
    if hit:
        logger.info(f"adoption cache HIT (age={(datetime.datetime.utcnow()-cache_ts).total_seconds():.0f}s)")
        return _make_response(cached, cache_ts, cache_hit=True)

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

    rows, err = _run_query(query, f"adoption(ecu={ecu},hvc={hvc})")
    if err:
        return jsonify({"error": f"BigQuery error: {err}"}), 503

    _cache_set(cache_key, rows)
    return _make_response(rows)


I2A_WINDOW_DAYS = 7  # Attribution window: actions within 7 days of first R&A view


@app.route("/api/i2a")
def get_i2a():
    """
    Insight-to-Action with 7-day attribution window.
    Queries raw tables directly (not the pre-aggregated Dataform table which uses 1-day).
    Returns weekly data with current, prior-year, and prior-week rates.
    """
    ecu, hvc, granularity, errors = _get_filters()
    if errors:
        return jsonify({"error": errors}), 400

    cache_key = f"i2a|{ecu}|{hvc}|{granularity}"
    cached, cache_ts, hit = _cache_get(cache_key)
    if hit:
        return jsonify(cached)

    period_expr_bu = _period_expr(granularity, "b.week")

    query = f"""
    WITH base_user AS (
        SELECT w.user_id, w.week,
               ecomm_level_end,
               CAST(is_high_value AS STRING) AS is_high_value
        FROM `mc-analytics-devel.bi_product.user_dimensions_weekly_rollup` w
        WHERE w.week >= '2024-01-01'
          AND w.week < DATE_TRUNC(CURRENT_DATE, WEEK)
          AND w.wau
          AND w.is_high_value IS NOT NULL
          AND {_ecu_clause(ecu, 'w')}
          AND {_hvc_clause(hvc, 'w', string_type=False)}
        GROUP BY ALL
    ),

    ra_views AS (
        SELECT user_id,
               DATE_TRUNC(DATE(timestamp), WEEK) AS week,
               MIN(DATE(timestamp)) AS first_viewed_date
        FROM `mc-business-intelligence.bi_activities.ecs_users_activities`
        WHERE DATE(timestamp) >= '2024-01-01'
          AND (
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
            OR screen IN ('/','/campaigns/','/campaigns','/customer-journey/','/audience/','/audience/contacts/','/audience/segments/','/sms/')
          )
        GROUP BY user_id, week
    ),

    campaigns AS (
        SELECT a.user_id,
               DATE_TRUNC(DATE(a.created_at), WEEK) AS week,
               COUNT(DISTINCT CONCAT(a.user_id, a.campaign_id)) AS campaigns_created
        FROM `mc-business-intelligence.bi_reporting.emails_bulk` a
        INNER JOIN ra_views v
            ON a.user_id = v.user_id
            AND DATE_TRUNC(DATE(a.created_at), WEEK) = v.week
            AND DATE(a.created_at) >= v.first_viewed_date
            AND DATE_DIFF(DATE(a.created_at), v.first_viewed_date, DAY) <= {I2A_WINDOW_DAYS}
        WHERE DATE(a.created_at) >= '2024-01-01'
        GROUP BY ALL
    ),

    automations AS (
        SELECT a.user_id,
               DATE_TRUNC(DATE(a.created_at), WEEK) AS week,
               COUNT(DISTINCT CONCAT(a.user_id, a.workflow_id)) AS automations_created
        FROM `mc-business-intelligence.bi_reporting.customer_journey_workflows` a
        INNER JOIN ra_views v
            ON a.user_id = v.user_id
            AND DATE_TRUNC(DATE(a.created_at), WEEK) = v.week
            AND DATE(a.created_at) >= v.first_viewed_date
            AND DATE_DIFF(DATE(a.created_at), v.first_viewed_date, DAY) <= {I2A_WINDOW_DAYS}
        WHERE DATE(a.created_at) >= '2024-01-01'
        GROUP BY ALL
    ),

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
            AND DATE_DIFF(a.action_date, v.first_viewed_date, DAY) <= {I2A_WINDOW_DAYS}
        WHERE a.action_date >= '2024-01-01'
        GROUP BY ALL
    ),

    weekly AS (
        SELECT
            {period_expr_bu} AS period_start,
            COUNT(DISTINCT b.user_id) AS active_users,
            COUNT(DISTINCT CASE WHEN c.campaigns_created > 0 THEN b.user_id END) AS campaign_users,
            COUNT(DISTINCT CASE WHEN a.automations_created > 0 THEN b.user_id END) AS automation_users,
            COUNT(DISTINCT CASE WHEN s.segments_created > 0 THEN b.user_id END) AS segment_users,
            COUNT(DISTINCT CASE WHEN c.campaigns_created > 0
                                  OR a.automations_created > 0
                                  OR s.segments_created > 0
                               THEN b.user_id END) AS actions_taken_users,
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
        period_start,
        active_users,
        campaign_users,
        automation_users,
        segment_users,
        actions_taken_users,
        total_campaigns,
        total_automations,
        total_segments,
        ROUND(SAFE_DIVIDE(campaign_users, active_users) * 100, 2) AS pct_campaigns_created,
        ROUND(SAFE_DIVIDE(automation_users, active_users) * 100, 2) AS pct_automations_created,
        ROUND(SAFE_DIVIDE(segment_users, active_users) * 100, 2) AS pct_segments_created,
        ROUND(SAFE_DIVIDE(actions_taken_users, active_users) * 100, 2) AS pct_actions_taken,
        ROUND(SAFE_DIVIDE(total_campaigns, active_users), 3) AS campaigns_per_user,
        ROUND(SAFE_DIVIDE(total_automations, active_users), 4) AS automations_per_user,
        ROUND(SAFE_DIVIDE(total_segments, active_users), 4) AS segments_per_user,
        -- Prior period values via LAG
        LAG(ROUND(SAFE_DIVIDE(campaign_users, active_users) * 100, 2)) OVER (ORDER BY period_start) AS pw_pct_campaigns_created,
        LAG(ROUND(SAFE_DIVIDE(automation_users, active_users) * 100, 2)) OVER (ORDER BY period_start) AS pw_pct_automations_created,
        LAG(ROUND(SAFE_DIVIDE(segment_users, active_users) * 100, 2)) OVER (ORDER BY period_start) AS pw_pct_segments_created,
        LAG(ROUND(SAFE_DIVIDE(actions_taken_users, active_users) * 100, 2)) OVER (ORDER BY period_start) AS pw_pct_actions_taken,
        LAG(ROUND(SAFE_DIVIDE(total_campaigns, active_users), 3)) OVER (ORDER BY period_start) AS pw_campaigns_per_user
    FROM weekly
    WHERE period_start IS NOT NULL
    ORDER BY period_start ASC
    """

    rows, err = _run_query(query, f"i2a(ecu={ecu},hvc={hvc},window={I2A_WINDOW_DAYS}d)")
    if err:
        return jsonify({"error": f"BigQuery error: {err}"}), 503

    # Add YoY by matching period_number (week-of-year) across years
    if rows and len(rows) > 52:
        for i in range(52, len(rows)):
            for key in ["pct_campaigns_created", "pct_automations_created",
                        "pct_segments_created", "pct_actions_taken", "campaigns_per_user"]:
                rows[i][f"py_{key}"] = rows[i - 52].get(key)
    elif rows:
        for r in rows:
            for key in ["pct_campaigns_created", "pct_automations_created",
                        "pct_segments_created", "pct_actions_taken", "campaigns_per_user"]:
                r[f"py_{key}"] = None

    _cache_set(cache_key, rows)
    return jsonify(rows)


@app.route("/api/engagement")
def get_engagement():
    """Engagement breakdown by page — owned, supported, and combined."""
    ecu, hvc, granularity, errors = _get_filters()
    if errors:
        return jsonify({"error": errors}), 400

    cache_key = f"engagement|{ecu}|{hvc}|{granularity}"
    cached, cache_ts, hit = _cache_get(cache_key)
    if hit:
        return _make_response(cached, cache_ts, cache_hit=True)

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

    rows, err = _run_query(query, f"engagement(ecu={ecu},hvc={hvc})")
    if err:
        return jsonify({"error": f"BigQuery error: {err}"}), 503

    # Pivot into per-period dicts with page-level counts
    period_map = {}
    for r in rows:
        ps = r["period_start"]
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

    result = list(period_map.values())
    result.sort(key=lambda x: x["period_start"])

    _cache_set(cache_key, result)
    return _make_response(result)


@app.route("/api/executive-summary")
def get_executive_summary():
    """Executive summary — analytics engine produces 6-8 prioritized insight bullets."""
    ecu, hvc, granularity, errors = _get_filters()
    if errors:
        return jsonify({"error": errors}), 400

    compare = request.args.get("compare", "wow")
    if compare not in VALID_COMPARE:
        return jsonify({"error": f"Invalid compare: '{compare}'"}), 400

    cache_key = f"exec_summary|{ecu}|{hvc}|{granularity}|{compare}"
    cached, cache_ts, hit = _cache_get(cache_key)
    if hit:
        return jsonify(cached)

    # Fetch metrics data internally
    with app.test_request_context(f"/api/metrics?ecu={ecu}&hvc={hvc}&granularity={granularity}"):
        metrics_response = get_metrics()
        metrics_data = json.loads(metrics_response.get_data())

    if not metrics_data:
        return jsonify({"period": None, "comparison_mode": compare, "summary": [], "analytics": {}})

    rate_keys = [
        "pct_attributed_rev", "pct_campaigns_created", "campaigns_per_user",
        "pct_automations_created", "automations_per_user",
        "pct_segments_created", "segments_per_user",
        "pct_actions_taken", "pct_ra_owned", "pct_ra_total",
    ]

    trends = compute_trends(metrics_data, rate_keys)
    anomalies = detect_anomalies(metrics_data, rate_keys)

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

    latest = metrics_data[-1] if metrics_data else {}
    funnel_data = {
        "active_users": latest.get("active_users", 0),
        "ra_viewed_users": latest.get("ra_active_users", 0),
        "ra_owned_users": latest.get("ra_owned_users", 0),
        "ra_engaged_users": 0,
        "actions_taken_users": latest.get("actions_taken_users", 0),
    }
    funnel = compute_funnel(funnel_data)

    correlations = compute_correlations(
        metrics_data, ["pct_ra_owned", "pct_ra_total"],
        ["pct_campaigns_created", "pct_actions_taken"],
    )

    summary = generate_executive_summary(
        trends, anomalies, segments, funnel, correlations,
        comparison_mode=compare,
    )

    result = {
        "period": latest.get("period_start"),
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
