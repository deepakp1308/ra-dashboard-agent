"""
Configuration for R&A Executive Dashboard.
Centralizes engagement taxonomy, metric definitions, and analytics thresholds.
"""

# ── Engagement Taxonomy ───────────────────────────────────────────────────────
# Maps display names to metric_name values in rpt_RA_L1L3_Test_03_04.
# "viewers" = reporting:viewed events; "engaged" = non-view interaction events.

ENGAGEMENT_TAXONOMY = {
    "owned_viewers": {
        "Marketing Dashboard": "Marketing_Dashboard_Viewers",
        "Audience Analytics": "Audience_analytic_viewers",
        "Conversion Insights": "Conversion_Insights_viewers",
        "Custom Reports": "Custom_Report_Viewers",
        "Reports": "Reports_Viewers",
        "SMS Report": "SmS_Report_Viewers",
        "Email Report": "Email_Report_Viewers",
        "Journey Report": "Journey_Report_Viewers",
        "Replicated": "Replicated_Viewers",
    },
    "supported_viewers": {
        "Homepage": "Homepage_Viewers",
        "All Campaigns": "All_Campaigns_Viewers",
        "All Journeys": "All_Journeys_Viewers",
        "Automation Overview": "Automation_Overview_Viewers",
        "All Contacts": "All_Contacts_Viewers",
        "Audience Dashboard": "Audience_Dashboard_Viewers",
        "Audience Segments": "Audience_Segment_Viewers",
        "SMS Overview": "SMS_Overview_Viewers",
    },
    "owned_engaged": {
        "Marketing Dashboard": "Marketing_Dashboard_Engaged",
        "Audience Analytics": "Audience_analytic_Engaged",
        "Conversion Insights": "Conversion_Insights_Engaged",
        "Custom Reports": "Custom_analytics_Engaged",
        "Reports": "Report_Engaged",
        "SMS Report": "SmS_Report_Engaged",
        "Email Report": "Email_Report_Engaged",
        "Journey Report": "CJB_Report_Engaged",
    },
    "supported_engaged": {
        "Homepage": "Homepage_Engaged",
        "All Campaigns": "All_campaign_Engaged",
        "Automation Overview": "Automation_overview_Engaged_users",
        "All Journeys": "All_journeys_Engaged",
        "All Contacts": "All_contacts_Engaged",
        "Audience Dashboard": "Audience_dashboard_Engaged",
        "Audience Segments": "Audience_Segment_Engaged",
        "SMS Overview": "Sms_overview_Engaged",
    },
    "combined": {
        "RA Viewed (All)": "RA_Viewed_Users",
        "RA Engaged (All)": "RA_Engaged_Users",
        "RA Owned Viewed": "RA_Owned_Viewed_Users",
        "RA Owned Engaged": "RA_Owned_Engaged_Users",
        "RA Supported Engaged": "RA_Supported_Engaged_Users",
    },
}

# Flat list of ALL engagement metric names for the SQL IN clause
ALL_ENGAGEMENT_METRICS = sorted(
    set(
        name
        for group in ENGAGEMENT_TAXONOMY.values()
        for name in group.values()
    )
)

# ── Insight-to-Action Metric Names ────────────────────────────────────────────

I2A_METRICS = [
    # Campaign
    "campaign_created_users",
    "total_campaigns_created",
    "campaign_sent_users",
    "total_campaigns_sent",
    # Automation
    "automation_created_users",
    "total_automations_created",
    # Segment
    "segment_created_users",
    "total_segments_created",
    # Composite
    "actions_taken_users",
    "total_actions",
]

# ── Core Metrics (existing + I2A expansion) ───────────────────────────────────

CORE_METRICS = [
    "campaign_created_users",
    "total_campaigns_created",
    "total_attributable_revenue",
    "c2_users",
    "RA_Viewed_Users",
    "RA_Owned_Viewed_Users",
    "RA_Supported_Engaged_Users",
    # I2A expansion
    "automation_created_users",
    "total_automations_created",
    "segment_created_users",
    "total_segments_created",
    "actions_taken_users",
    "total_actions",
    "campaign_sent_users",
    "total_campaigns_sent",
]

# ── Analytics Thresholds ──────────────────────────────────────────────────────

ANOMALY_Z_THRESHOLD = 2.0          # Z-score threshold for anomaly detection
ANOMALY_ROLLING_WINDOW = 8         # Weeks for rolling stats
MOVING_AVERAGE_WINDOW = 4          # Weeks for moving average smoothing
TREND_MIN_PERIODS = 3              # Minimum periods to declare a trend
CORRELATION_MIN_PERIODS = 8        # Minimum periods for correlation
SEGMENT_DIFF_THRESHOLD = 1.0       # Std devs for segment difference flagging
EXECUTIVE_SUMMARY_MAX_BULLETS = 8  # Max bullets in executive summary
EXECUTIVE_SUMMARY_MIN_BULLETS = 6  # Min bullets in executive summary

# ── Metric Display Config ─────────────────────────────────────────────────────
# Maps metric keys to human-readable names and formatting hints.

METRIC_DISPLAY = {
    "pct_attributed_rev": {
        "label": "% Attributed Revenue",
        "format": "pct",
        "higher_is_better": True,
    },
    "pct_campaigns_created": {
        "label": "% Created Campaign",
        "format": "pct",
        "higher_is_better": True,
    },
    "campaigns_per_user": {
        "label": "Campaigns per User",
        "format": "ratio",
        "higher_is_better": True,
    },
    "pct_automations_created": {
        "label": "% Created Automation",
        "format": "pct",
        "higher_is_better": True,
    },
    "automations_per_user": {
        "label": "Automations per User",
        "format": "ratio",
        "higher_is_better": True,
    },
    "pct_segments_created": {
        "label": "% Created Segment",
        "format": "pct",
        "higher_is_better": True,
    },
    "segments_per_user": {
        "label": "Segments per User",
        "format": "ratio",
        "higher_is_better": True,
    },
    "pct_actions_taken": {
        "label": "% Took Any Action",
        "format": "pct",
        "higher_is_better": True,
    },
    "pct_ra_owned": {
        "label": "R&A Adoption (Owned)",
        "format": "pct",
        "higher_is_better": True,
    },
    "pct_ra_total": {
        "label": "R&A Adoption (All)",
        "format": "pct",
        "higher_is_better": True,
    },
}

# ── Funnel Stages ─────────────────────────────────────────────────────────────
# Ordered from broadest to most specific for funnel analysis.

FUNNEL_STAGES = [
    {"key": "active_users", "label": "Weekly Active Users (WAU)"},
    {"key": "ra_viewed_users", "label": "Viewed Any R&A Page"},
    {"key": "ra_owned_users", "label": "Viewed Owned R&A Page"},
    {"key": "ra_engaged_users", "label": "Engaged with R&A"},
    {"key": "actions_taken_users", "label": "Took Action (I2A)"},
]

# ── Recommended Actions Templates ─────────────────────────────────────────────
# Used by the executive summary generator to suggest concrete next steps.

# ── Per-Page Funnel Configuration ─────────────────────────────────────────────
# Maps each R&A page to its viewer metric, engaged metric, and ECS filter
# for per-page I2A attribution.

PAGE_FUNNEL_CONFIG = [
    {
        "label": "Marketing Dashboard",
        "viewer_metric": "Marketing_Dashboard_Viewers",
        "engaged_metric": "Marketing_Dashboard_Engaged",
        "ecs_filter": """(
            (scope_area = 'analytics' AND object_detail IN ('marketing_dashboard', 'email_dashboard'))
            OR screen IN ('/analytics/marketing-dashboard','/analytics/marketing-dashboard/',
                          'analytics/marketing-dashboard','analytics/marketing-dashboard/',
                          '/analytics/email-dashboard/','analytics/email-dashboard','/analytics/email-dashboard')
        ) AND screen NOT IN ('/') AND event = 'reporting:viewed'""",
    },
    {
        "label": "Audience Analytics",
        "viewer_metric": "Audience_analytic_viewers",
        "engaged_metric": "Audience_analytic_Engaged",
        "ecs_filter": """(
            (scope_area = 'analytics' AND object_detail = 'audience_analytics')
            OR screen IN ('/analytics/audience-analytics/','/analytics/audience-analytics','analytics/audience-analytics')
        ) AND screen NOT IN ('/') AND event = 'reporting:viewed'""",
    },
    {
        "label": "Conversion Dashboard",
        "viewer_metric": "Conversion_Insights_viewers",
        "engaged_metric": "Conversion_Insights_Engaged",
        "ecs_filter": """(
            (scope_area = 'business_analytics' AND object_detail = 'conversion_insights')
            OR screen LIKE '%analytics/conversion-insights%'
        ) AND screen NOT IN ('/') AND event = 'reporting:viewed'""",
    },
    {
        "label": "Predictive Analytics",
        "viewer_metric": "Conversion_Insights_viewers",
        "engaged_metric": "Conversion_Insights_Engaged",
        "ecs_filter": """(
            scope_area = 'subscription_management' AND object_detail = 'revenue_plans'
        ) AND screen NOT IN ('/') AND event = 'reporting:viewed'""",
    },
    {
        "label": "Email Report",
        "viewer_metric": "Email_Report_Viewers",
        "engaged_metric": "Email_Report_Engaged",
        "ecs_filter": """(
            (initiative_name = 'marketing_crm_analytics' AND scope_area = 'campaign_analytics'
             AND object_detail IN ('email_overview_report','email_activity_bounced_report',
                'email_ecomm_order_history_report','email_activity_sent_report',
                'email_activity_unsubscribed_report','email_social_report',
                'email_activity_opened_report','email_activity_clicked_report',
                'email_ecomm_product_activity_report','email_click_performance_report',
                'email_activity_complained_report','email_activity_not_opened_report'))
            OR screen IN ('/analytics/report/overview','/reports/clicks','/reports/activity/open',
                '/reports/activity/bounced','/reports/activity/unsubscribed','/reports/activity/sent',
                '/reports/activity/clicked','/reports/activity/not-opened','reports/social-stats',
                '/reports/ecommerce/history','/reports/activity/abuse','/reports/ecommerce/activity',
                '/reports/summary','analytics/reports/overview','/reports/summary/',
                '/analytics/reports/click-performance','/analytics/reports/recipient-activity')
        ) AND event = 'reporting:viewed'""",
    },
    {
        "label": "SMS Report",
        "viewer_metric": "SmS_Report_Viewers",
        "engaged_metric": "SmS_Report_Engaged",
        "ecs_filter": """(
            (scope_area = 'analytics' AND object_detail = 'sms_report')
            OR screen IN ('analytics/sms','/analytics/sms')
        ) AND event = 'reporting:viewed'""",
    },
    {
        "label": "Custom Report",
        "viewer_metric": "Custom_Report_Viewers",
        "engaged_metric": "Custom_analytics_Engaged",
        "ecs_filter": """(
            (scope_area = 'analytics' AND object_detail = 'custom_reports')
            OR screen IN ('analytics/custom-reports','/analytics/custom-reports','/analytics/custom-reports/',
                '/analytics/reports/custom-reports/builder','/analytics/reports/custom-reports/',
                'analytics/custom-reports/builder','analytics/custom-reports/graph/',
                '/analytics/custom-reports/builder','/analytics/custom-reports/graph',
                '/analytics/custom-reports/details','/analytics/reports/custom-reports')
        ) AND screen NOT IN ('/') AND event = 'reporting:viewed'""",
    },
    {
        "label": "Automation / Journey Report",
        "viewer_metric": "Journey_Report_Viewers",
        "engaged_metric": "CJB_Report_Engaged",
        "ecs_filter": """(
            (initiative_name = 'core_offerings' AND object_detail IN ('cjb_original_report','cjb_overview_report','customer_journey_builder_report'))
            OR screen IN ('analytics/cjb_report','/customer-journey/report','customer_journey/report','customer-journey/report')
        ) AND event = 'reporting:viewed'""",
    },
    {
        "label": "MVT / Multivariate Report",
        "viewer_metric": None,
        "engaged_metric": None,
        "ecs_filter": None,  # Not tracked in ECS
    },
]


ACTION_TEMPLATES = {
    "declining_engagement": [
        "Increase campaign frequency targeting {segment} users",
        "Launch re-engagement automation flow for {segment}",
        "Optimize send timing based on {segment} activity patterns",
    ],
    "rising_engagement": [
        "Scale successful patterns from {segment} to other segments",
        "Create targeted segmentation for high-performing {segment} cohort",
        "Expand content types that resonate with {segment}",
    ],
    "low_conversion": [
        "Run A/B tests on campaign templates for {segment}",
        "Simplify the campaign creation flow for {metric} improvement",
        "Add in-app prompts after R&A page views to drive action",
    ],
    "high_conversion": [
        "Document and replicate {segment} conversion patterns",
        "Invest in features that support {metric} growth",
    ],
    "anomaly_positive": [
        "Investigate driver behind {metric} spike — replicate if organic",
    ],
    "anomaly_negative": [
        "Investigate root cause of {metric} drop — check data pipeline and product changes",
        "Monitor {metric} closely over next 2 weeks for recovery",
    ],
}
