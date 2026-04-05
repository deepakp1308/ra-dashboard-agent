"""
Schema checks — validate config metric names against BigQuery UNPIVOT output
and detect drift (new metrics in BQ not in config).
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import CORE_METRICS, ALL_ENGAGEMENT_METRICS, ENGAGEMENT_TAXONOMY

# BigQuery UNPIVOT metric names (authoritative list from rpt_RA_L1L3_Test_03_04.sqlx)
BQ_UNPIVOT_METRICS = {
    "RA_Supported_Viewed_Users", "RA_Owned_Viewed_Users",
    "Homepage_Viewers", "Automation_Overview_Viewers", "All_Campaigns_Viewers",
    "All_Journeys_Viewers", "All_Contacts_Viewers", "Audience_Dashboard_Viewers",
    "Audience_Segment_Viewers", "SMS_Overview_Viewers",
    "Marketing_Dashboard_Viewers", "Audience_analytic_viewers",
    "Conversion_Insights_viewers", "Custom_Report_Viewers", "Reports_Viewers",
    "SmS_Report_Viewers", "Email_Report_Viewers", "Journey_Report_Viewers",
    "Replicated_Viewers",
    "RA_Engaged_Users", "RA_Supported_Engaged_Users", "RA_Owned_Engaged_Users",
    "Homepage_Engaged", "All_campaign_Engaged", "Automation_overview_Engaged_users",
    "All_journeys_Engaged", "All_contacts_Engaged", "Audience_dashboard_Engaged",
    "Audience_Segment_Engaged", "Sms_overview_Engaged",
    "Marketing_Dashboard_Engaged", "Audience_analytic_Engaged",
    "Conversion_Insights_Engaged", "Custom_analytics_Engaged", "Report_Engaged",
    "SmS_Report_Engaged", "Email_Report_Engaged", "CJB_Report_Engaged",
    "RA_Viewed_Users", "actions_taken_users", "total_campaigns_created",
    "total_campaigns_sent", "total_automations_created", "total_segments_created",
    "total_actions", "campaign_created_users", "campaign_sent_users",
    "automation_created_users", "segment_created_users",
    "c2_users", "total_attributable_revenue",
    "total_c2s_collected_from_form_or_integrations",
    "c1s_collecting_c2s_from_form_or_integrations",
    "total_attributable_orders", "total_attributable_aov",
    "ecomm_integration_users", "one_plus_ecomm_integration_users",
    "ecomm_users_engaging_with_RA",
}


def check_core_metrics_exist():
    """Validate all CORE_METRICS exist in BigQuery UNPIVOT output."""
    missing = set(CORE_METRICS) - BQ_UNPIVOT_METRICS
    if missing:
        return {"status": "FAIL", "details": f"Missing from BQ: {missing}"}
    return {"status": "PASS", "details": f"{len(CORE_METRICS)}/{len(CORE_METRICS)} metrics found"}


def check_engagement_metrics_exist():
    """Validate all ENGAGEMENT_TAXONOMY metrics exist in BigQuery UNPIVOT."""
    all_tax = set()
    for group in ENGAGEMENT_TAXONOMY.values():
        all_tax.update(group.values())
    missing = all_tax - BQ_UNPIVOT_METRICS
    if missing:
        return {"status": "FAIL", "details": f"Missing from BQ: {missing}"}
    return {"status": "PASS", "details": f"{len(all_tax)}/{len(all_tax)} engagement metrics found"}


def check_drift_detection():
    """Detect metrics in BigQuery not referenced in any config."""
    all_configured = set(CORE_METRICS) | set(ALL_ENGAGEMENT_METRICS)
    # Add combined group
    for group in ENGAGEMENT_TAXONOMY.values():
        all_configured.update(group.values())
    unconfigured = BQ_UNPIVOT_METRICS - all_configured
    if unconfigured:
        return {"status": "WARN", "details": f"{len(unconfigured)} BQ metrics not in config: {sorted(unconfigured)[:5]}..."}
    return {"status": "PASS", "details": "All BQ metrics are configured"}


def check_no_orphaned_config_metrics():
    """Detect metrics in config that don't exist in BigQuery."""
    all_configured = set(CORE_METRICS) | set(ALL_ENGAGEMENT_METRICS)
    orphaned = all_configured - BQ_UNPIVOT_METRICS
    if orphaned:
        return {"status": "FAIL", "details": f"Config metrics not in BQ: {orphaned}"}
    return {"status": "PASS", "details": "No orphaned config metrics"}


def run_all():
    return {
        "schema.core_metrics_exist": check_core_metrics_exist(),
        "schema.engagement_metrics_exist": check_engagement_metrics_exist(),
        "schema.drift_detection": check_drift_detection(),
        "schema.no_orphaned_metrics": check_no_orphaned_config_metrics(),
    }
