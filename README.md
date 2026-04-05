# R&A Executive Dashboard — Decision Intelligence System

## What This Is

A two-page executive reporting dashboard for Mailchimp's Reporting & Analytics (R&A) team that transforms raw product data into actionable decision intelligence. It surfaces weekly/monthly KPIs, AI-generated executive summaries, engagement breakdowns, and insight-to-action metrics — all powered by live BigQuery data from Mailchimp's product analytics pipeline.

**Live Dashboard:** Published via GitHub Pages at this repo's URL.

---

## Goal

Evolve the R&A weekly performance review (WPR) from a descriptive reporting tool into a **decision intelligence system** that answers:
- What happened? (KPIs, trends)
- Why did it happen? (anomaly detection, segment analysis)
- What should we do? (recommended actions, opportunities, threats)

---

## What It Shows

### Page 1 — Executive Overview
| Section | Description |
|---------|-------------|
| **Executive Summary** | 6-8 AI-generated bullets: trends, anomalies, opportunities, threats, actions |
| **KPI Grid** | 4 headline metrics with dual WoW + YoY deltas |
| **% Attributed Revenue** | Share of GMV attributed to Mailchimp campaigns (ECU only) |
| **% Created Campaign (7d)** | Users who created a campaign within 7 days of R&A view |
| **Campaigns per User (7d)** | Average campaigns per WAU within 7-day attribution window |
| **R&A Adoption** | Owned pages vs owned + supported CTAs (de-duplicated union) |

### Page 2 — Detailed Analytics
| Section | Description |
|---------|-------------|
| **I2A Breakdown** | Campaign, Automation, Segment creation rates with WoW + YoY |
| **Engagement Drilldown** | Per-page user counts: All / Owned / Supported / Engaged tabs |

### Filters
- **Segment:** All / ECU / Non-ECU
- **Value:** All / HVC / Non-HVC
- **Tenure:** All / New (<90d) / Tenured (>=90d)
- **Granularity:** Weekly / Monthly (default)

---

## How It Was Built

### Architecture
```
BigQuery (live data)
  |
  +-- rpt_RA_L1L3_Test_03_04 (Dataform pre-aggregated) --> /api/metrics
  +-- ecs_users_activities (raw ECS events)             --> /api/adoption
  +-- emails_bulk + workflows + segments (raw)          --> /api/i2a (7-day window)
  +-- L2/L3 engagement metrics                          --> /api/engagement
  |
  v
Flask server (server.py) --> analytics.py (trend/anomaly/funnel/summary engine)
  |
  v
Single-page HTML (Chart.js) --> Two-page tab layout
```

### Data Pipeline
1. **Dataform SQL** (5 views by Data Science): Viewed, Engaged, Insight-to-Action, Combined, Report
2. **Flask API** (5 endpoints): Metrics, Adoption, I2A, Engagement, Executive Summary
3. **Analytics Engine** (Python): Moving averages, z-score anomaly detection, Pearson correlations, funnel analysis, segment comparison, rule-based executive summary generation
4. **Frontend** (single HTML): Chart.js line charts, dual WoW+YoY comparison, category-filtered executive summary

### Key Design Decisions
- **7-day I2A attribution window** instead of 1-day — analysis showed +6.6pp lift, zero gain beyond 7 days
- **Both WoW AND YoY shown simultaneously** — no toggle, executives see both comparisons at once
- **Adoption de-duplication** — uses OR (union) not + (addition) to avoid double-counting users in both owned and supported
- **Monthly default** — executives think in months, not weeks
- **Black/grey color scheme** — clean, professional, no distracting colors

---

## Components

### Source Files
| File | Purpose |
|------|---------|
| `server.py` | Flask backend — 5 API endpoints, BigQuery queries, caching, logging |
| `analytics.py` | Analytics engine — trends, anomalies, correlations, funnel, executive summary |
| `config.py` | Engagement taxonomy, metric display config, thresholds, action templates |
| `static/index.html` | Full frontend — two-page layout, Chart.js, dual comparison rendering |
| `qa_agent.py` | Companion QA agent — 27 automated quality checks |
| `qa_checks/` | 6 check modules: schema, API, rates, consistency, analytics, frontend |

### Test Suite (222 tests)
| File | Tests | Coverage |
|------|-------|----------|
| `test_analytics.py` | 53 | Trends, anomalies, segments, funnel, correlations, summary, edge cases |
| `test_comparison.py` | 13 | WoW/YoY delta computation |
| `test_engagement.py` | 19 | Taxonomy structure, BigQuery field matching |
| `test_config_validation.py` | 15 | Config completeness, cross-referencing |
| `test_server_endpoints.py` | 25 | Flask endpoint tests (mocked BigQuery) |
| `test_data_quality.py` | 20 | SQL correctness, rate formulas, data integrity |
| `test_frontend.py` | 40 | HTML structure, JS logic, design system compliance |

---

## Quality Checks

### QA Agent (`qa_agent.py`)
Run automated quality validation:
```bash
python3 qa_agent.py --mode offline     # Code/config checks (no server needed)
python3 qa_agent.py --mode full        # All 27 checks (server must be running)
python3 qa_agent.py --check schema,rates --json  # Specific categories
```

### What the QA Agent Validates
- **Schema:** Config metric names match BigQuery UNPIVOT exactly, drift detection
- **API:** Response shapes, column presence, sorting, timing, adoption de-dup
- **Rates:** Bounds (0-100%), numerator <= denominator, formula verification
- **Consistency:** ECU+NonECU = All, HVC+NonHVC = All, cross-endpoint alignment
- **Analytics:** Trend direction vs actual data, z-score recomputation, funnel monotonicity
- **Frontend:** HTML structure, filter elements, JS logic, design system tokens

### Reliability Hardening
- **Input validation** — whitelist filter values before SQL interpolation
- **Error handling** — all BigQuery calls wrapped in try/except with structured logging
- **Data freshness** — `extracted_at` timestamp + staleness warning at 12h
- **Double-counting guard** — `category = 'L1'` filter on metrics query
- **Fetch timeout** — 30s AbortController on all frontend API calls

---

## Running Locally

### Prerequisites
- Python 3.9+
- `pip3 install flask google-cloud-bigquery requests`
- `gcloud auth application-default login`

### Start
```bash
cd ra-executive-dashboard
python3 server.py
# Open http://localhost:5050
```

### Run Tests
```bash
python3 -m pytest tests/ -v
```

---

## Data Sources

| Source | Table | Used By |
|--------|-------|---------|
| Dataform report | `mc-analytics-devel.bi_product.rpt_RA_L1L3_Test_03_04` | /api/metrics, /api/engagement |
| ECS events | `mc-business-intelligence.bi_activities.ecs_users_activities` | /api/adoption |
| User dimensions | `mc-analytics-devel.bi_product.user_dimensions_weekly_rollup` | /api/adoption, /api/i2a (tenure) |
| Email campaigns | `mc-business-intelligence.bi_reporting.emails_bulk` | /api/i2a |
| Automations | `mc-business-intelligence.bi_reporting.customer_journey_workflows` | /api/i2a |
| Segments | `mc-business-intelligence.bi_reporting.tags_segments_daily_rollup` | /api/i2a |
