"""
Analytics engine for R&A Executive Dashboard.
Computes trends, anomalies, correlations, funnel analysis, and executive summaries.
Pure Python — no BigQuery dependency. Operates on data dicts returned by API queries.
"""

import math
import random
from config import (
    ANOMALY_Z_THRESHOLD,
    ANOMALY_ROLLING_WINDOW,
    MOVING_AVERAGE_WINDOW,
    TREND_MIN_PERIODS,
    CORRELATION_MIN_PERIODS,
    SEGMENT_DIFF_THRESHOLD,
    EXECUTIVE_SUMMARY_MAX_BULLETS,
    EXECUTIVE_SUMMARY_MIN_BULLETS,
    METRIC_DISPLAY,
    ACTION_TEMPLATES,
    FUNNEL_STAGES,
)


# ── Utility ───────────────────────────────────────────────────────────────────

def _safe_div(a, b):
    """Safe division returning None when denominator is 0 or None."""
    if b is None or b == 0 or a is None:
        return None
    return a / b


def _pct_change(cur, prev):
    """Percentage change from prev to cur. Returns None if prev is 0/None."""
    if prev is None or prev == 0 or cur is None:
        return None
    return ((cur - prev) / abs(prev)) * 100


def _mean(values):
    """Mean of non-None values."""
    clean = [v for v in values if v is not None]
    return sum(clean) / len(clean) if clean else None


def _stddev(values):
    """Population standard deviation of non-None values."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    m = sum(clean) / len(clean)
    variance = sum((x - m) ** 2 for x in clean) / len(clean)
    return math.sqrt(variance) if variance > 0 else 0.0


def _pearson_r(xs, ys):
    """Pearson correlation coefficient between two lists (aligned, same length)."""
    pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    n = len(pairs)
    if n < CORRELATION_MIN_PERIODS:
        return None
    mx = sum(p[0] for p in pairs) / n
    my = sum(p[1] for p in pairs) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pairs)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pairs))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pairs))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


# ── Trend Analysis ────────────────────────────────────────────────────────────

def compute_moving_average(values, window=MOVING_AVERAGE_WINDOW):
    """Compute moving average over a list of values (None-safe)."""
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        window_vals = [v for v in values[start:i + 1] if v is not None]
        result.append(sum(window_vals) / len(window_vals) if window_vals else None)
    return result


def compute_growth_rate(values, periods=4):
    """Compound growth rate over the last N periods."""
    clean = [v for v in values[-periods:] if v is not None and v > 0]
    if len(clean) < 2:
        return None
    return ((clean[-1] / clean[0]) ** (1 / (len(clean) - 1)) - 1) * 100


def compute_trends(data, metric_keys):
    """
    Compute trend analysis for each metric across time series data.

    Args:
        data: list of dicts with period_start and metric values
        metric_keys: list of metric key names to analyze

    Returns:
        dict of metric_key -> {
            "current", "previous", "wow_delta", "yoy_delta",
            "moving_avg", "growth_rate", "direction", "streak"
        }
    """
    if not data or len(data) < 2:
        return {}

    results = {}
    for key in metric_keys:
        values = [d.get(key) for d in data]
        clean_values = [v for v in values if v is not None]
        if not clean_values:
            continue

        current = values[-1]
        previous = values[-2] if len(values) >= 2 else None

        # WoW delta (from pw_ columns if available, else adjacent values)
        pw_key = f"pw_{key}"
        pw_val = data[-1].get(pw_key)
        wow_delta = _pct_change(current, pw_val) if pw_val is not None else _pct_change(current, previous)

        # YoY delta (from py_ columns)
        py_key = f"py_{key}"
        py_val = data[-1].get(py_key)
        yoy_delta = _pct_change(current, py_val)

        # Moving average
        ma = compute_moving_average(values)

        # Growth rate
        growth = compute_growth_rate(values)

        # Direction and streak
        streak = 0
        if len(values) >= 2:
            direction = "up" if values[-1] is not None and previous is not None and values[-1] > previous else "down"
            for i in range(len(values) - 1, 0, -1):
                if values[i] is not None and values[i - 1] is not None:
                    if (direction == "up" and values[i] > values[i - 1]) or \
                       (direction == "down" and values[i] < values[i - 1]):
                        streak += 1
                    else:
                        break
                else:
                    break
        else:
            direction = "flat"

        results[key] = {
            "current": current,
            "previous": previous,
            "wow_delta": wow_delta,
            "yoy_delta": yoy_delta,
            "moving_avg": ma[-1] if ma else None,
            "growth_rate_4w": growth,
            "direction": direction if streak >= TREND_MIN_PERIODS else "flat",
            "streak": streak,
        }

    return results


# ── Anomaly Detection ─────────────────────────────────────────────────────────

def detect_anomalies(data, metric_keys, threshold=ANOMALY_Z_THRESHOLD,
                     window=ANOMALY_ROLLING_WINDOW):
    """
    Detect anomalies using z-score over a rolling window.

    Returns:
        list of {
            "metric", "period", "value", "z_score",
            "direction" ("spike"/"drop"), "severity" ("warning"/"critical")
        }
    """
    anomalies = []
    if not data or len(data) < window:
        return anomalies

    for key in metric_keys:
        values = [d.get(key) for d in data]
        for i in range(window, len(values)):
            current = values[i]
            if current is None:
                continue

            window_vals = [v for v in values[max(0, i - window):i] if v is not None]
            if len(window_vals) < window // 2:
                continue

            m = _mean(window_vals)
            s = _stddev(window_vals)
            if m is None or s is None or s == 0:
                continue

            z = (current - m) / s
            if abs(z) >= threshold:
                anomalies.append({
                    "metric": key,
                    "metric_label": METRIC_DISPLAY.get(key, {}).get("label", key),
                    "period": data[i].get("period_start", ""),
                    "value": current,
                    "mean": round(m, 2),
                    "z_score": round(z, 2),
                    "direction": "spike" if z > 0 else "drop",
                    "severity": "critical" if abs(z) >= threshold * 1.5 else "warning",
                })

    # Sort by absolute z-score descending (most extreme first)
    anomalies.sort(key=lambda a: abs(a["z_score"]), reverse=True)
    return anomalies


# ── Segmentation Analysis ─────────────────────────────────────────────────────

def analyze_segments(segment_data, metric_keys):
    """
    Compare metrics across segments (e.g., ECU vs Non-ECU, HVC vs Non-HVC).

    Args:
        segment_data: dict of segment_name -> list of period dicts
        metric_keys: metrics to compare

    Returns:
        list of {
            "metric", "segment_a", "segment_b",
            "value_a", "value_b", "diff", "pct_diff", "significant"
        }
    """
    results = []
    segment_names = list(segment_data.keys())
    if len(segment_names) < 2:
        return results

    for i in range(len(segment_names)):
        for j in range(i + 1, len(segment_names)):
            seg_a, seg_b = segment_names[i], segment_names[j]
            data_a, data_b = segment_data[seg_a], segment_data[seg_b]
            if not data_a or not data_b:
                continue

            for key in metric_keys:
                vals_a = [d.get(key) for d in data_a if d.get(key) is not None]
                vals_b = [d.get(key) for d in data_b if d.get(key) is not None]

                if not vals_a or not vals_b:
                    continue

                mean_a = _mean(vals_a)
                mean_b = _mean(vals_b)
                if mean_a is None or mean_b is None:
                    continue

                diff = mean_a - mean_b
                pooled_std = _stddev(vals_a + vals_b)
                significant = (
                    pooled_std is not None
                    and pooled_std > 0
                    and abs(diff) / pooled_std >= SEGMENT_DIFF_THRESHOLD
                )

                results.append({
                    "metric": key,
                    "metric_label": METRIC_DISPLAY.get(key, {}).get("label", key),
                    "segment_a": seg_a,
                    "segment_b": seg_b,
                    "value_a": round(mean_a, 2),
                    "value_b": round(mean_b, 2),
                    "diff": round(diff, 2),
                    "pct_diff": round(_pct_change(mean_a, mean_b) or 0, 1),
                    "significant": significant,
                })

    return results


# ── Funnel Analysis ───────────────────────────────────────────────────────────

def compute_funnel(data):
    """
    Compute funnel drop-off rates between stages.

    Args:
        data: latest period dict with stage metric values

    Returns:
        list of {
            "stage", "label", "value", "rate", "dropoff_rate", "is_bottleneck"
        }
    """
    if not data:
        return []

    stages = []
    prev_value = None
    max_dropoff = 0
    max_dropoff_idx = -1

    for idx, stage_def in enumerate(FUNNEL_STAGES):
        value = data.get(stage_def["key"])
        if value is None:
            value = 0

        rate = _safe_div(value, data.get(FUNNEL_STAGES[0]["key"], 1))
        dropoff = _safe_div(prev_value - value, prev_value) if prev_value and prev_value > 0 else 0

        if dropoff and dropoff > max_dropoff and idx > 0:
            max_dropoff = dropoff
            max_dropoff_idx = idx

        stages.append({
            "stage": stage_def["key"],
            "label": stage_def["label"],
            "value": value,
            "rate": round((rate or 0) * 100, 1),
            "dropoff_rate": round((dropoff or 0) * 100, 1),
            "is_bottleneck": False,
        })
        prev_value = value

    if max_dropoff_idx >= 0:
        stages[max_dropoff_idx]["is_bottleneck"] = True

    return stages


# ── Correlation / Driver Analysis ─────────────────────────────────────────────

def compute_correlations(data, engagement_keys, outcome_keys):
    """
    Compute Pearson correlation between engagement metrics and outcome metrics.

    Returns:
        list of {"engagement", "outcome", "r", "strength"} sorted by |r| desc
    """
    results = []
    for eng_key in engagement_keys:
        eng_vals = [d.get(eng_key) for d in data]
        for out_key in outcome_keys:
            out_vals = [d.get(out_key) for d in data]
            r = _pearson_r(eng_vals, out_vals)
            if r is not None:
                strength = (
                    "strong" if abs(r) >= 0.7
                    else "moderate" if abs(r) >= 0.4
                    else "weak"
                )
                results.append({
                    "engagement": eng_key,
                    "engagement_label": METRIC_DISPLAY.get(eng_key, {}).get("label", eng_key),
                    "outcome": out_key,
                    "outcome_label": METRIC_DISPLAY.get(out_key, {}).get("label", out_key),
                    "r": round(r, 3),
                    "strength": strength,
                })

    results.sort(key=lambda x: abs(x["r"]), reverse=True)
    return results


# ── Ranking / Prioritization ─────────────────────────────────────────────────

def rank_items(items, score_fn):
    """Rank a list of items by a scoring function. Higher score = higher priority."""
    scored = [(item, score_fn(item)) for item in items]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [item for item, _ in scored]


# ── Executive Summary Generation ──────────────────────────────────────────────

def generate_executive_summary(
    trends, anomalies, segments, funnel, correlations, comparison_mode="wow"
):
    """
    Generate a concise executive summary with 6-8 bullet points.

    Args:
        trends: dict from compute_trends()
        anomalies: list from detect_anomalies()
        segments: list from analyze_segments()
        funnel: list from compute_funnel()
        correlations: list from compute_correlations()
        comparison_mode: "wow" or "yoy"

    Returns:
        list of {"category", "severity", "text", "metric", "score"}
    """
    bullets = []
    delta_label = "WoW" if comparison_mode == "wow" else "YoY"
    delta_key = "wow_delta" if comparison_mode == "wow" else "yoy_delta"

    # ── Trends ────────────────────────────────────────────────────────────
    for key, t in trends.items():
        display = METRIC_DISPLAY.get(key, {})
        label = display.get("label", key)
        higher_better = display.get("higher_is_better", True)
        delta_val = t.get(delta_key)

        if delta_val is None:
            continue

        if t["streak"] >= TREND_MIN_PERIODS:
            direction_word = "improving" if (delta_val > 0) == higher_better else "declining"
            severity = "positive" if direction_word == "improving" else "negative"
            score = abs(delta_val) * t["streak"]
            bullets.append({
                "category": "trend",
                "severity": severity,
                "text": f"{label} has been {direction_word} for {t['streak']} consecutive weeks "
                        f"({'+' if delta_val > 0 else ''}{delta_val:.1f}% {delta_label})",
                "metric": key,
                "score": score,
            })

    # ── Anomalies ─────────────────────────────────────────────────────────
    for a in anomalies[:3]:  # Top 3 by z-score
        display = METRIC_DISPLAY.get(a["metric"], {})
        label = a.get("metric_label", a["metric"])
        higher_better = display.get("higher_is_better", True)
        is_good = (a["direction"] == "spike") == higher_better

        severity = "positive" if is_good else "warning"
        category = "opportunity" if is_good else "anomaly"
        score = abs(a["z_score"]) * 10

        bullets.append({
            "category": category,
            "severity": severity,
            "text": f"{label} showed an unusual {a['direction']} "
                    f"(z={a['z_score']:+.1f}, value={a['value']:.1f} vs mean={a['mean']:.1f})",
            "metric": a["metric"],
            "score": score,
        })

    # ── Segment Insights ──────────────────────────────────────────────────
    sig_segments = [s for s in segments if s["significant"]]
    for s in sig_segments[:2]:  # Top 2 significant differences
        label = s.get("metric_label", s["metric"])
        higher_seg = s["segment_a"] if s["diff"] > 0 else s["segment_b"]
        lower_seg = s["segment_b"] if s["diff"] > 0 else s["segment_a"]
        diff_pct = abs(s["pct_diff"])

        bullets.append({
            "category": "opportunity",
            "severity": "positive",
            "text": f"{higher_seg} outperforms {lower_seg} on {label} "
                    f"by {diff_pct:.0f}% — investigate and replicate success patterns",
            "metric": s["metric"],
            "score": diff_pct,
        })

    # ── Funnel Bottleneck ─────────────────────────────────────────────────
    bottleneck = next((s for s in funnel if s["is_bottleneck"]), None)
    if bottleneck and bottleneck["dropoff_rate"] > 20:
        bullets.append({
            "category": "threat",
            "severity": "warning",
            "text": f"Funnel bottleneck at '{bottleneck['label']}' — "
                    f"{bottleneck['dropoff_rate']:.0f}% drop-off rate. "
                    f"Improving this stage has highest potential impact",
            "metric": bottleneck["stage"],
            "score": bottleneck["dropoff_rate"],
        })

    # ── Correlation-Driven Opportunities ──────────────────────────────────
    strong_corr = [c for c in correlations if c["strength"] == "strong"]
    for c in strong_corr[:1]:  # Top 1
        bullets.append({
            "category": "opportunity",
            "severity": "positive",
            "text": f"Strong correlation (r={c['r']:.2f}) between "
                    f"{c.get('engagement_label', c['engagement'])} and "
                    f"{c.get('outcome_label', c['outcome'])} — "
                    f"increasing engagement on this asset may drive conversions",
            "metric": c["engagement"],
            "score": abs(c["r"]) * 20,
        })

    # ── Recommended Actions ───────────────────────────────────────────────
    # Generate actions based on top threats and opportunities
    threat_bullets = [b for b in bullets if b["severity"] in ("warning", "negative")]
    opp_bullets = [b for b in bullets if b["category"] == "opportunity"]

    if threat_bullets:
        top_threat = max(threat_bullets, key=lambda b: b["score"])
        templates = ACTION_TEMPLATES.get("declining_engagement", [])
        if templates:
            action_text = templates[0].format(
                segment="affected", metric=top_threat["metric"]
            )
            bullets.append({
                "category": "action",
                "severity": "info",
                "text": f"Recommended: {action_text}",
                "metric": top_threat["metric"],
                "score": top_threat["score"] * 0.9,
            })

    if opp_bullets:
        top_opp = max(opp_bullets, key=lambda b: b["score"])
        templates = ACTION_TEMPLATES.get("rising_engagement", [])
        if templates:
            action_text = templates[0].format(
                segment="high-performing", metric=top_opp["metric"]
            )
            bullets.append({
                "category": "action",
                "severity": "info",
                "text": f"Recommended: {action_text}",
                "metric": top_opp["metric"],
                "score": top_opp["score"] * 0.8,
            })

    # ── Sort by score, trim to 6-8 ────────────────────────────────────────
    bullets.sort(key=lambda b: b["score"], reverse=True)

    # Ensure category diversity: at least 1 from each non-empty category
    categories_seen = set()
    final = []
    remaining = []

    for b in bullets:
        if b["category"] not in categories_seen and len(final) < EXECUTIVE_SUMMARY_MAX_BULLETS:
            final.append(b)
            categories_seen.add(b["category"])
        else:
            remaining.append(b)

    # Fill up to max with highest-scored remaining
    for b in remaining:
        if len(final) >= EXECUTIVE_SUMMARY_MAX_BULLETS:
            break
        final.append(b)

    # Ensure minimum
    while len(final) < EXECUTIVE_SUMMARY_MIN_BULLETS and remaining:
        final.append(remaining.pop(0))

    final.sort(key=lambda b: b["score"], reverse=True)

    # Remove score from output (internal ranking only)
    for b in final:
        b.pop("score", None)

    return final
