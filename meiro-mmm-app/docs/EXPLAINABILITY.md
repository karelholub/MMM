# Explainability Guide: Enhanced Attribution & MMM Insights

This guide covers the expanded explainability capabilities including per-channel/campaign driver analyses, feature importance extraction, and richer narrative explanations tied to configuration and data changes.

---

## Overview

The enhanced explainability system provides:

- **Driver Analysis**: What drove changes in metrics between periods
- **Feature Importance**: Which channels matter most in MMM models
- **Config Impact**: How configuration changes affected results
- **Data Quality Impact**: How data health influences confidence
- **Campaign Explanations**: Detailed performance breakdowns
- **Narrative Generation**: Human-readable insights combining all factors

---

## Core Concepts

### Driver Analysis

**Drivers** are entities (channels, campaigns, etc.) that contribute most to metric changes.

**Key metrics:**
- **Delta**: Absolute change in metric
- **% Change**: Relative change
- **Contribution**: Share of total change

**Example:**
```
Conversions increased by 150 (15% change) from 1000 to 1150.
The largest contributor was "paid_social" with an increase of 80 (53% of total change).
The biggest % change was "affiliate" which increased by 200%.
```

### Feature Importance

**Feature importance** quantifies which channels drive MMM model outcomes.

**Metrics:**
- **Contribution Share**: % of total modeled effect
- **Elasticity**: Spend sensitivity (how much KPI changes per % spend change)
- **ROI Ranking**: Channels ordered by return

**Interpretation:**
- High contribution + high elasticity = strategic lever
- High contribution + low elasticity = saturated channel
- Low contribution + high elasticity = growth opportunity

### Confidence Scoring

**Confidence** reflects data quality and affects interpretation reliability.

| Score | Label | Interpretation |
|-------|-------|----------------|
| 85-100 | High | Results are reliable for decision-making |
| 70-84 | Medium | Results are directionally accurate |
| 50-69 | Low | Interpret with caution |
| 0-49 | Very Low | Results may be unreliable |

---

## API Reference

### Compute Metric Drivers

```http
POST /api/explainability/drivers
Content-Type: application/json

{
  "current_data": {
    "paid_search": 450,
    "paid_social": 320,
    "email": 180
  },
  "previous_data": {
    "paid_search": 420,
    "paid_social": 280,
    "email": 200
  },
  "metric_name": "conversions",
  "top_n": 5
}
```

**Response:**
```json
{
  "metric": "conversions",
  "delta": 50,
  "current_value": 950,
  "previous_value": 900,
  "pct_change": 5.56,
  "top_contributors": [
    {
      "id": "paid_social",
      "delta": 40,
      "current_value": 320,
      "previous_value": 280,
      "pct_change": 14.29,
      "contribution_to_total_change": 0.80
    },
    {
      "id": "paid_search",
      "delta": 30,
      "current_value": 450,
      "previous_value": 420,
      "pct_change": 7.14,
      "contribution_to_total_change": 0.60
    }
  ],
  "top_movers": [...],
  "narrative": "Conversions increased by 50.0 (5.6% change) from 900.0 to 950.0. The largest contributor was paid_social with an increase of 40.0 (80.0% of total change)."
}
```

### Get MMM Feature Importance

```http
GET /api/explainability/mmm-importance/{run_id}
```

**Response:**
```json
{
  "channel_importance": [
    {
      "channel": "paid_search",
      "contribution": 4500,
      "share": 45.0,
      "rank": 1
    },
    {
      "channel": "paid_social",
      "contribution": 3200,
      "share": 32.0,
      "rank": 2
    }
  ],
  "elasticities": {
    "paid_search": 0.75,
    "paid_social": 0.62,
    "email": 0.45
  },
  "roi_ranking": [
    {"channel": "email", "roi": 3.5},
    {"channel": "paid_search", "roi": 2.8},
    {"channel": "paid_social", "roi": 2.1}
  ],
  "engine": "bayesian",
  "narrative": "The most important channel is paid_search contributing 45.0% of total modeled effect. The top 3 channels (paid_search, paid_social, email) account for 90.0% of total contribution. The highest ROI is email at 3.50. Channels with high spend elasticity (>0.5): paid_search, paid_social. These are highly sensitive to budget changes."
}
```

### Get Config Change Impact

```http
GET /api/explainability/config-impact/{config_id}?lookback_days=30
```

**Response:**
```json
{
  "config_id": "config_v2",
  "config_version": 2,
  "status": "active",
  "recent_changes": [
    {
      "at": "2026-02-01T10:00:00Z",
      "actor": "admin@example.com",
      "action": "update",
      "diff": {
        "windows": {
          "click_lookback_days": [30, 45]
        }
      }
    }
  ],
  "windows": {
    "click_lookback_days": 45,
    "impression_lookback_days": 7,
    "session_timeout_minutes": 30
  },
  "narrative": "Configuration 'config_v2' version 2 is currently active. There have been 1 changes in the last 30 days. Config was activated on 2026-02-01 by admin@example.com. Attribution time windows were modified, which affects which touchpoints are eligible."
}
```

### Get Data Quality Impact

```http
GET /api/explainability/data-quality/{scope}?scope_id={id}
```

**Response:**
```json
{
  "confidence_score": 87,
  "confidence_label": "High",
  "components": {
    "match_rate": 0.95,
    "join_rate": 0.92,
    "dedup_rate": 0.98,
    "freshness_lag_minutes": 120,
    "missing_rate": 0.05
  },
  "issues": [],
  "narrative": "Data quality confidence is High (87/100). Data quality is good across all measured dimensions. High confidence - results are reliable for decision-making."
}
```

### Explain Campaign Performance

```http
POST /api/explainability/campaign/{campaign_id}
Content-Type: application/json

{
  "attribution_result": {
    "total_conversions": 1000,
    "total_value": 50000,
    "channels": [
      {
        "channel": "brand_q1_2026",
        "conversions": 150,
        "value": 7500
      }
    ]
  },
  "spend_data": {
    "brand_q1_2026": 3000
  }
}
```

**Response:**
```json
{
  "campaign_id": "brand_q1_2026",
  "attribution_metrics": {
    "conversions": 150,
    "value": 7500,
    "share": 15.0
  },
  "efficiency_metrics": {
    "cpa": 20.0,
    "roi": 1.5,
    "roas": 2.5
  },
  "vs_average": {
    "conversions_vs_avg": 50.0
  },
  "recommendations": [
    "Consider increasing budget for this high-performing campaign."
  ],
  "narrative": "Campaign 'brand_q1_2026' generated 150 conversions (15.0% of total) with $7500.00 in attributed value. This is 50% above average, indicating strong performance. ROI is strong at 1.50 (150% return)."
}
```

### Generate Narrative

```http
GET /api/explainability/narrative?scope=channel&config_id=config_v2&from_date=2026-01-01&to_date=2026-01-31
```

**Response:**
```json
{
  "period": {
    "current": {"from": "2026-01-01", "to": "2026-01-31"},
    "previous": {"from": "2025-12-01", "to": "2025-12-31"}
  },
  "drivers": [...],
  "data_health": {...},
  "config": {...},
  "feature_importance": {...},
  "narrative": [
    "Conversions increased by 150.0 (15% change) from 1000 to 1150.",
    "Data quality confidence is High (87/100).",
    "Configuration 'config_v2' version 2 is currently active."
  ]
}
```

---

## Python API

### Compute Drivers

```python
from app.services_explainability import compute_metric_drivers

current = {"paid_search": 450, "paid_social": 320}
previous = {"paid_search": 420, "paid_social": 280}

result = compute_metric_drivers(current, previous, "conversions")

print(f"Delta: {result['delta']}")
print(f"Narrative: {result['narrative']}")
print(f"Top contributor: {result['top_contributors'][0]['id']}")
```

### Extract Feature Importance

```python
from app.services_explainability import extract_mmm_feature_importance

mmm_result = {
    "contrib": [
        {"channel": "paid_search", "mean_contribution": 4500, "elasticity": 0.75},
        {"channel": "paid_social", "mean_contribution": 3200, "elasticity": 0.62},
    ],
    "roi": [
        {"channel": "paid_search", "roi": 2.8},
        {"channel": "paid_social", "roi": 2.1},
    ],
    "engine": "bayesian",
}

importance = extract_mmm_feature_importance(mmm_result)

print(f"Top channel: {importance['channel_importance'][0]['channel']}")
print(f"Elasticities: {importance['elasticities']}")
print(f"Narrative: {importance['narrative']}")
```

### Analyze Config Impact

```python
from app.services_explainability import analyze_config_impact

impact = analyze_config_impact(db, config_id="config_v2", lookback_days=30)

print(f"Version: {impact['config_version']}")
print(f"Recent changes: {len(impact['recent_changes'])}")
print(f"Narrative: {impact['narrative']}")
```

### Analyze Data Quality

```python
from app.services_explainability import analyze_data_quality_impact

dq = analyze_data_quality_impact(db, scope="channel", scope_id="paid_social")

print(f"Confidence: {dq['confidence_label']} ({dq['confidence_score']}/100)")
print(f"Issues: {dq['issues']}")
print(f"Narrative: {dq['narrative']}")
```

### Explain Campaign

```python
from app.services_explainability import explain_campaign_performance

attribution_result = {
    "total_conversions": 1000,
    "total_value": 50000,
    "channels": [
        {"channel": "brand_q1", "conversions": 150, "value": 7500}
    ],
}

spend_data = {"brand_q1": 3000}

explanation = explain_campaign_performance(
    db=db,
    campaign_id="brand_q1",
    attribution_result=attribution_result,
    spend_data=spend_data,
)

print(f"Narrative: {explanation['narrative']}")
print(f"Recommendations: {explanation['recommendations']}")
```

---

## Use Cases

### 1. Performance Review Dashboard

```python
# Explain month-over-month changes
current_month = {"paid_search": 4500, "paid_social": 3200, "email": 1500}
previous_month = {"paid_search": 4200, "paid_social": 2800, "email": 1600}

drivers = compute_metric_drivers(current_month, previous_month, "conversions")

print("### Monthly Performance Review\n")
print(drivers['narrative'])
print("\n### Top Contributors:")
for c in drivers['top_contributors'][:3]:
    print(f"- {c['id']}: {c['delta']:+.0f} ({c['pct_change']:+.1f}%)")
```

### 2. Budget Planning

```python
# Extract feature importance from MMM
importance = extract_mmm_feature_importance(mmm_result)

print("### Budget Allocation Insights\n")
print(importance['narrative'])
print("\n### Channel Priorities:")
for ch in importance['channel_importance'][:3]:
    elasticity = importance['elasticities'].get(ch['channel'], 0)
    print(f"{ch['rank']}. {ch['channel']}: {ch['share']:.1f}% contribution, {elasticity:.2f} elasticity")

# High elasticity = budget-sensitive
high_elasticity = [
    (ch, elas) for ch, elas in importance['elasticities'].items()
    if elas > 0.5
]
if high_elasticity:
    print("\n⚠️  Budget-sensitive channels (test incremental spend):")
    for ch, elas in high_elasticity:
        print(f"  - {ch}: {elas:.2f}")
```

### 3. Troubleshooting Low Performance

```python
# Check data quality for underperforming channel
dq = analyze_data_quality_impact(db, scope="channel", scope_id="display")

if dq['confidence_score'] < 70:
    print(f"⚠️  Low confidence ({dq['confidence_score']}/100)")
    print("Issues:")
    for issue in dq['issues']:
        print(f"  - {issue}")
    print("\nRecommendation: Fix data quality issues before optimizing spend.")
else:
    print("✅ Data quality is good - performance issues are likely strategic.")
```

### 4. Config Change Validation

```python
# Check if recent config changes affected results
impact = analyze_config_impact(db, config_id="config_v2", lookback_days=7)

if impact['recent_changes']:
    print("⚠️  Recent config changes detected:")
    for change in impact['recent_changes'][:3]:
        print(f"  - {change['action']} by {change['actor']} on {change['at'][:10]}")
        if change['diff']:
            print(f"    Changes: {list(change['diff'].keys())}")
    print("\nNote: Attribution results may reflect config changes.")
else:
    print("✅ No recent config changes - results are stable.")
```

### 5. Executive Summary

```python
# Generate comprehensive narrative
summary = generate_explainability_summary(
    db=db,
    scope="global",
    scope_id=None,
    config_id="config_v2",
    from_date=datetime(2026, 1, 1),
    to_date=datetime(2026, 1, 31),
)

print("### Executive Summary: January 2026\n")
for narrative in summary['narrative']:
    print(f"- {narrative}\n")

if summary['feature_importance']:
    print("### Strategic Insights:")
    for ch in summary['feature_importance']['channel_importance'][:3]:
        print(f"- {ch['channel']}: {ch['share']:.0f}% contribution")
```

---

## Best Practices

### 1. Regular Driver Analysis

Run driver analysis weekly to catch changes early:

```python
# Automated weekly report
def weekly_driver_report(db):
    # Get last 2 weeks of data
    this_week = get_week_data(db, week_offset=0)
    last_week = get_week_data(db, week_offset=1)
    
    drivers = compute_metric_drivers(this_week, last_week, "conversions")
    
    # Alert on large changes
    if abs(drivers['pct_change']) > 15:
        send_alert(f"⚠️  Large change detected: {drivers['narrative']}")
    
    return drivers
```

### 2. Feature Importance in Planning

Use feature importance to guide budget allocation:

```python
def suggest_budget_allocation(mmm_result, total_budget):
    importance = extract_mmm_feature_importance(mmm_result)
    
    allocations = {}
    for ch in importance['channel_importance']:
        channel = ch['channel']
        share = ch['share'] / 100.0
        elasticity = importance['elasticities'].get(channel, 0.5)
        
        # Weight by contribution + elasticity
        weight = share * (1 + elasticity)
        allocations[channel] = total_budget * weight
    
    # Normalize
    total_weight = sum(allocations.values())
    return {k: v / total_weight * total_budget for k, v in allocations.items()}
```

### 3. Config Change Tracking

Track config changes and correlate with performance:

```python
def validate_config_change(db, config_id, days_since_change=7):
    impact = analyze_config_impact(db, config_id, lookback_days=days_since_change)
    
    if not impact['recent_changes']:
        return {"status": "stable", "message": "No recent changes"}
    
    # Compare performance before/after
    # (Simplified - would fetch attribution results)
    
    return {
        "status": "changed",
        "changes": impact['recent_changes'],
        "recommendation": "Monitor results for next 7 days to validate impact"
    }
```

### 4. Data Quality Monitoring

Alert on low confidence:

```python
def monitor_confidence(db, scopes):
    alerts = []
    
    for scope, scope_id in scopes:
        dq = analyze_data_quality_impact(db, scope, scope_id)
        
        if dq['confidence_score'] < 70:
            alerts.append({
                "scope": f"{scope}/{scope_id}",
                "confidence": dq['confidence_score'],
                "issues": dq['issues'],
            })
    
    if alerts:
        send_notification(f"Low confidence detected in {len(alerts)} scopes")
    
    return alerts
```

### 5. Campaign Performance Reviews

Automate campaign reviews:

```python
def review_campaigns(db, attribution_result, spend_data):
    campaigns = [ch['channel'] for ch in attribution_result['channels']]
    
    reviews = []
    for campaign_id in campaigns:
        explanation = explain_campaign_performance(
            db=db,
            campaign_id=campaign_id,
            attribution_result=attribution_result,
            spend_data=spend_data,
        )
        
        # Flag underperformers
        if explanation['efficiency_metrics'].get('roi', 0) < 0.5:
            explanation['flag'] = "⚠️  UNDERPERFORMING"
        elif explanation['efficiency_metrics'].get('roi', 0) > 1.5:
            explanation['flag'] = "✅ HIGH PERFORMER"
        
        reviews.append(explanation)
    
    return reviews
```

---

## Troubleshooting

### Empty Driver Analysis

**Symptom:** `top_contributors` is empty

**Cause:** No data for period or all values are zero

**Fix:**
```python
# Check data availability
if not current_data or not previous_data:
    logger.warning("No data for driver analysis")
elif all(v == 0 for v in current_data.values()):
    logger.warning("All current values are zero")
```

### Low Confidence Scores

**Symptom:** `confidence_score` < 50

**Causes:**
- Low match rate (< 0.8)
- Low join rate (< 0.7)
- High missing data (> 20%)
- Data staleness (> 48 hours)

**Fix:** Review data quality components and address root causes

### Config Change Not Detected

**Symptom:** Recent config change not showing in explainability

**Cause:** Audit not logged

**Fix:** Ensure all config updates go through versioned API that logs audits

---

## Integration Examples

### Dashboard Integration

```typescript
// Frontend: Display explainability panel
import ExplainabilityPanel from './components/ExplainabilityPanel'

<ExplainabilityPanel 
  scope="channel"
  scopeId="paid_social"
  configId={currentConfig?.id}
/>
```

### Slack Notifications

```python
def send_weekly_insights(db, slack_webhook):
    # Generate narrative
    summary = generate_explainability_summary(...)
    
    # Format for Slack
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Weekly Attribution Insights"}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(summary['narrative'])}
            }
        ]
    }
    
    requests.post(slack_webhook, json=message)
```

### Email Reports

```python
def generate_email_report(db):
    html = "<h1>Attribution Performance Report</h1>"
    
    # Drivers
    drivers = compute_metric_drivers(...)
    html += f"<h2>Key Changes</h2><p>{drivers['narrative']}</p>"
    
    # Feature importance
    importance = extract_mmm_feature_importance(...)
    html += f"<h2>Channel Importance</h2><p>{importance['narrative']}</p>"
    
    # Data quality
    dq = analyze_data_quality_impact(...)
    html += f"<h2>Data Health</h2><p>{dq['narrative']}</p>"
    
    send_email(to="stakeholders@example.com", html=html)
```

---

## References

- **Service**: `app/services_explainability.py`
- **API Endpoints**: `app/main.py` (search for `/api/explainability`)
- **Frontend Component**: `frontend/src/components/ExplainabilityPanel.tsx`
- **Quality Service**: `app/services_quality.py`
