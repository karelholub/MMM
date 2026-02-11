# Explainability: Quick Reference

## Key Endpoints

### Compute Drivers
```bash
curl -X POST http://localhost:8000/api/explainability/drivers \
  -H "Content-Type: application/json" \
  -d '{"current_data": {"channelA": 100}, "previous_data": {"channelA": 80}, "metric_name": "conversions"}'
```

### MMM Feature Importance
```bash
curl http://localhost:8000/api/explainability/mmm-importance/{run_id}
```

### Config Impact
```bash
curl http://localhost:8000/api/explainability/config-impact/{config_id}?lookback_days=30
```

### Data Quality Impact
```bash
curl http://localhost:8000/api/explainability/data-quality/channel?scope_id=paid_social
```

### Campaign Explanation
```bash
curl -X POST http://localhost:8000/api/explainability/campaign/brand_q1 \
  -H "Content-Type: application/json" \
  -d '{"attribution_result": {...}, "spend_data": {"brand_q1": 3000}}'
```

### Narrative
```bash
curl "http://localhost:8000/api/explainability/narrative?scope=channel&from_date=2026-01-01&to_date=2026-01-31"
```

---

## Python Quick Start

```python
from app.services_explainability import (
    compute_metric_drivers,
    extract_mmm_feature_importance,
    analyze_config_impact,
    analyze_data_quality_impact,
    explain_campaign_performance,
)

# Drivers
result = compute_metric_drivers(
    current_data={"paid_search": 450, "paid_social": 320},
    previous_data={"paid_search": 420, "paid_social": 280},
    metric_name="conversions"
)
print(result['narrative'])

# Feature importance
importance = extract_mmm_feature_importance(mmm_result)
print(f"Top channel: {importance['channel_importance'][0]['channel']}")

# Config impact
impact = analyze_config_impact(db, config_id="config_v2")
print(f"Recent changes: {len(impact['recent_changes'])}")

# Data quality
dq = analyze_data_quality_impact(db, scope="channel", scope_id="paid_social")
print(f"Confidence: {dq['confidence_label']} ({dq['confidence_score']}/100)")

# Campaign
explanation = explain_campaign_performance(db, "brand_q1", attribution_result, spend_data)
print(explanation['recommendations'])
```

---

## Key Metrics

### Driver Metrics
- **Delta**: Absolute change
- **% Change**: Relative change
- **Contribution**: Share of total change

### Feature Importance
- **Contribution Share**: % of total effect
- **Elasticity**: Spend sensitivity
- **ROI Rank**: Return ranking

### Confidence Levels
| Score | Label | Action |
|-------|-------|--------|
| 85-100 | High | Trust results |
| 70-84 | Medium | Use directionally |
| 50-69 | Low | Interpret cautiously |
| 0-49 | Very Low | Fix data quality |

---

## Common Patterns

### Monthly Performance Review
```python
current = get_month_data(2026, 1)
previous = get_month_data(2025, 12)

drivers = compute_metric_drivers(current, previous, "conversions")
print(drivers['narrative'])
```

### Budget Optimization
```python
importance = extract_mmm_feature_importance(mmm_result)

for ch in importance['channel_importance'][:3]:
    elasticity = importance['elasticities'][ch['channel']]
    print(f"{ch['channel']}: {ch['share']:.0f}% contrib, {elasticity:.2f} elasticity")
```

### Data Quality Check
```python
dq = analyze_data_quality_impact(db, "channel", "paid_social")

if dq['confidence_score'] < 70:
    print(f"⚠️  Issues: {', '.join(dq['issues'])}")
else:
    print("✅ Data quality is good")
```

### Config Validation
```python
impact = analyze_config_impact(db, "config_v2", lookback_days=7)

if impact['recent_changes']:
    print(f"⚠️  {len(impact['recent_changes'])} changes in last 7 days")
    print("Monitor results to validate impact")
```

---

## Interpretation Guide

### Driver Analysis
```
"The largest contributor was paid_social with an increase of 40 (80% of total change)"
```
**Meaning:** paid_social drove 80% of the total metric increase

### Feature Importance
```
"Channels with high spend elasticity (>0.5): paid_search, paid_social"
```
**Meaning:** These channels respond strongly to budget changes - test incremental spend

### Elasticity Values
- **> 0.7**: Highly budget-sensitive - prime for optimization
- **0.5 - 0.7**: Moderately sensitive - good for scaling
- **0.3 - 0.5**: Low sensitivity - may be saturated
- **< 0.3**: Minimal response - consider reallocation

### ROI Interpretation
- **> 2.0**: Strong performer - increase budget
- **1.0 - 2.0**: Profitable - maintain or optimize
- **0.5 - 1.0**: Marginal - improve efficiency
- **< 0.5**: Underperforming - review strategy

---

## Narrative Templates

### Performance Change
```
"{metric} {increased/decreased} by {delta} ({pct}% change) from {prev} to {curr}.
The largest contributor was {channel} with a {direction} of {amount} ({share}% of total change)."
```

### Feature Importance
```
"The most important channel is {channel} contributing {share}% of total modeled effect.
The top 3 channels account for {sum}% of total contribution.
The highest ROI is {channel} at {roi}."
```

### Data Quality
```
"Data quality confidence is {label} ({score}/100).
{Issues or 'Data quality is good across all measured dimensions.'}
{Recommendation based on score}"
```

---

## Troubleshooting

### Empty Drivers
**Cause:** No data or all zeros
**Fix:** Check data availability for both periods

### Low Confidence
**Causes:** Low match rate, join rate, high missing data
**Fix:** Review DQ components and address root causes

### Missing Config Changes
**Cause:** Changes not logged in audits
**Fix:** Use versioned config API

### Zero Elasticities
**Cause:** Insufficient spend variation or flat response
**Fix:** Review MMM model convergence

---

## Dashboard Integration

```typescript
// React component
import ExplainabilityPanel from './components/ExplainabilityPanel'

<ExplainabilityPanel 
  scope="channel"
  scopeId="paid_social"
  configId={config?.id}
/>
```

---

## Monitoring SQL

```sql
-- Track driver changes over time
SELECT 
  date,
  channel,
  conversions,
  LAG(conversions) OVER (PARTITION BY channel ORDER BY date) as prev_conversions,
  conversions - LAG(conversions) OVER (PARTITION BY channel ORDER BY date) as delta
FROM channel_performance
ORDER BY date DESC, delta DESC;

-- Confidence trends
SELECT 
  ts_bucket::date,
  scope,
  scope_id,
  confidence_score,
  confidence_label
FROM attribution_quality_snapshots
WHERE scope = 'channel'
ORDER BY ts_bucket DESC;
```

---

## Resources

- [Full Guide](EXPLAINABILITY.md)
- [API Docs](http://localhost:8000/docs)
- [Frontend Component](../frontend/src/components/ExplainabilityPanel.tsx)
