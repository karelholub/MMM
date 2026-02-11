# Taxonomy & DQ: Quick Reference

## Key Endpoints

### Validate UTMs
```bash
curl -X POST http://localhost:8000/api/taxonomy/validate-utm \
  -H "Content-Type: application/json" \
  -d '{"utm_source": "google", "utm_medium": "cpc"}'
```

### Map Channel
```bash
curl -X POST http://localhost:8000/api/taxonomy/map-channel \
  -H "Content-Type: application/json" \
  -d '{"source": "google", "medium": "cpc"}'
```

### Unknown Share Report
```bash
curl http://localhost:8000/api/taxonomy/unknown-share
```

### Coverage Report
```bash
curl http://localhost:8000/api/taxonomy/coverage
```

### Channel Confidence
```bash
curl http://localhost:8000/api/taxonomy/channel-confidence?channel=paid_social
```

---

## Python Quick Start

```python
from app.services_taxonomy import (
    validate_utm_params,
    map_to_channel,
    compute_unknown_share,
    compute_channel_confidence,
    normalize_touchpoint_with_confidence,
)

# Validate UTMs
params = {"utm_source": "google", "utm_medium": "cpc"}
result = validate_utm_params(params)
print(f"Valid: {result.is_valid}, Confidence: {result.confidence}")

# Map to channel
mapping = map_to_channel("google", "cpc")
print(f"Channel: {mapping.channel} ({mapping.confidence:.2f})")

# Normalize touchpoint
tp = {"utm_source": "fb", "utm_medium": "cpc"}
normalized, confidence = normalize_touchpoint_with_confidence(tp)
print(f"Normalized: {normalized['channel']} ({confidence:.2f})")

# Unknown share
report = compute_unknown_share(journeys)
print(f"Unknown: {report.unknown_share:.1%}")

# Channel confidence
conf = compute_channel_confidence(journeys, "paid_social")
print(f"Mean: {conf['mean_confidence']:.2f}")
```

---

## Confidence Score Reference

| Score | Meaning | Action |
|-------|---------|--------|
| 1.0 | Perfect match | ✅ Good |
| 0.8 | Partial match | ✅ Good |
| 0.6 | Fuzzy/alias | ⚠️ Review aliases |
| 0.3 | Fallback | ⚠️ Add taxonomy rule |
| 0.0 | No data | ❌ Fix UTM tagging |

---

## Target Metrics

| Metric | Target | Alert |
|--------|--------|-------|
| Unknown share | < 10% | > 15% |
| Touchpoint confidence | > 0.8 | < 0.7 |
| Source coverage | > 90% | < 80% |
| Medium coverage | > 85% | < 75% |

---

## Common Issues

### High Unknown Share
```bash
# Check patterns
curl http://localhost:8000/api/taxonomy/unknown-share

# Add rule
curl -X POST http://localhost:8000/api/taxonomy \
  -d '{"channel_rules": [...], "source_aliases": {...}}'
```

### Low Confidence
```bash
# Check channel
curl http://localhost:8000/api/taxonomy/channel-confidence?channel=paid_social

# Review samples and add aliases
```

### UTM Validation Errors
```bash
# Validate before launch
curl -X POST http://localhost:8000/api/taxonomy/validate-utm \
  -d '{"utm_source": "google", "utm_medium": "cpc", "utm_campaign": "brand"}'
```

---

## DQ Alert Setup

```sql
INSERT INTO dq_alert_rules (name, metric_key, source, threshold_type, threshold_value, severity)
VALUES 
  ('High unknown share', 'unknown_channel_share', 'taxonomy', 'gt', 0.15, 'warning'),
  ('Low confidence', 'mean_touchpoint_confidence', 'taxonomy', 'lt', 0.7, 'warning');
```

---

## Reserved Values

These are treated as missing/unknown:
- `(not set)`
- `(none)`
- `null`
- `undefined`
- `n/a`
- `(not provided)`
- `(direct)`

---

## Standard UTM Parameters

✅ **Required:**
- `utm_source` (required for attribution)

✅ **Recommended:**
- `utm_medium` (for channel mapping)
- `utm_campaign`

✅ **Optional:**
- `utm_term` (for paid search keywords)
- `utm_content` (for A/B testing)

---

## Taxonomy Rule Priority

Rules are evaluated in order. First match wins.

```json
{
  "channel_rules": [
    {"name": "Paid Search", "source_regex": "google|bing", "medium_regex": "cpc"},
    {"name": "Paid Social", "source_regex": "facebook|instagram", "medium_regex": "cpc|social"},
    {"name": "Email", "medium_regex": "email"},
    {"name": "Direct", "medium_regex": "(none|direct)"}
  ]
}
```

---

## Pre-Campaign Checklist

- [ ] UTM parameters validated
- [ ] Channel mapping confidence > 0.7
- [ ] No reserved values in UTMs
- [ ] Campaign name follows naming convention
- [ ] Source/medium match taxonomy rules

---

## Monitoring Dashboard SQL

```sql
-- Unknown share trend
SELECT 
  ts_bucket::date,
  metric_value as unknown_share
FROM dq_snapshots
WHERE metric_key = 'unknown_channel_share' AND source = 'taxonomy'
ORDER BY ts_bucket DESC LIMIT 30;

-- Confidence trend
SELECT 
  ts_bucket::date,
  metric_value as mean_confidence
FROM dq_snapshots
WHERE metric_key = 'mean_touchpoint_confidence' AND source = 'taxonomy'
ORDER BY ts_bucket DESC LIMIT 30;

-- Coverage trend
SELECT 
  ts_bucket::date,
  MAX(CASE WHEN metric_key = 'source_coverage' THEN metric_value END) as source_cov,
  MAX(CASE WHEN metric_key = 'medium_coverage' THEN metric_value END) as medium_cov
FROM dq_snapshots
WHERE source = 'taxonomy'
GROUP BY ts_bucket::date
ORDER BY ts_bucket::date DESC LIMIT 30;
```

---

## Resources

- [Full Guide](TAXONOMY_AND_DQ.md)
- [API Docs](http://localhost:8000/docs)
- [Data Contracts](DATA_CONTRACTS_AND_MMM_ALIGNMENT.md)
