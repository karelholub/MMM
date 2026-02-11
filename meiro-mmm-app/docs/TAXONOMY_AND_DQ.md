# Taxonomy & Data Quality: Enhanced Guide

This guide covers the strengthened taxonomy system with UTM validation, channel mapping confidence, unknown share tracking, and per-entity quality scoring.

---

## Overview

The enhanced taxonomy and DQ infrastructure provides:

- **UTM Validation**: Comprehensive validation of UTM parameters with normalization and error detection
- **Confidence Scoring**: Per-touchpoint, per-journey, and per-channel confidence metrics
- **Unknown Share Tracking**: Monitor unmapped traffic and identify gaps in taxonomy
- **Coverage Analysis**: Measure how well your taxonomy covers incoming traffic
- **Per-Entity DQ**: Quality scores for individual touchpoints, journeys, and channels

---

## Core Concepts

### Confidence Scoring

Confidence scores range from 0.0 to 1.0 and represent how reliably a touchpoint can be attributed:

| Score | Interpretation | Example |
|-------|----------------|---------|
| 1.0   | Perfect match with both source and medium | google + cpc → paid_search |
| 0.8   | Match with source or medium only | google + (missing) → paid_search |
| 0.6   | Fuzzy match or alias used | fb + cpc → paid_social |
| 0.3   | Fallback to default channel | (missing) + (missing) → unknown |
| 0.0   | No information | Empty touchpoint |

### Unknown Share

**Unknown share** = percentage of touchpoints that map to "unknown" channel or have confidence < 0.5.

Target: < 10% unknown share for reliable attribution.

---

## API Reference

### Validate UTM Parameters

```http
POST /api/taxonomy/validate-utm
Content-Type: application/json

{
  "utm_source": "google",
  "utm_medium": "cpc",
  "utm_campaign": "brand_2026_q1",
  "utm_content": "hero_banner"
}
```

**Response:**
```json
{
  "is_valid": true,
  "warnings": [],
  "errors": [],
  "normalized": {
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "brand_2026_q1",
    "utm_content": "hero_banner"
  },
  "confidence": 1.0
}
```

**Common Warnings:**
- "Non-standard parameter 'source' mapped to 'utm_source'"
- "Reserved value '(not set)' in utm_source"
- "Missing utm_medium (recommended for channel mapping)"

**Common Errors:**
- "Missing utm_source (required for attribution)"

### Map Source/Medium to Channel

```http
POST /api/taxonomy/map-channel
Content-Type: application/json

{
  "source": "google",
  "medium": "cpc"
}
```

**Response:**
```json
{
  "channel": "paid_search",
  "matched_rule": "Paid Search",
  "confidence": 1.0,
  "source": "google",
  "medium": "cpc",
  "fallback_reason": null
}
```

### Get Unknown Share Report

```http
GET /api/taxonomy/unknown-share?limit=20
```

**Response:**
```json
{
  "total_touchpoints": 10000,
  "unknown_count": 850,
  "unknown_share": 0.085,
  "by_source": {
    "(not set)": 450,
    "partner_xyz": 200,
    "newsletter": 150,
    "app": 50
  },
  "by_medium": {
    "(none)": 400,
    "referral": 300,
    "push": 150
  },
  "top_unmapped_patterns": [
    {"source": "(not set)", "medium": "(none)", "count": 350},
    {"source": "partner_xyz", "medium": "referral", "count": 200},
    {"source": "newsletter", "medium": "email", "count": 150}
  ],
  "sample_unmapped": [...]
}
```

**Interpretation:**
- 8.5% of touchpoints are unmapped (target: < 10%)
- Top unmapped: "(not set) / (none)" — likely direct traffic, consider adding rule
- "partner_xyz / referral" — new partner, add to taxonomy
- "newsletter / email" — email already mapped, check for typo in source

### Get Taxonomy Coverage

```http
GET /api/taxonomy/coverage
```

**Response:**
```json
{
  "channel_distribution": {
    "paid_search": 4500,
    "paid_social": 3200,
    "email": 1500,
    "direct": 600,
    "unknown": 200
  },
  "source_coverage": 0.92,
  "medium_coverage": 0.88,
  "rule_usage": {
    "Paid Search": 4500,
    "Paid Social": 3200,
    "Email": 1500,
    "Direct": 600
  },
  "top_unmapped_patterns": [...]
}
```

**Interpretation:**
- 92% of sources have good mapping (>= 0.5 confidence)
- 88% of mediums have good mapping
- "Paid Search" rule matches 45% of traffic
- Consider adding rules for unmapped patterns

### Get Channel Confidence

```http
GET /api/taxonomy/channel-confidence?channel=paid_social
```

**Response:**
```json
{
  "mean_confidence": 0.87,
  "touchpoint_count": 3200,
  "low_confidence_count": 180,
  "low_confidence_share": 0.056,
  "sample_low_confidence": [
    {
      "source": "fb",
      "medium": "cpm",
      "campaign": "brand_awareness",
      "confidence": 0.45,
      "warnings": ["Non-standard parameter 'fb' mapped to 'facebook'"]
    }
  ]
}
```

**Interpretation:**
- Mean confidence: 0.87 (good, > 0.8 target)
- 5.6% of touchpoints have low confidence (< 0.5)
- Sample shows "fb" alias issue — consider adding to aliases

### Compute Taxonomy DQ Snapshots

```http
POST /api/taxonomy/compute-dq
```

Computes and persists:
- `unknown_channel_share`
- `mean_touchpoint_confidence`
- `mean_journey_confidence`
- `source_coverage`
- `medium_coverage`
- `low_confidence_touchpoint_share`

**Response:**
```json
{
  "computed": 6,
  "metrics": [
    {
      "source": "taxonomy",
      "metric_key": "unknown_channel_share",
      "metric_value": 0.085,
      "ts_bucket": "2026-02-11T14:00:00Z"
    },
    ...
  ]
}
```

---

## Python API

### Validate UTM Parameters

```python
from app.services_taxonomy import validate_utm_params

params = {
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "brand",
    "source": "google",  # Duplicate - will warn
}

result = validate_utm_params(params)
print(f"Valid: {result.is_valid}")
print(f"Confidence: {result.confidence}")
print(f"Warnings: {result.warnings}")
print(f"Normalized: {result.normalized}")
```

### Map to Channel

```python
from app.services_taxonomy import map_to_channel

mapping = map_to_channel(source="google", medium="cpc")
print(f"Channel: {mapping.channel}")  # paid_search
print(f"Confidence: {mapping.confidence}")  # 1.0
print(f"Matched rule: {mapping.matched_rule}")  # Paid Search
```

### Normalize Touchpoint with Confidence

```python
from app.services_taxonomy import normalize_touchpoint_with_confidence

touchpoint = {
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "brand",
    "timestamp": "2026-02-10T10:00:00Z",
}

normalized, confidence = normalize_touchpoint_with_confidence(touchpoint)
print(f"Channel: {normalized['channel']}")  # paid_search
print(f"Confidence: {confidence}")  # 1.0
print(f"Validation: {normalized['_utm_validation']}")
```

### Compute Unknown Share

```python
from app.services_taxonomy import compute_unknown_share

journeys = load_journeys_from_db(db)
report = compute_unknown_share(journeys)

print(f"Unknown share: {report.unknown_share:.1%}")
print(f"Top unmapped patterns:")
for (source, medium), count in sorted(report.by_source_medium.items(), key=lambda x: -x[1])[:5]:
    print(f"  {source} / {medium}: {count}")
```

### Compute Channel Confidence

```python
from app.services_taxonomy import compute_channel_confidence

confidence = compute_channel_confidence(journeys, channel="paid_social")
print(f"Mean confidence: {confidence['mean_confidence']:.2f}")
print(f"Low confidence share: {confidence['low_confidence_share']:.1%}")
```

---

## Taxonomy Configuration

### Default Taxonomy

```json
{
  "channel_rules": [
    {
      "name": "Paid Search",
      "channel": "paid_search",
      "source_regex": "google|bing|baidu",
      "medium_regex": "cpc|ppc|paid_search"
    },
    {
      "name": "Paid Social",
      "channel": "paid_social",
      "source_regex": "facebook|meta|instagram|linkedin|twitter|tiktok",
      "medium_regex": "cpc|paid_social|social"
    },
    {
      "name": "Email",
      "channel": "email",
      "medium_regex": "email"
    },
    {
      "name": "Direct",
      "channel": "direct",
      "medium_regex": "(none|direct)"
    }
  ],
  "source_aliases": {
    "fb": "facebook",
    "ig": "instagram",
    "g": "google"
  },
  "medium_aliases": {
    "paid": "cpc",
    "cpm": "display"
  }
}
```

### Adding Custom Rules

```http
POST /api/taxonomy
Content-Type: application/json

{
  "channel_rules": [
    ...existing rules...,
    {
      "name": "Affiliate",
      "channel": "affiliate",
      "medium_regex": "affiliate|referral"
    },
    {
      "name": "Display",
      "channel": "display",
      "medium_regex": "display|banner|cpm"
    }
  ],
  "source_aliases": {
    ...existing aliases...,
    "partner_xyz": "affiliate_xyz"
  }
}
```

---

## Best Practices

### 1. Monitor Unknown Share

Track unknown share over time and set alerts:

```sql
SELECT 
  ts_bucket::date as date,
  metric_value as unknown_share
FROM dq_snapshots
WHERE metric_key = 'unknown_channel_share'
  AND source = 'taxonomy'
ORDER BY ts_bucket DESC
LIMIT 30;
```

**Target:** < 10% unknown share

**Actions if high:**
- Review top unmapped patterns
- Add missing rules to taxonomy
- Fix UTM parameter typos
- Educate marketing teams on UTM standards

### 2. Set Up DQ Alerts

Create alert rules for taxonomy metrics:

```sql
INSERT INTO dq_alert_rules (name, metric_key, source, threshold_type, threshold_value, severity, is_enabled)
VALUES 
  ('High unknown share', 'unknown_channel_share', 'taxonomy', 'gt', 0.15, 'warning', true),
  ('Low touchpoint confidence', 'mean_touchpoint_confidence', 'taxonomy', 'lt', 0.7, 'warning', true),
  ('Low source coverage', 'source_coverage', 'taxonomy', 'lt', 0.8, 'critical', true);
```

### 3. Regular Taxonomy Audits

Monthly checklist:
- [ ] Review unknown share report
- [ ] Check top unmapped patterns
- [ ] Add missing rules/aliases
- [ ] Review channel confidence scores
- [ ] Update UTM standards documentation

### 4. Validate UTMs Before Campaign Launch

```python
# Pre-flight check
utm_params = {
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "brand_2026_q1",
}

validation = validate_utm_params(utm_params)
if not validation.is_valid:
    print("❌ UTM validation failed:")
    for error in validation.errors:
        print(f"  - {error}")
else:
    print("✅ UTMs valid")
    if validation.warnings:
        print("⚠️  Warnings:")
        for warning in validation.warnings:
            print(f"  - {warning}")
    
    # Check channel mapping
    mapping = map_to_channel(
        source=validation.normalized.get("utm_source"),
        medium=validation.normalized.get("utm_medium")
    )
    print(f"Maps to: {mapping.channel} (confidence: {mapping.confidence:.2f})")
```

### 5. Per-Channel Quality Monitoring

Track confidence by channel:

```python
channels = ["paid_search", "paid_social", "email", "direct", "display"]

for channel in channels:
    confidence = compute_channel_confidence(journeys, channel)
    print(f"{channel}:")
    print(f"  Mean confidence: {confidence['mean_confidence']:.2f}")
    print(f"  Low confidence: {confidence['low_confidence_share']:.1%}")
    
    if confidence['low_confidence_share'] > 0.1:
        print(f"  ⚠️  High low-confidence share!")
        print(f"  Sample issues:")
        for sample in confidence['sample_low_confidence'][:3]:
            print(f"    - {sample['source']} / {sample['medium']} ({sample['confidence']:.2f})")
```

---

## Troubleshooting

### High Unknown Share

**Symptoms:** `unknown_channel_share` > 15%

**Diagnosis:**
```http
GET /api/taxonomy/unknown-share?limit=50
```

**Fixes:**
1. Check top unmapped patterns
2. Add missing rules:
   ```json
   {
     "name": "Partner Traffic",
     "channel": "partner",
     "source_regex": "partner_.*",
     "medium_regex": "referral"
   }
   ```
3. Add source aliases:
   ```json
   {
     "source_aliases": {
       "partner_xyz": "partner_official"
     }
   }
   ```

### Low Confidence Scores

**Symptoms:** `mean_touchpoint_confidence` < 0.7

**Diagnosis:**
```http
GET /api/taxonomy/channel-confidence?channel=paid_social
```

**Fixes:**
1. Review low-confidence samples
2. Standardize UTM parameters
3. Add missing aliases
4. Train teams on UTM best practices

### Reserved Values in UTMs

**Symptoms:** Warnings like "Reserved value '(not set)' in utm_source"

**Cause:** Analytics platforms use reserved values for missing data

**Fixes:**
1. Ensure proper UTM tagging on all campaigns
2. Add fallback rules for direct traffic:
   ```json
   {
     "name": "Direct (Not Set)",
     "channel": "direct",
     "source_regex": "\\(not set\\)|\\(none\\)",
     "medium_regex": "\\(not set\\)|\\(none\\)|direct"
   }
   ```

### Source/Medium Mismatches

**Symptoms:** Good source coverage but low medium coverage

**Cause:** Missing or inconsistent medium values

**Fixes:**
1. Make utm_medium required in campaign guidelines
2. Add medium aliases for common typos
3. Use validation endpoint in campaign creation tools

---

## Integration Examples

### Pre-Campaign Validation

```python
# Campaign setup validation
campaign = {
    "name": "Brand Q1 2026",
    "url": "https://example.com?utm_source=google&utm_medium=cpc&utm_campaign=brand_q1_2026",
}

# Extract UTMs from URL
from urllib.parse import urlparse, parse_qs
params = parse_qs(urlparse(campaign["url"]).query)
utm_params = {k: v[0] for k, v in params.items() if k.startswith("utm_")}

# Validate
validation = validate_utm_params(utm_params)
mapping = map_to_channel(
    source=utm_params.get("utm_source"),
    medium=utm_params.get("utm_medium")
)

if not validation.is_valid or mapping.confidence < 0.7:
    print(f"⚠️  Campaign '{campaign['name']}' has quality issues:")
    print(f"  UTM confidence: {validation.confidence:.2f}")
    print(f"  Channel mapping confidence: {mapping.confidence:.2f}")
    print(f"  Warnings: {validation.warnings}")
    # Block campaign launch or send alert
else:
    print(f"✅ Campaign '{campaign['name']}' validated")
    print(f"  Maps to: {mapping.channel}")
```

### Nightly DQ Report

```python
# Scheduled task (cron, Celery, etc.)
from app.services_taxonomy import (
    compute_unknown_share,
    compute_taxonomy_coverage,
    persist_taxonomy_dq_snapshots,
)

journeys = load_journeys_from_db(db)

# Compute metrics
unknown_report = compute_unknown_share(journeys)
coverage = compute_taxonomy_coverage(journeys)

# Persist snapshots
snapshots = persist_taxonomy_dq_snapshots(db, journeys)

# Generate report
report = f"""
Taxonomy DQ Report - {datetime.now().date()}

Unknown Share: {unknown_report.unknown_share:.1%}
Source Coverage: {coverage['source_coverage']:.1%}
Medium Coverage: {coverage['medium_coverage']:.1%}

Top Unmapped Patterns:
"""

for (source, medium), count in sorted(
    unknown_report.by_source_medium.items(),
    key=lambda x: -x[1]
)[:10]:
    report += f"  - {source} / {medium}: {count}\n"

# Send email/Slack notification
send_notification(report)
```

---

## Metrics Reference

### Taxonomy DQ Metrics

| Metric | Description | Target | Alert Threshold |
|--------|-------------|--------|-----------------|
| `unknown_channel_share` | % of touchpoints mapping to "unknown" | < 10% | > 15% |
| `mean_touchpoint_confidence` | Average confidence across all touchpoints | > 0.8 | < 0.7 |
| `mean_journey_confidence` | Average confidence across all journeys | > 0.8 | < 0.7 |
| `source_coverage` | % of sources with good mapping (>= 0.5) | > 90% | < 80% |
| `medium_coverage` | % of mediums with good mapping (>= 0.5) | > 85% | < 75% |
| `low_confidence_touchpoint_share` | % of touchpoints with confidence < 0.5 | < 15% | > 25% |

---

## Database Schema

### DQ Snapshots (Taxonomy)

```sql
SELECT * FROM dq_snapshots 
WHERE source = 'taxonomy' 
ORDER BY ts_bucket DESC 
LIMIT 10;
```

| ts_bucket | metric_key | metric_value | meta_json |
|-----------|------------|--------------|-----------|
| 2026-02-11 14:00 | unknown_channel_share | 0.085 | {"unknown_count": 850, "total": 10000} |
| 2026-02-11 14:00 | mean_touchpoint_confidence | 0.87 | {"sample_size": 10000} |
| 2026-02-11 14:00 | source_coverage | 0.92 | {} |

---

## References

- **Service**: `app/services_taxonomy.py`
- **DQ Integration**: `app/services_data_quality.py`
- **API Endpoints**: `app/main.py` (search for `/api/taxonomy`)
- **Taxonomy Storage**: `backend/app/data/taxonomy.json`
