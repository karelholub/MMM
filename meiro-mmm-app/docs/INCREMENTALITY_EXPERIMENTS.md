# Incrementality Experiments Guide

This guide covers the mature incrementality experiment infrastructure, including automated assignment, exposure tracking, and nightly reporting.

---

## Overview

Incrementality experiments (holdout tests) measure the causal impact of marketing channels by randomly withholding treatment from a control group and comparing outcomes.

**Key features:**
- **Deterministic assignment**: Hash-based assignment ensures stable control/treatment groups
- **Exposure tracking**: Log when treatment is actually delivered (e.g., message sent)
- **Automated reporting**: Nightly jobs compute results and generate alerts
- **Power analysis**: Estimate required sample size before launching
- **Time series visualization**: Track cumulative metrics over experiment duration

---

## Core Concepts

### Assignment vs. Exposure

- **Assignment**: Which group (treatment/control) a profile belongs to
- **Exposure**: When treatment was actually delivered to a profile

Example: A profile is assigned to treatment, but the email fails to send. The profile has an assignment but no exposure.

### Deterministic Assignment

Uses hash-based assignment (`SHA256(profile_id:experiment_id:salt)`) to ensure:
- Same profile always gets same group for a given experiment
- No need to pre-assign before exposure (lazy assignment)
- Stable across restarts and distributed systems

---

## API Reference

### Create Experiment

```http
POST /api/experiments
Content-Type: application/json

{
  "name": "Email holdout Q1 2026",
  "channel": "email",
  "start_at": "2026-03-01T00:00:00Z",
  "end_at": "2026-03-31T23:59:59Z",
  "conversion_key": "purchase",
  "notes": "Test email incrementality for Q1 campaign"
}
```

**Response:**
```json
{
  "id": 1,
  "name": "Email holdout Q1 2026",
  "channel": "email",
  "status": "draft",
  "start_at": "2026-03-01T00:00:00Z",
  "end_at": "2026-03-31T23:59:59Z",
  "conversion_key": "purchase"
}
```

### Assign Profiles

```http
POST /api/experiments/1/assign
Content-Type: application/json

{
  "profile_ids": ["user_001", "user_002", "user_003"],
  "treatment_rate": 0.8
}
```

**Response:**
```json
{
  "assigned": 3,
  "treatment": 2,
  "control": 1
}
```

Assignment is **deterministic** — calling this endpoint multiple times with the same profile IDs will produce the same assignments.

### Record Exposures

```http
POST /api/experiments/1/exposures
Content-Type: application/json

{
  "exposures": [
    {
      "profile_id": "user_001",
      "exposure_ts": "2026-03-05T10:30:00Z",
      "campaign_id": "email_q1_2026",
      "message_id": "msg_12345"
    },
    {
      "profile_id": "user_002",
      "exposure_ts": "2026-03-05T10:31:00Z",
      "campaign_id": "email_q1_2026",
      "message_id": "msg_12346"
    }
  ]
}
```

**Response:**
```json
{
  "recorded": 2
}
```

### Record Outcomes

```http
POST /api/experiments/1/outcomes
Content-Type: application/json

{
  "outcomes": [
    {
      "profile_id": "user_001",
      "conversion_ts": "2026-03-07T14:22:00Z",
      "value": 120.50
    }
  ]
}
```

**Response:**
```json
{
  "inserted": 1
}
```

### Get Results

```http
GET /api/experiments/1/results
```

**Response:**
```json
{
  "experiment_id": 1,
  "status": "running",
  "treatment": {
    "n": 800,
    "conversions": 45,
    "conversion_rate": 0.05625,
    "total_value": 5420.00
  },
  "control": {
    "n": 200,
    "conversions": 8,
    "conversion_rate": 0.04,
    "total_value": 960.00
  },
  "uplift_abs": 0.01625,
  "uplift_rel": 0.40625,
  "ci_low": 0.002,
  "ci_high": 0.031,
  "p_value": 0.032,
  "insufficient_data": false
}
```

**Interpretation:**
- Treatment conversion rate: 5.625%
- Control conversion rate: 4.0%
- Absolute uplift: 1.625 percentage points
- Relative uplift: 40.6% increase
- 95% CI: [0.2pp, 3.1pp]
- p-value: 0.032 (significant at α=0.05)

### Get Time Series

```http
GET /api/experiments/1/time-series?freq=D
```

Returns daily cumulative metrics for visualization:

```json
{
  "data": [
    {
      "date": "2026-03-01",
      "treatment_n": 120,
      "treatment_conversions": 5,
      "treatment_rate": 0.0417,
      "control_n": 30,
      "control_conversions": 1,
      "control_rate": 0.0333,
      "uplift_abs": 0.0084,
      "uplift_rel": 0.252
    },
    ...
  ]
}
```

### Power Analysis

Estimate required sample size before launching:

```http
POST /api/experiments/power-analysis
Content-Type: application/json

{
  "baseline_rate": 0.05,
  "mde": 0.01,
  "alpha": 0.05,
  "power": 0.8,
  "treatment_rate": 0.5
}
```

**Response:**
```json
{
  "total_sample_size": 3142,
  "treatment_size": 1571,
  "control_size": 1571,
  "baseline_rate": 0.05,
  "mde": 0.01,
  "alpha": 0.05,
  "power": 0.8
}
```

**Interpretation:** To detect a 1pp lift from a 5% baseline with 80% power and 5% significance, you need ~3,142 total profiles.

### Auto-Assign from Historical Data

Automatically assign profiles based on conversion paths:

```http
POST /api/experiments/1/auto-assign
Content-Type: application/json

{
  "channel": "email",
  "start_date": "2026-03-01T00:00:00Z",
  "end_date": "2026-03-31T23:59:59Z",
  "treatment_rate": 0.5
}
```

This finds all profiles who had touchpoints in the specified channel during the period and assigns them deterministically. Useful for post-hoc analysis.

---

## Nightly Reporting

### Running Manually

```bash
# From backend directory
python -m app.scheduler --task nightly-report
```

### Scheduling with Cron

```cron
# Run at 2 AM daily
0 2 * * * cd /path/to/backend && python -m app.scheduler --task nightly-report
```

### In-Process Scheduling (Development)

```python
from app.scheduler import start_scheduler

# Start background scheduler
start_scheduler()
```

### Report Output

The nightly report computes results for all running experiments and generates alerts:

```json
{
  "as_of_date": "2026-03-15T02:00:00Z",
  "experiments": [
    {
      "id": 1,
      "name": "Email holdout Q1 2026",
      "channel": "email",
      "status": "running",
      "results": { ... }
    }
  ],
  "alerts": [
    {
      "experiment_id": 1,
      "message": "Experiment 'Email holdout Q1 2026' shows significant positive uplift (p=0.0234)",
      "severity": "success"
    }
  ]
}
```

**Alert severities:**
- `info`: Insufficient data
- `success`: Significant positive uplift
- `warning`: Significant negative uplift
- `error`: Computation failure

---

## Best Practices

### 1. Power Analysis First

Always run power analysis before launching to ensure you have sufficient sample size:

```python
from app.services_incrementality import estimate_sample_size

n = estimate_sample_size(
    baseline_rate=0.05,  # Current conversion rate
    mde=0.01,            # Want to detect 1pp lift
    alpha=0.05,
    power=0.8,
)
print(f"Need {n} total profiles")
```

### 2. Deterministic Assignment

Use deterministic assignment for stable groups:

```python
from app.services_incrementality import assign_profiles_deterministic

counts = assign_profiles_deterministic(
    db=db,
    experiment_id=1,
    profile_ids=["user_001", "user_002", ...],
    treatment_rate=0.5,
)
```

Benefits:
- Same profile always gets same group
- Can assign incrementally as new profiles arrive
- No risk of re-randomization

### 3. Track Exposures Separately

Don't assume assignment = exposure. Track actual delivery:

```python
from app.services_incrementality import record_exposure

# After sending email
record_exposure(
    db=db,
    experiment_id=1,
    profile_id="user_001",
    exposure_ts=datetime.utcnow(),
    campaign_id="email_q1_2026",
    message_id="msg_12345",
)
```

### 4. Monitor Time Series

Check cumulative metrics daily to catch issues early:

```python
from app.services_incrementality import get_experiment_time_series

df = get_experiment_time_series(db, experiment_id=1, freq="D")
print(df[["date", "treatment_rate", "control_rate", "uplift_abs"]])
```

### 5. Set Realistic MDEs

Don't aim for tiny effects unless you have huge sample sizes:

| Baseline | MDE | Sample size (80% power) |
|----------|-----|-------------------------|
| 5%       | 0.5pp | 12,568 |
| 5%       | 1.0pp | 3,142 |
| 5%       | 2.0pp | 786 |
| 10%      | 1.0pp | 6,284 |
| 10%      | 2.0pp | 1,571 |

### 6. Run Long Enough

Allow time for conversions to materialize:
- Email: 7-14 days
- Push: 3-7 days
- SMS: 1-3 days

### 7. Check Balance

Verify treatment and control groups are similar:

```sql
SELECT 
  e.group,
  COUNT(*) as n,
  AVG(p.age) as avg_age,
  AVG(p.ltv) as avg_ltv
FROM experiment_assignments e
JOIN profiles p ON e.profile_id = p.id
WHERE e.experiment_id = 1
GROUP BY e.group;
```

---

## Integration Examples

### Meiro CDP Integration

```python
# 1. Fetch eligible profiles from Meiro CDP
from app.connectors import meiro_cdp

profiles = meiro_cdp.fetch_segment(segment_id="active_users")
profile_ids = [p["id"] for p in profiles]

# 2. Assign to experiment
counts = assign_profiles_deterministic(
    db=db,
    experiment_id=1,
    profile_ids=profile_ids,
    treatment_rate=0.8,
)

# 3. Get treatment group for campaign
assignments = db.query(ExperimentAssignment).filter(
    ExperimentAssignment.experiment_id == 1,
    ExperimentAssignment.group == "treatment"
).all()

treatment_ids = [a.profile_id for a in assignments]

# 4. Send campaign to treatment only
# (control group receives no message)
```

### Webhook Integration

```python
# Record exposure from webhook
@app.post("/webhooks/message-sent")
def message_sent_webhook(body: dict):
    record_exposure(
        db=db,
        experiment_id=body["experiment_id"],
        profile_id=body["profile_id"],
        exposure_ts=datetime.fromisoformat(body["sent_at"]),
        campaign_id=body["campaign_id"],
        message_id=body["message_id"],
    )
    return {"status": "ok"}

# Record outcome from webhook
@app.post("/webhooks/conversion")
def conversion_webhook(body: dict):
    record_outcome(
        db=db,
        experiment_id=body["experiment_id"],
        profile_id=body["profile_id"],
        conversion_ts=datetime.fromisoformat(body["converted_at"]),
        value=body["value"],
    )
    return {"status": "ok"}
```

---

## Troubleshooting

### "Insufficient data" error

**Cause:** Not enough assignments or outcomes to compute uplift.

**Solution:** Wait for more data, or check that:
- Assignments were recorded (`/api/experiments/{id}/assign`)
- Outcomes are being tracked (`/api/experiments/{id}/outcomes`)
- Experiment is within date range

### Assignments not stable

**Cause:** Using random assignment instead of deterministic.

**Solution:** Use `assign_profiles_deterministic` instead of random shuffle.

### No exposures recorded

**Cause:** Exposure tracking not integrated.

**Solution:** Add `record_exposure` calls after message delivery:

```python
# After sending email/push/SMS
record_exposure(db, experiment_id, profile_id, exposure_ts)
```

### Time series empty

**Cause:** No assignments or outcomes recorded.

**Solution:** Check that experiment has data:

```sql
SELECT COUNT(*) FROM experiment_assignments WHERE experiment_id = 1;
SELECT COUNT(*) FROM experiment_outcomes WHERE experiment_id = 1;
```

---

## Database Schema

### experiment_assignments

| Column | Type | Description |
|--------|------|-------------|
| id | int | Primary key |
| experiment_id | int | FK to experiments |
| profile_id | str | Profile identifier |
| group | str | "treatment" or "control" |
| assigned_at | datetime | Assignment timestamp |

### experiment_exposures

| Column | Type | Description |
|--------|------|-------------|
| id | int | Primary key |
| experiment_id | int | FK to experiments |
| profile_id | str | Profile identifier |
| exposure_ts | datetime | Exposure timestamp |
| campaign_id | str | Campaign identifier (optional) |
| message_id | str | Message identifier (optional) |

### experiment_outcomes

| Column | Type | Description |
|--------|------|-------------|
| id | int | Primary key |
| experiment_id | int | FK to experiments |
| profile_id | str | Profile identifier |
| conversion_ts | datetime | Conversion timestamp |
| value | float | Conversion value |

### experiment_results

| Column | Type | Description |
|--------|------|-------------|
| id | int | Primary key |
| experiment_id | int | FK to experiments (unique) |
| computed_at | datetime | Last computation time |
| uplift_abs | float | Absolute uplift (pp) |
| uplift_rel | float | Relative uplift (%) |
| ci_low | float | 95% CI lower bound |
| ci_high | float | 95% CI upper bound |
| p_value | float | Two-sided p-value |
| treatment_size | int | Treatment group size |
| control_size | int | Control group size |
| meta_json | json | Additional metadata |

---

## References

- **Statistical methods**: Two-sample proportion test with normal approximation
- **Power analysis**: Based on standard sample size formulas for proportion tests
- **Deterministic assignment**: SHA256 hashing for stable randomization
- **Backend service**: `app/services_incrementality.py`
- **Frontend UI**: `frontend/src/modules/Incrementality.tsx`
- **Scheduler**: `app/scheduler.py`
