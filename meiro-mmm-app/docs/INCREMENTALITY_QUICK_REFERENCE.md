# Incrementality Experiments: Quick Reference

## Common Operations

### Create Experiment

```bash
curl -X POST http://localhost:8000/api/experiments \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Email holdout Q1",
    "channel": "email",
    "start_at": "2026-03-01T00:00:00Z",
    "end_at": "2026-03-31T23:59:59Z",
    "conversion_key": "purchase"
  }'
```

### Assign Profiles (Deterministic)

```bash
curl -X POST http://localhost:8000/api/experiments/1/assign \
  -H "Content-Type: application/json" \
  -d '{
    "profile_ids": ["user_001", "user_002", "user_003"],
    "treatment_rate": 0.8
  }'
```

### Record Exposures

```bash
curl -X POST http://localhost:8000/api/experiments/1/exposures \
  -H "Content-Type: application/json" \
  -d '{
    "exposures": [
      {
        "profile_id": "user_001",
        "exposure_ts": "2026-03-05T10:30:00Z",
        "campaign_id": "email_q1",
        "message_id": "msg_12345"
      }
    ]
  }'
```

### Record Outcomes

```bash
curl -X POST http://localhost:8000/api/experiments/1/outcomes \
  -H "Content-Type: application/json" \
  -d '{
    "outcomes": [
      {
        "profile_id": "user_001",
        "conversion_ts": "2026-03-07T14:22:00Z",
        "value": 120.50
      }
    ]
  }'
```

### Get Results

```bash
curl http://localhost:8000/api/experiments/1/results
```

### Get Time Series

```bash
curl http://localhost:8000/api/experiments/1/time-series?freq=D
```

### Power Analysis

```bash
curl -X POST http://localhost:8000/api/experiments/power-analysis \
  -H "Content-Type: application/json" \
  -d '{
    "baseline_rate": 0.05,
    "mde": 0.01,
    "alpha": 0.05,
    "power": 0.8
  }'
```

### Run Nightly Report

```bash
python -m app.scheduler --task nightly-report
```

---

## Python Examples

### Complete Workflow

```python
from datetime import datetime, timedelta
from app.db import get_db
from app.models_config_dq import Experiment
from app.services_incrementality import (
    assign_profiles_deterministic,
    record_exposures_batch,
    record_outcomes_batch,
    compute_experiment_results,
    estimate_sample_size,
)

db = next(get_db())

# 1. Power analysis
n = estimate_sample_size(baseline_rate=0.05, mde=0.01)
print(f"Need {n} profiles")

# 2. Create experiment
exp = Experiment(
    name="Email holdout Q1",
    channel="email",
    start_at=datetime(2026, 3, 1),
    end_at=datetime(2026, 3, 31),
    status="running",
    conversion_key="purchase",
)
db.add(exp)
db.commit()

# 3. Assign profiles
profile_ids = ["user_001", "user_002", "user_003"]
counts = assign_profiles_deterministic(
    db=db,
    experiment_id=exp.id,
    profile_ids=profile_ids,
    treatment_rate=0.8,
)
print(f"Assigned: {counts}")

# 4. Get treatment group
from app.models_config_dq import ExperimentAssignment
treatment = db.query(ExperimentAssignment).filter(
    ExperimentAssignment.experiment_id == exp.id,
    ExperimentAssignment.group == "treatment"
).all()
treatment_ids = [a.profile_id for a in treatment]

# 5. Send campaign to treatment (pseudocode)
for pid in treatment_ids:
    send_email(pid, campaign_id="email_q1")
    # Record exposure after sending
    record_exposures_batch(db, exp.id, [{
        "profile_id": pid,
        "exposure_ts": datetime.utcnow(),
        "campaign_id": "email_q1",
    }])

# 6. Record conversions (from webhook/batch job)
outcomes = [
    {"profile_id": "user_001", "conversion_ts": datetime.utcnow(), "value": 120.0},
]
record_outcomes_batch(db, exp.id, outcomes)

# 7. Compute results
results = compute_experiment_results(db, exp.id)
print(f"Uplift: {results['uplift_abs']:.4f} ({results['uplift_rel']:.2%})")
print(f"p-value: {results['p_value']:.4f}")
```

---

## Sample Size Table

| Baseline | MDE | Sample Size (α=0.05, power=0.8) |
|----------|-----|----------------------------------|
| 2%       | 0.5pp | 12,568 |
| 2%       | 1.0pp | 3,142 |
| 5%       | 0.5pp | 25,136 |
| 5%       | 1.0pp | 6,284 |
| 5%       | 2.0pp | 1,571 |
| 10%      | 1.0pp | 12,568 |
| 10%      | 2.0pp | 3,142 |
| 10%      | 5.0pp | 502 |

---

## Interpretation Guide

### Uplift Metrics

- **Absolute uplift**: Difference in conversion rates (pp)
  - Example: 5.5% - 4.0% = 1.5pp
- **Relative uplift**: Percentage increase
  - Example: 1.5pp / 4.0% = 37.5% increase
- **95% CI**: Range where true uplift likely falls
  - Example: [0.2pp, 2.8pp] means we're 95% confident the true uplift is between 0.2pp and 2.8pp
- **p-value**: Probability of observing this result if there's no true effect
  - p < 0.05 → statistically significant at 5% level

### Statistical Significance

| p-value | Interpretation |
|---------|----------------|
| < 0.01  | Very strong evidence of effect |
| 0.01-0.05 | Strong evidence (significant at 5% level) |
| 0.05-0.10 | Weak evidence (marginally significant) |
| > 0.10  | Insufficient evidence |

### Practical Significance

Even if statistically significant, check if the effect is **practically meaningful**:

- Is the uplift large enough to justify the cost?
- Does the CI include values that would change the decision?
- Is the sample size large enough to trust the estimate?

---

## Troubleshooting

### "Insufficient data"

**Check:**
```sql
-- Assignments
SELECT group, COUNT(*) FROM experiment_assignments 
WHERE experiment_id = 1 GROUP BY group;

-- Outcomes
SELECT COUNT(*) FROM experiment_outcomes 
WHERE experiment_id = 1;
```

**Need:** At least 10 profiles per group with some conversions.

### Assignments not stable

**Problem:** Using random assignment instead of deterministic.

**Fix:** Use `assign_profiles_deterministic()` instead of random shuffle.

### No exposures

**Problem:** Not calling `record_exposure()` after message delivery.

**Fix:** Add exposure tracking after sending:
```python
record_exposure(db, exp_id, profile_id, datetime.utcnow())
```

### Time series empty

**Check:**
```sql
SELECT MIN(assigned_at), MAX(assigned_at), COUNT(*) 
FROM experiment_assignments WHERE experiment_id = 1;
```

**Need:** At least one assignment with a timestamp.

---

## Cron Setup

```bash
# Edit crontab
crontab -e

# Add nightly report at 2 AM
0 2 * * * cd /path/to/backend && /usr/bin/python3 -m app.scheduler --task nightly-report >> /var/log/mmm-nightly.log 2>&1
```

---

## Monitoring

### Check Experiment Health

```python
from app.services_incrementality import get_experiment_time_series

df = get_experiment_time_series(db, experiment_id=1, freq="D")

# Check balance
print(df[["date", "treatment_n", "control_n"]].tail())

# Check conversion rates
print(df[["date", "treatment_rate", "control_rate", "uplift_abs"]].tail())

# Alert if imbalance
latest = df.iloc[-1]
if latest["treatment_n"] / latest["control_n"] > 1.2:
    print("WARNING: Treatment group is >20% larger than control")
```

### Check Alert Log

```bash
tail -f /var/log/mmm-nightly-report.log
```

---

## Best Practices Checklist

- [ ] Run power analysis before launching
- [ ] Use deterministic assignment for stable groups
- [ ] Track exposures separately from assignments
- [ ] Set realistic MDE (don't aim for tiny effects)
- [ ] Run long enough for conversions to materialize
- [ ] Monitor time series daily
- [ ] Check group balance regularly
- [ ] Document experiment hypothesis and decision criteria
- [ ] Set up nightly reporting cron job
- [ ] Archive completed experiments

---

## Resources

- [Full Guide](INCREMENTALITY_EXPERIMENTS.md)
- [API Docs](http://localhost:8000/docs)
- [Enhancements Summary](INCREMENTALITY_ENHANCEMENTS_SUMMARY.md)
