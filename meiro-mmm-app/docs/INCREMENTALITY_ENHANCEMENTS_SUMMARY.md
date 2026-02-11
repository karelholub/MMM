# Incrementality Experiments: Enhancements Summary

This document summarizes the mature incrementality experiment features added to the Meiro MMM application.

---

## What Was Added

### 1. Backend Service (`app/services_incrementality.py`)

**Core Functions:**

- **`deterministic_assignment()`**: Hash-based stable assignment (treatment/control)
- **`assign_profiles_deterministic()`**: Batch assign profiles with deterministic hashing
- **`record_exposure()`** / **`record_exposures_batch()`**: Track when treatment is delivered
- **`record_outcome()`** / **`record_outcomes_batch()`**: Track conversions
- **`compute_experiment_results()`**: Calculate uplift with confidence intervals
- **`estimate_sample_size()`**: Power analysis for sample size estimation
- **`run_nightly_report()`**: Automated reporting for all running experiments
- **`get_experiment_time_series()`**: Daily/weekly cumulative metrics
- **`auto_assign_from_conversion_paths()`**: Post-hoc assignment from historical data

**Key Improvements:**

✅ **Deterministic assignment** replaces random shuffle — same profile always gets same group
✅ **Exposure tracking** separates assignment from actual delivery
✅ **Power analysis** helps plan experiments before launch
✅ **Time series** enables monitoring experiment health over time
✅ **Automated reporting** with alert generation

---

### 2. API Endpoints (Enhanced `app/main.py`)

**New Endpoints:**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/experiments/{id}/exposures` | POST | Record exposures (batch) |
| `/api/experiments/{id}/exposures` | GET | Get recent exposures |
| `/api/experiments/{id}/time-series` | GET | Get daily/weekly metrics |
| `/api/experiments/power-analysis` | POST | Estimate sample size |
| `/api/experiments/nightly-report` | POST | Run nightly report manually |
| `/api/experiments/{id}/auto-assign` | POST | Auto-assign from conversion paths |

**Updated Endpoints:**

- `/api/experiments/{id}/assign` — Now uses deterministic assignment

---

### 3. Frontend UI (`frontend/src/modules/Incrementality.tsx`)

**New Features:**

✅ **Power calculator**: Interactive form to estimate required sample size
✅ **Time series chart**: Visualize cumulative conversion rates and sample sizes over time
✅ **Improved UX**: Toggle visibility for power calc and time series

**UI Components:**

- Power analysis form with inputs for baseline rate, MDE, alpha, power
- Time series chart with dual Y-axes (conversion rate + sample size)
- Responsive layout with collapsible sections

---

### 4. Scheduler (`app/scheduler.py`)

**Features:**

✅ **CLI entry point**: Run tasks via `python -m app.scheduler --task nightly-report`
✅ **APScheduler integration**: In-process scheduling for development
✅ **Alert logging**: Logs experiment alerts by severity
✅ **Notification hooks**: Placeholder for email/Slack/webhook notifications

**Usage:**

```bash
# Manual run
python -m app.scheduler --task nightly-report

# Cron (production)
0 2 * * * cd /path/to/backend && python -m app.scheduler --task nightly-report
```

---

### 5. Documentation

**New Docs:**

- **`docs/INCREMENTALITY_EXPERIMENTS.md`**: Comprehensive guide covering:
  - API reference with examples
  - Best practices (power analysis, deterministic assignment, exposure tracking)
  - Integration examples (Meiro CDP, webhooks)
  - Troubleshooting
  - Database schema reference

- **`docs/DATA_CONTRACTS_AND_MMM_ALIGNMENT.md`**: Clarifies data contracts for MMM and attribution

- **`backend/crontab.example`**: Example cron configuration

**Updated Docs:**

- **`README.md`**: Added incrementality features overview

---

## Key Technical Decisions

### 1. Deterministic Assignment

**Why:** Ensures stable control/treatment groups across calls, no risk of re-randomization.

**How:** `SHA256(profile_id:experiment_id:salt)` → map to [0, 1) → compare to treatment_rate

**Benefits:**
- Same profile always gets same group
- Can assign incrementally as new profiles arrive
- No need to store assignments before exposure (lazy assignment)

### 2. Exposure Tracking

**Why:** Assignment ≠ delivery. Track when treatment is actually sent.

**Use case:** Profile assigned to treatment, but email bounces → no exposure recorded.

**Impact:** More accurate uplift measurement (intent-to-treat vs. per-protocol analysis).

### 3. Power Analysis

**Why:** Avoid underpowered experiments that can't detect meaningful effects.

**Formula:** Two-sample proportion test with normal approximation.

**Output:** Total sample size needed for given baseline rate, MDE, alpha, power.

### 4. Time Series

**Why:** Monitor experiment health daily, catch issues early.

**Data:** Cumulative metrics (assignments, conversions, rates) by date.

**Visualization:** Dual Y-axis chart (conversion rate + sample size).

### 5. Nightly Reporting

**Why:** Automate result computation, reduce manual work.

**Alerts:**
- `info`: Insufficient data
- `success`: Significant positive uplift
- `warning`: Significant negative uplift
- `error`: Computation failure

**Future:** Email/Slack notifications, dashboard integration.

---

## Migration Guide

### For Existing Experiments

**Before:**
```python
# Random assignment (not stable)
import random
random.shuffle(profile_ids)
n_treat = int(len(profile_ids) * 0.5)
treatment = profile_ids[:n_treat]
control = profile_ids[n_treat:]
```

**After:**
```python
# Deterministic assignment (stable)
from app.services_incrementality import assign_profiles_deterministic

counts = assign_profiles_deterministic(
    db=db,
    experiment_id=1,
    profile_ids=profile_ids,
    treatment_rate=0.5,
)
```

### For New Experiments

**Workflow:**

1. **Power analysis** → Estimate sample size
2. **Create experiment** → Set dates, channel, conversion key
3. **Assign profiles** → Use deterministic assignment
4. **Track exposures** → Log when treatment is delivered
5. **Record outcomes** → Log conversions
6. **Monitor time series** → Check daily metrics
7. **Review nightly report** → Check for alerts

---

## Performance Considerations

### Database Indexes

Ensure indexes exist on:
- `experiment_assignments(experiment_id, profile_id)`
- `experiment_exposures(experiment_id, profile_id)`
- `experiment_outcomes(experiment_id, profile_id)`
- `experiment_results(experiment_id)` (unique)

### Batch Operations

Use batch endpoints for large-scale operations:
- `record_exposures_batch()` instead of individual `record_exposure()` calls
- `record_outcomes_batch()` instead of individual `record_outcome()` calls

### Time Series Caching

For large experiments, consider caching time series results:
```python
# Cache daily time series for 1 hour
@cache(ttl=3600)
def get_experiment_time_series_cached(experiment_id):
    return get_experiment_time_series(db, experiment_id, freq="D")
```

---

## Testing

### Unit Tests

```python
# Test deterministic assignment
def test_deterministic_assignment():
    group1 = deterministic_assignment("user_001", 1, 0.5, "")
    group2 = deterministic_assignment("user_001", 1, 0.5, "")
    assert group1 == group2  # Same profile → same group

# Test power analysis
def test_power_analysis():
    n = estimate_sample_size(baseline_rate=0.05, mde=0.01)
    assert n > 0
    assert n == estimate_sample_size(baseline_rate=0.05, mde=0.01)  # Deterministic
```

### Integration Tests

```python
# Test full experiment workflow
def test_experiment_workflow(db):
    # Create experiment
    exp = Experiment(name="Test", channel="email", ...)
    db.add(exp)
    db.commit()
    
    # Assign profiles
    counts = assign_profiles_deterministic(db, exp.id, ["u1", "u2", "u3"], 0.5)
    assert counts["treatment"] + counts["control"] == 3
    
    # Record exposures
    record_exposure(db, exp.id, "u1", datetime.utcnow())
    
    # Record outcomes
    record_outcome(db, exp.id, "u1", datetime.utcnow(), 100.0)
    
    # Compute results
    results = compute_experiment_results(db, exp.id)
    assert results["experiment_id"] == exp.id
```

---

## Future Enhancements

### Short Term

- [ ] Email/Slack notifications for nightly alerts
- [ ] Experiment dashboard with all active experiments
- [ ] Export results to CSV/PDF
- [ ] Multi-armed bandit allocation (Thompson sampling)

### Medium Term

- [ ] Sequential testing (early stopping)
- [ ] Stratified randomization (by segment)
- [ ] Covariate adjustment (CUPED)
- [ ] Meta-analysis across experiments

### Long Term

- [ ] Bayesian A/B testing
- [ ] Causal inference with observational data
- [ ] Synthetic control methods
- [ ] Integration with experimentation platforms (Optimizely, LaunchDarkly)

---

## Support

For questions or issues:
1. Check the [Incrementality Experiments Guide](INCREMENTALITY_EXPERIMENTS.md)
2. Review API docs at http://localhost:8000/docs
3. Check logs: `tail -f /var/log/mmm-nightly-report.log`
4. Open an issue on GitHub

---

## Changelog

### v0.3.0 (2026-02-11)

**Added:**
- Deterministic assignment with hash-based randomization
- Exposure tracking API and database tables
- Power analysis endpoint
- Time series visualization
- Nightly reporting scheduler
- Auto-assignment from conversion paths
- Comprehensive documentation

**Changed:**
- Assignment endpoint now uses deterministic hashing (breaking change for existing experiments)

**Fixed:**
- Experiment results now handle insufficient data gracefully
- Time series handles missing timestamps correctly

---

## License

Same as parent project.
