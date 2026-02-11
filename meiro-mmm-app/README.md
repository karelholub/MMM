# Meiro MMM App

Unified platform for **Marketing Mix Modeling (MMM)** and **Multi-Touch Attribution** with integrated incrementality experiments.

## Features

- **MMM**: Bayesian media mix modeling with PyMC-Marketing (adstock, saturation, ROI, budget optimization)
- **Attribution**: Multi-touch attribution (last-touch, linear, time-decay, position-based, Markov)
- **Incrementality**: Automated holdout experiments with deterministic assignment, exposure tracking, and nightly reporting
- **Data Quality**: Versioned model configs, DQ snapshots, and alert rules
- **Meiro CDP Integration**: Fetch conversion paths and aggregate to MMM-ready datasets

## Quick Start

```bash
# Start services
docker compose up --build
```

- **Frontend**: http://localhost:5173
- **API**: http://localhost:8000/api/health
- **Docs**: http://localhost:8000/docs

## Documentation

- [MMM Data Contracts & Attribution Alignment](docs/DATA_CONTRACTS_AND_MMM_ALIGNMENT.md)
- [Incrementality Experiments Guide](docs/INCREMENTALITY_EXPERIMENTS.md)
- [Taxonomy & Data Quality Guide](docs/TAXONOMY_AND_DQ.md)
- [Taxonomy Quick Reference](docs/TAXONOMY_QUICK_REFERENCE.md)
- [Explainability Guide](docs/EXPLAINABILITY.md)
- [Explainability Quick Reference](docs/EXPLAINABILITY_QUICK_REFERENCE.md)

## Key Capabilities

### Marketing Mix Modeling

Upload weekly/daily spend + KPI data, configure channels and covariates, run Bayesian MMM:

```bash
POST /api/models
{
  "dataset_id": "weekly-sales",
  "kpi": "sales",
  "spend_channels": ["meta_spend", "google_spend", "tv_spend"],
  "covariates": ["price_index", "holiday"]
}
```

Get channel contributions, ROI, and budget optimization recommendations.

### Multi-Touch Attribution

Upload conversion paths (customer journeys), run attribution models:

```bash
POST /api/attribution/run?model=markov
```

Compare attribution vs. MMM ROI, analyze campaign performance.

### Incrementality Experiments

Create holdout tests for owned channels (email, push, SMS):

```bash
POST /api/experiments
{
  "name": "Email holdout Q1",
  "channel": "email",
  "start_at": "2026-03-01T00:00:00Z",
  "end_at": "2026-03-31T23:59:59Z"
}
```

Features:
- **Deterministic assignment**: Stable control/treatment groups via hashing
- **Exposure tracking**: Log when treatment is delivered
- **Power analysis**: Estimate required sample size
- **Nightly reporting**: Automated result computation and alerts
- **Time series**: Visualize cumulative metrics

### Taxonomy & Data Quality

Enhanced UTM validation, channel mapping, and quality monitoring:

```bash
# Validate UTMs before campaign launch
POST /api/taxonomy/validate-utm
{
  "utm_source": "google",
  "utm_medium": "cpc",
  "utm_campaign": "brand"
}

# Monitor unknown share
GET /api/taxonomy/unknown-share

# Channel confidence scores
GET /api/taxonomy/channel-confidence?channel=paid_social
```

Features:
- **UTM validation**: Comprehensive validation with normalization and error detection
- **Confidence scoring**: Per-touchpoint, per-journey, and per-channel quality metrics
- **Unknown share tracking**: Monitor unmapped traffic (target: < 10%)
- **Coverage analysis**: Measure taxonomy effectiveness
- **DQ alerts**: Automated alerts for taxonomy issues

### Scheduled Tasks

```bash
# Nightly experiment report
python -m app.scheduler --task nightly-report

# Or use cron (see backend/crontab.example)
0 2 * * * cd /path/to/backend && python -m app.scheduler --task nightly-report
```

## Architecture

- **Backend**: FastAPI + SQLAlchemy + PyMC-Marketing
- **Frontend**: React + Vite + TanStack Query + Recharts
- **Database**: SQLite (dev) / PostgreSQL (prod)
- **Scheduler**: APScheduler / cron for nightly jobs

## Development

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload

# Frontend
cd frontend
npm install
npm run dev
```

## Production Deployment

1. Set environment variables (see `backend/.env.example`)
2. Configure PostgreSQL connection
3. Run migrations: `alembic upgrade head`
4. Set up cron jobs (see `backend/crontab.example`)
5. Deploy with Docker Compose or Kubernetes

## Next Steps

- [ ] Integrate Meiro CDP real-time data sync
- [ ] Add email/Slack notifications for experiment alerts
- [ ] Implement budget optimizer with constraints
- [ ] Add A/B test meta-analysis across experiments
- [ ] Support multi-KPI MMM (revenue, profit, LTV)