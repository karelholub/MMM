# Meiro MMM App (Prototype Scaffold)

Standalone web app to integrate with **Meiro CDP** and run **PyMC‑Marketing** MMM.
This scaffold boots a FastAPI backend and a Vite React front-end. The backend
currently returns placeholder attributions using Ridge to let the UI work immediately.

## Quick start (Dev)

```bash
# in project root
docker compose up --build
```

- Frontend: http://localhost:5173
- API:      http://localhost:8000/api/health

Click **Run model** to execute a mocked run over `sample-weekly-01.csv`.

## Next steps

- Replace `_fit_model` in `backend/app/main.py` with real PyMC‑Marketing pipeline
  (adstock, saturation, priors, MCMC), persist posterior/diagnostics to S3/Postgres.
- Add dataset upload wizard on the frontend, map columns (KPI, channels, covariates).
- Add Meiro connectors (S3/Redshift pull, write-back of insights as attributes/segments).