# Alerts API

REST API for alert events and alert rules: list, filter, paginate, ack/snooze/resolve, and manage rules with RBAC.

## Base path

All endpoints are under `/api` (e.g. `GET /api/alerts`).

## RBAC

- **View**: Anyone can `GET /api/alerts` and `GET /api/alert-rules` (and detail by id).
- **Edit**: To ack/snooze/resolve alerts or create/update/delete rules, the request must carry:
  - `X-User-Id` (optional): user identifier; if omitted, `system` is used for audit.
  - `X-User-Role`: one of `admin` or `editor`. If missing or not in this set, mutation endpoints return `403 Forbidden`.

## Pagination and filtering

- **Alerts list**: `page` (1-based) and `per_page` (1–100, default 20). Response includes `items`, `total`, `page`, `per_page`.
- **Alert rules list**: No pagination; returns all matching rules (scope / is_enabled filters).

## Deep links

Alert detail and list items include a `deep_link` object for UI navigation:

- `entity_type`: `channel` | `campaign` | `pipeline` | `data_quality`
- `entity_id`: id of the entity (e.g. channel name, campaign id)
- `url`: path to open in the app (e.g. `/dashboard/attribution/performance?channel=...`)

---

## Endpoints

### Alerts (events)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/alerts` | List alerts (paginated, filterable) |
| GET | `/api/alerts/{id}` | Alert detail (context_json, related_entities, deep_link) |
| POST | `/api/alerts/{id}/ack` | Acknowledge (requires edit role) |
| POST | `/api/alerts/{id}/snooze` | Snooze with duration (requires edit role) |
| POST | `/api/alerts/{id}/resolve` | Manually resolve (requires edit role) |

### Alert rules

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/alert-rules` | List rules (optional scope, is_enabled) |
| GET | `/api/alert-rules/{id}` | Rule detail |
| POST | `/api/alert-rules` | Create rule (requires edit role; params_json validated) |
| PUT | `/api/alert-rules/{id}` | Update rule (requires edit role) |
| DELETE | `/api/alert-rules/{id}` | Disable (default) or delete rule (requires edit role) |

---

## Example requests

### List alerts (open only, page 1)

```bash
curl -s "http://localhost:8000/api/alerts?status=open&page=1&per_page=20"
```

### List alerts with filters

```bash
curl -s "http://localhost:8000/api/alerts?status=all&severity=critical&rule_type=anomaly_kpi&search=revenue&page=1&per_page=10"
```

### Get alert detail

```bash
curl -s "http://localhost:8000/api/alerts/1"
```

### Acknowledge alert (with RBAC headers)

```bash
curl -s -X POST "http://localhost:8000/api/alerts/1/ack" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: editor"
```

### Snooze alert for 60 minutes

```bash
curl -s -X POST "http://localhost:8000/api/alerts/1/snooze" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: admin" \
  -d '{"duration_minutes": 60}'
```

### Manually resolve alert

```bash
curl -s -X POST "http://localhost:8000/api/alerts/1/resolve" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: editor"
```

### List alert rules

```bash
curl -s "http://localhost:8000/api/alert-rules"
curl -s "http://localhost:8000/api/alert-rules?scope=workspace-1&is_enabled=true"
```

### Create alert rule

```bash
curl -s -X POST "http://localhost:8000/api/alert-rules" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: admin" \
  -d '{
    "name": "Revenue anomaly",
    "scope": "default",
    "severity": "warn",
    "rule_type": "anomaly_kpi",
    "schedule": "daily",
    "kpi_key": "revenue",
    "params_json": {"zscore_threshold": 2.5, "lookback_days": 14, "min_volume": 100}
  }'
```

### Update alert rule

```bash
curl -s -X PUT "http://localhost:8000/api/alert-rules/1" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: editor" \
  -d '{"is_enabled": false, "params_json": {"zscore_threshold": 3.0}}'
```

### Disable alert rule (default)

```bash
curl -s -X DELETE "http://localhost:8000/api/alert-rules/1?disable_only=true" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: admin"
```

### Delete alert rule permanently

```bash
curl -s -X DELETE "http://localhost:8000/api/alert-rules/1?disable_only=false" \
  -H "X-User-Id: user-123" \
  -H "X-User-Role: admin"
```

---

## Validation

- **params_json** (on create/update rule): Only allowed keys per `rule_type` are accepted; values must be string, number, boolean, or list of primitives. Unknown keys are stripped. Allowed keys by type:
  - `anomaly_kpi`: `zscore_threshold`, `lookback_days`, `min_volume`
  - `threshold`: `threshold_value`, `threshold_direction`, `lookback_days`, `min_volume`
  - `data_freshness`: `max_lag_hours`, `lookback_days`
  - `pipeline_health`: `max_failure_count`, `window_hours`
- **severity**: `info` | `warn` | `critical`
- **rule_type**: `anomaly_kpi` | `threshold` | `data_freshness` | `pipeline_health`
- **schedule**: `hourly` | `daily`
- **status** (query): `open` | `all`

## Audit fields

- **Alert rules**: `created_by` (required on create), `updated_by` (set on update/disable).
- **Alert events**: `updated_by` set when ack/snooze/resolve is called.
