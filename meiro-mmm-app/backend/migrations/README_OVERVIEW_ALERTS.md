# Overview snapshots and alerts (migration 004)

Tables introduced in `004_overview_snapshots_and_alerts.sql` and ORM in `app.models_overview_alerts`.

## Tables

| Table | Purpose |
|-------|--------|
| **metric_snapshots** | Hourly or daily KPI snapshots for Overview dashboards. Stores `ts`, `scope` (account/workspace), `kpi_key`, `kpi_value`, optional `dimensions_json`, and `computed_from` (raw vs model). Optional for MVP; schema exists for future use. |
| **alert_rules** | Rule definitions: name, scope, severity (info/warn/critical), `rule_type` (anomaly_kpi, threshold, data_freshness, pipeline_health), optional `kpi_key` and `dimension_filters_json`, `params_json` (zscore_threshold, lookback_days, min_volume, threshold_value, etc.), schedule (hourly/daily), created_by, timestamps. Rules are separate from events. |
| **alert_events** | Fired events from rules. Each row is one detection: `rule_id`, `ts_detected`, severity, title, message, `context_json` (observed/expected/zscore/dims), `status` (open/ack/snoozed/resolved), optional assignee, `related_entities_json` (campaign_id, channel, pipeline, etc.). Clear separation: rules define *what* to check; events record *when* it fired. |
| **notification_channels** | Delivery channels for alerts: type (email, slack_webhook), `config_json` (webhook URL, email list ref), `is_verified`. |
| **user_notification_prefs** | Per-user, per-channel preferences: `user_id`, `channel_id`, `severities_json`, `digest_mode` (realtime/daily), `quiet_hours_json`, `is_enabled`. One row per (user_id, channel_id). |

## Indexes

- `metric_snapshots`: `(ts, kpi_key)`, `scope`
- `alert_rules`: `(scope, is_enabled)`
- `alert_events`: `ts_detected`, `status`, `rule_id`
- `user_notification_prefs`: `user_id`, `channel_id`

## Backwards compatibility

Migration only adds new tables; no changes to existing tables. Safe to run on existing DBs.
