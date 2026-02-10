-- Attribution quality snapshots for confidence indicators
CREATE TABLE IF NOT EXISTS attribution_quality_snapshots (
    id SERIAL PRIMARY KEY,
    ts_bucket TIMESTAMP NOT NULL,
    scope VARCHAR(32) NOT NULL,
    scope_id VARCHAR(255),
    conversion_key VARCHAR(64),
    confidence_score DOUBLE PRECISION NOT NULL,
    confidence_label VARCHAR(16) NOT NULL,
    components_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_aq_ts_bucket ON attribution_quality_snapshots(ts_bucket);
CREATE INDEX IF NOT EXISTS ix_aq_scope ON attribution_quality_snapshots(scope, scope_id);

-- Experiments for incrementality
CREATE TABLE IF NOT EXISTS experiments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    channel VARCHAR(64) NOT NULL,
    start_at TIMESTAMP NOT NULL,
    end_at TIMESTAMP NOT NULL,
    status VARCHAR(32) NOT NULL,
    config_id VARCHAR(36),
    config_version INTEGER,
    conversion_key VARCHAR(64),
    notes TEXT,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS experiment_assignments (
    id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    profile_id VARCHAR(128) NOT NULL,
    "group" VARCHAR(16) NOT NULL,
    assigned_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_exp_assign_experiment ON experiment_assignments(experiment_id);
CREATE INDEX IF NOT EXISTS ix_exp_assign_profile ON experiment_assignments(profile_id);

CREATE TABLE IF NOT EXISTS experiment_exposures (
    id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    profile_id VARCHAR(128) NOT NULL,
    exposure_ts TIMESTAMP NOT NULL,
    campaign_id VARCHAR(128),
    message_id VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS ix_exp_exposure_experiment ON experiment_exposures(experiment_id);

CREATE TABLE IF NOT EXISTS experiment_outcomes (
    id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiments(id),
    profile_id VARCHAR(128) NOT NULL,
    conversion_ts TIMESTAMP NOT NULL,
    value DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_exp_outcome_experiment ON experiment_outcomes(experiment_id);

CREATE TABLE IF NOT EXISTS experiment_results (
    id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL UNIQUE REFERENCES experiments(id),
    computed_at TIMESTAMP NOT NULL,
    uplift_abs DOUBLE PRECISION NOT NULL,
    uplift_rel DOUBLE PRECISION,
    ci_low DOUBLE PRECISION,
    ci_high DOUBLE PRECISION,
    p_value DOUBLE PRECISION,
    treatment_size INTEGER NOT NULL,
    control_size INTEGER NOT NULL,
    meta_json JSONB
);

-- Pathing, clustering, anomalies
CREATE TABLE IF NOT EXISTS conversion_paths (
    id SERIAL PRIMARY KEY,
    conversion_id VARCHAR(128) NOT NULL,
    profile_id VARCHAR(128) NOT NULL,
    conversion_key VARCHAR(64),
    conversion_ts TIMESTAMP NOT NULL,
    path_json JSONB NOT NULL,
    path_hash VARCHAR(64) NOT NULL,
    length INTEGER NOT NULL,
    first_touch_ts TIMESTAMP NOT NULL,
    last_touch_ts TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_conv_paths_conv ON conversion_paths(conversion_id);
CREATE INDEX IF NOT EXISTS ix_conv_paths_hash ON conversion_paths(path_hash);

CREATE TABLE IF NOT EXISTS path_aggregates (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    conversion_key VARCHAR(64),
    path_hash VARCHAR(64) NOT NULL,
    count INTEGER NOT NULL,
    avg_time_to_convert DOUBLE PRECISION,
    top_channels_json JSONB
);

CREATE INDEX IF NOT EXISTS ix_path_aggs_date ON path_aggregates(date);

CREATE TABLE IF NOT EXISTS path_clusters (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    conversion_key VARCHAR(64),
    cluster_id INTEGER NOT NULL,
    cluster_name VARCHAR(255),
    size INTEGER NOT NULL,
    centroid_json JSONB,
    top_paths_json JSONB
);

CREATE INDEX IF NOT EXISTS ix_path_clusters_date ON path_clusters(date);

CREATE TABLE IF NOT EXISTS path_cluster_membership (
    id SERIAL PRIMARY KEY,
    path_hash VARCHAR(64) NOT NULL,
    conversion_key VARCHAR(64),
    cluster_id INTEGER NOT NULL,
    date TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_path_membership_hash ON path_cluster_membership(path_hash);

CREATE TABLE IF NOT EXISTS path_anomalies (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP NOT NULL,
    conversion_key VARCHAR(64),
    anomaly_type VARCHAR(64) NOT NULL,
    metric_key VARCHAR(128) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    baseline_value DOUBLE PRECISION,
    severity VARCHAR(32) NOT NULL,
    details_json JSONB,
    status VARCHAR(32) NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_path_anomalies_date ON path_anomalies(date);

