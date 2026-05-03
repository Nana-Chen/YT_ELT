CREATE SCHEMA IF NOT EXISTS `video-analytics-prod.mlops`;

CREATE TABLE IF NOT EXISTS `video-analytics-prod.mlops.model_metrics` (
    run_id STRING NOT NULL,
    trained_at TIMESTAMP NOT NULL,
    model_name STRING NOT NULL,
    training_rows INT64,
    mae FLOAT64,
    r2 FLOAT64,
    features STRING
);
