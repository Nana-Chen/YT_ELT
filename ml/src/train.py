import argparse
import json
import os
from pathlib import Path
from datetime import datetime, timezone

import joblib
import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    from .features import FEATURE_COLUMNS, build_features, normalize_columns
except ImportError:
    from features import FEATURE_COLUMNS, build_features, normalize_columns


DEFAULT_QUERY = """
SELECT
    Video_ID,
    Video_Title,
    Upload_Date,
    Duration,
    Video_Type,
    Video_Views,
    Likes_Count,
    Comments_Count
FROM `{project_id}.core.video_monitor`
"""

DEFAULT_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "video-analytics-prod")
DEFAULT_METRICS_TABLE = f"{DEFAULT_PROJECT_ID}.mlops.model_metrics"


def load_training_data(source, query):
    if source.startswith("bigquery://"):
        project_id = source.replace("bigquery://", "", 1)
        client = bigquery.Client(project=project_id)
        return client.query(query).to_dataframe()

    with open(source, "r", encoding="utf-8") as raw_file:
        return pd.DataFrame(json.load(raw_file))


def train_model(df, random_state=42, run_id=None):
    df = normalize_columns(df)
    df = df.dropna(subset=["video_views", "upload_date", "duration"])

    x = build_features(df)
    y = pd.to_numeric(df["video_views"], errors="coerce").fillna(0)

    test_size = 0.2 if len(df) >= 20 else 0.1
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=test_size, random_state=random_state
    )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "regressor",
                RandomForestRegressor(
                    n_estimators=200,
                    min_samples_leaf=3,
                    random_state=random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    model.fit(x_train, y_train)
    predictions = model.predict(x_test)

    metrics = {
        "run_id": run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "model_name": "video_performance_views",
        "training_rows": int(len(df)),
        "features": FEATURE_COLUMNS,
        "mae": float(mean_absolute_error(y_test, predictions)),
        "r2": float(r2_score(y_test, predictions)),
    }
    return model, metrics


def append_metrics_history(metrics, output_path):
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as metrics_file:
        metrics_file.write(json.dumps(metrics) + "\n")


def write_metrics_to_bigquery(metrics, table_id):
    client = bigquery.Client(project=table_id.split(".")[0])
    rows = [
        {
            "run_id": metrics["run_id"],
            "trained_at": metrics["trained_at"],
            "model_name": metrics["model_name"],
            "training_rows": metrics["training_rows"],
            "mae": metrics["mae"],
            "r2": metrics["r2"],
            "features": ",".join(metrics["features"]),
        }
    ]
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Failed to write metrics to BigQuery: {errors}")


def main():
    parser = argparse.ArgumentParser(description="Train the YouTube video views model.")
    parser.add_argument(
        "--source",
        default="data/YT_data_2026-05-01.json",
        help="Local JSON file or bigquery://PROJECT_ID.",
    )
    parser.add_argument("--query", default=None)
    parser.add_argument("--model-output", default="ml/models/video_views_model.joblib")
    parser.add_argument("--metrics-output", default="ml/models/metrics.json")
    parser.add_argument("--metrics-history-output", default="ml/models/metrics_history.ndjson")
    parser.add_argument(
        "--write-metrics-bigquery",
        action="store_true",
        help="Append this training run's metrics to BigQuery.",
    )
    parser.add_argument("--metrics-table", default=DEFAULT_METRICS_TABLE)
    args = parser.parse_args()

    query = args.query
    if query is None:
        project_id = (
            args.source.replace("bigquery://", "", 1)
            if args.source.startswith("bigquery://")
            else DEFAULT_PROJECT_ID
        )
        query = DEFAULT_QUERY.format(project_id=project_id)

    df = load_training_data(args.source, query)
    model, metrics = train_model(df)

    model_output = Path(args.model_output)
    metrics_output = Path(args.metrics_output)
    model_output.parent.mkdir(parents=True, exist_ok=True)
    metrics_output.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(model, model_output)
    metrics_output.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    append_metrics_history(metrics, args.metrics_history_output)

    if args.write_metrics_bigquery:
        write_metrics_to_bigquery(metrics, args.metrics_table)

    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
