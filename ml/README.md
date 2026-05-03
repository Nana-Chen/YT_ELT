# Video Performance ML Deployment

This is a small MLOps-style deployment for predicting expected YouTube views from video metadata and engagement signals.

## 1. Train

Local JSON:

```bash
python3 -m venv ml/.venv
ml/.venv/bin/pip install -r ml/requirements.txt
ml/.venv/bin/python ml/src/train.py --source data/YT_data_2026-05-01.json
```

BigQuery:

```bash
BIGQUERY_PROJECT_ID=video-analytics-prod ml/.venv/bin/python ml/src/train.py --source bigquery://video-analytics-prod
```

Artifacts:

- `ml/models/video_views_model.joblib`
- `ml/models/metrics.json`
- `ml/models/metrics_history.ndjson`

To send training metrics to BigQuery for Looker/Looker Studio:

```bash
bq query --use_legacy_sql=false < bigquery/ml_model_metrics.sql
BIGQUERY_PROJECT_ID=video-analytics-prod ml/.venv/bin/python ml/src/train.py \
  --source bigquery://video-analytics-prod \
  --write-metrics-bigquery
```

## 2. Run Locally

```bash
ml/.venv/bin/uvicorn ml.src.app:app --host 0.0.0.0 --port 8080
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

Prediction:

```bash
curl -X POST http://127.0.0.1:8080/predict \
  -H "Content-Type: application/json" \
  -d '{
    "upload_date": "2026-05-01T00:00:00Z",
    "duration": "PT8M20S",
    "likes_count": 1000,
    "comments_count": 120
  }'
```

## 3. Docker Deploy

```bash
docker compose -f ml/deploy/docker-compose.yml up --build
```

## 4. Ops Checklist

- Train job writes versioned artifacts: model file and metrics file.
- API exposes `/health`, `/metrics`, and `/predict`.
- Docker image is isolated from the Airflow image.
- Next production step: push the image to Artifact Registry and deploy to Cloud Run.
- Monitoring idea: log prediction input ranges and compare predicted views with actual BigQuery views daily.
