# Video Analytics Platform

Airflow pipeline for extracting YouTube video stats, loading them to BigQuery, running Soda data quality checks, and feeding a Looker/Looker Studio video monitoring dashboard.

## BigQuery setup

Create an Airflow Google Cloud connection named `google_cloud_default`, or set `GCP_CONN_ID` to another Airflow connection id.

Required environment variables:

- `API_KEY`: YouTube Data API key
- `AIRFLOW_VAR_CHANNEL_HANDLE`: YouTube channel handle, for example `brand_channel`
- `BIGQUERY_PROJECT_ID` or `GCP_PROJECT`: target Google Cloud project
- `BIGQUERY_LOCATION`: BigQuery location, defaults to `US`
- `GOOGLE_APPLICATION_CREDENTIALS`: service account JSON path when using Application Default Credentials

The DAG writes:

- `staging.yt_api`: raw YouTube API rows
- `core.yt_api`: transformed current-state rows
- `core.yt_video_daily_metrics`: one daily snapshot per video for monitoring trends

## Dashboard

Run `bigquery/video_monitor_view.sql` in BigQuery.

For Looker, use the LookML files in `looker/`.

For Looker Studio, connect to the BigQuery view `core.video_monitor` and follow `dashboards/looker_studio_video_monitor.md`.
