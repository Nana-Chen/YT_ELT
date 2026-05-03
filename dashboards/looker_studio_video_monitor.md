# Looker Studio Video Performance Dashboard

Data source: BigQuery table or view `core.video_monitor`.

Use the SQL in `bigquery/video_monitor_view.sql` first. Then connect Looker Studio to the resulting BigQuery view.

Dashboard filter:

- Control type: Drop-down list
- Field: `Video_ID`
- Default value: the YouTube video id you want to monitor

Recommended charts:

- Scorecard: latest `Video_Views`
- Scorecard: latest `Likes_Count`
- Scorecard: latest `Comments_Count`
- Time series: `Snapshot_Date` by `Video_Views`
- Time series: `Snapshot_Date` by `Daily_View_Growth`
- Time series: `Snapshot_Date` by `Likes_Count` and `Comments_Count`
- Table: `Snapshot_Date`, `Video_Title`, `Video_Views`, `Daily_View_Growth`, `Likes_Count`, `Comments_Count`, `Like_Rate`, `Comment_Rate`

Suggested calculated fields in Looker Studio:

```text
Latest Views
MAX(Video_Views)
```

```text
Latest Likes
MAX(Likes_Count)
```

```text
Latest Comments
MAX(Comments_Count)
```

```text
Engagement Rate
(SUM(Likes_Count) + SUM(Comments_Count)) / SUM(Video_Views)
```

The dashboard becomes useful after the DAG runs on more than one day, because `core.yt_video_daily_metrics` stores one snapshot per video per day.

## Model Metrics Dashboard

Data source: BigQuery table `mlops.model_metrics`.

Run `bigquery/ml_model_metrics.sql` first, then train with:

```bash
BIGQUERY_PROJECT_ID=video-analytics-prod ml/.venv/bin/python ml/src/train.py \
  --source bigquery://video-analytics-prod \
  --write-metrics-bigquery
```

Recommended charts:

- Scorecard: latest `r2`
- Scorecard: latest `mae`
- Time series: `trained_at` by `r2`
- Time series: `trained_at` by `mae`
- Table: `trained_at`, `model_name`, `training_rows`, `r2`, `mae`, `features`
