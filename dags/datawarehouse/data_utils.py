import os

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


TABLE = "yt_api"
DAILY_METRICS_TABLE = "yt_video_daily_metrics"
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GCP_PROJECT")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")


def get_bigquery_client():
    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BIGQUERY_LOCATION)
    return hook.get_client(project_id=BIGQUERY_PROJECT_ID, location=BIGQUERY_LOCATION)


def get_project_id(client):
    if BIGQUERY_PROJECT_ID:
        return BIGQUERY_PROJECT_ID
    if client.project:
        return client.project
    raise ValueError("Set BIGQUERY_PROJECT_ID or GCP_PROJECT for BigQuery tasks.")


def table_ref(client, dataset, table=TABLE):
    return f"`{get_project_id(client)}.{dataset}.{table}`"


def create_schema(client, schema):
    dataset_id = f"{get_project_id(client)}.{schema}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BIGQUERY_LOCATION
    client.create_dataset(dataset, exists_ok=True)


def create_table(client, schema):
    if schema == "staging":
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_ref(client, schema)} (
            Video_ID STRING NOT NULL,
            Video_Title STRING NOT NULL,
            Upload_Date TIMESTAMP NOT NULL,
            Duration STRING NOT NULL,
            Video_Views INT64,
            Likes_Count INT64,
            Comments_Count INT64
        )
        """
    else:
        table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_ref(client, schema)} (
            Video_ID STRING NOT NULL,
            Video_Title STRING NOT NULL,
            Upload_Date TIMESTAMP NOT NULL,
            Duration TIME NOT NULL,
            Video_Type STRING NOT NULL,
            Video_Views INT64,
            Likes_Count INT64,
            Comments_Count INT64
        )
        """

    client.query(table_sql).result()

    if schema == "core":
        daily_metrics_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_ref(client, schema, DAILY_METRICS_TABLE)} (
            Video_ID STRING NOT NULL,
            Snapshot_Date DATE NOT NULL,
            Video_Title STRING NOT NULL,
            Upload_Date TIMESTAMP NOT NULL,
            Duration TIME NOT NULL,
            Video_Type STRING NOT NULL,
            Video_Views INT64,
            Likes_Count INT64,
            Comments_Count INT64
        )
        """
        client.query(daily_metrics_sql).result()


def get_video_ids(client, schema):
    rows = client.query(f"SELECT Video_ID FROM {table_ref(client, schema)}").result()
    return [row.Video_ID for row in rows]
