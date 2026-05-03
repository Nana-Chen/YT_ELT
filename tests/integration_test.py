import os

import pytest
import requests


def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")

    if not api_key or not channel_handle:
        pytest.skip("AIRFLOW_VAR_API_KEY and AIRFLOW_VAR_CHANNEL_HANDLE are required.")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}")


def test_real_bigquery_connection(bigquery_project_id):
    pytest.importorskip("google.cloud.bigquery")

    if not bigquery_project_id or not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        pytest.skip("BIGQUERY_PROJECT_ID/GCP_PROJECT and GOOGLE_APPLICATION_CREDENTIALS are required.")

    from google.cloud import bigquery

    client = bigquery.Client(project=bigquery_project_id)
    rows = list(client.query("SELECT 1 AS ok").result())

    assert rows[0].ok == 1
