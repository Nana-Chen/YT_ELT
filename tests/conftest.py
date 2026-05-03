import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag


@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="test_api_key"):
        yield Variable.get("API_KEY")


@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="brand_channel"):
        yield Variable.get("CHANNEL_HANDLE")


@pytest.fixture
def mock_bigquery_conn_vars():
    conn = Connection(
        conn_id="google_cloud_default",
        conn_type="google_cloud_platform",
        extra={"extra__google_cloud_platform__project": "video-analytics-prod"},
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="google_cloud_default")


@pytest.fixture()
def dagbag():
    yield DagBag()


@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable


@pytest.fixture
def bigquery_project_id():
    return os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GCP_PROJECT")
