def test_api_key(api_key):
    assert api_key == "test_api_key"


def test_channel_handle(channel_handle):
    assert channel_handle == "brand_channel"


def test_bigquery_conn(mock_bigquery_conn_vars):
    conn = mock_bigquery_conn_vars
    assert conn.conn_id == "google_cloud_default"
    assert conn.conn_type == "google_cloud_platform"
    assert conn.extra_dejson["extra__google_cloud_platform__project"] == "video-analytics-prod"


def test_dags_integrity(dagbag):
    # 1.
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("===========")
    print(dagbag.import_errors)

    # 2.
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("===========")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."

    # 3.
    assert dagbag.size() == 3
    print("===========")
    print(dagbag.size())

    # 4.
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }
    print("===========")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))
