from datawarehouse.data_utils import (
    get_bigquery_client,
    create_schema,
    create_table,
    get_video_ids,
    table_ref,
)


from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import (
    insert_rows,
    update_rows,
    delete_rows,
    upsert_daily_metric,
)
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)
table = "yt_api"


def _json_file_path_from_trigger():
    try:
        context = get_current_context()
    except Exception:
        return None

    dag_run = context.get("dag_run")
    if not dag_run or not dag_run.conf:
        return None
    return dag_run.conf.get("json_file_path")


@task
def staging_table():
    schema = "staging"

    try:
        client = get_bigquery_client()
        YT_data = load_data(_json_file_path_from_trigger())

        create_schema(client, schema)
        create_table(client, schema)

        table_ids = get_video_ids(client, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(client, schema, row)
            else:
                if row["video_id"] in table_ids:
                    update_rows(client, schema, row)
                else:
                    insert_rows(client, schema, row)

        ids_in_json = {row["video_id"] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(client, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e


@task
def core_table():

    schema = "core"

    try:
        client = get_bigquery_client()
        create_schema(client, schema)
        create_table(client, schema)

        table_ids = get_video_ids(client, schema)
        current_video_ids = set()

        rows = client.query(f"SELECT * FROM {table_ref(client, 'staging')}").result()

        for row in rows:
            row = dict(row.items())
            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(client, schema, transformed_row)
                upsert_daily_metric(client, transformed_row)

            #因为 core 表是空的，所以不需要判断这条数据存不存在直接转换，然后插入

            else:
                transformed_row = transform_data(row)
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(client, schema, transformed_row)
                    upsert_daily_metric(client, transformed_row)
                #已存在的旧视频更新，没有存在就插入
                else:
                    insert_rows(client, schema, transformed_row)
                    upsert_daily_metric(client, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(client, schema, ids_to_delete)

        logger.info(f"{schema} table update completed")

    except Exception as e:
        # Log any exceptions that occur
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
