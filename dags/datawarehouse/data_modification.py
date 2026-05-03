import logging
from datetime import date

from google.cloud import bigquery

from datawarehouse.data_utils import table_ref


logger = logging.getLogger(__name__)


def _int_or_none(value):
    return int(value) if value is not None else None


def _query(client, sql, params=None):
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=job_config).result()


def _staging_params(row):
    return [
        bigquery.ScalarQueryParameter("Video_ID", "STRING", row["video_id"]),
        bigquery.ScalarQueryParameter("Video_Title", "STRING", row["title"]),
        bigquery.ScalarQueryParameter("Upload_Date", "TIMESTAMP", row["publishedAt"]),
        bigquery.ScalarQueryParameter("Duration", "STRING", row["duration"]),
        bigquery.ScalarQueryParameter("Video_Views", "INT64", _int_or_none(row["viewCount"])),
        bigquery.ScalarQueryParameter("Likes_Count", "INT64", _int_or_none(row["likeCount"])),
        bigquery.ScalarQueryParameter(
            "Comments_Count", "INT64", _int_or_none(row["commentCount"])
        ),
    ]


def _core_params(row):
    return [
        bigquery.ScalarQueryParameter("Video_ID", "STRING", row["Video_ID"]),
        bigquery.ScalarQueryParameter("Video_Title", "STRING", row["Video_Title"]),
        bigquery.ScalarQueryParameter("Upload_Date", "TIMESTAMP", row["Upload_Date"]),
        bigquery.ScalarQueryParameter("Duration", "TIME", row["Duration"]),
        bigquery.ScalarQueryParameter("Video_Type", "STRING", row["Video_Type"]),
        bigquery.ScalarQueryParameter("Video_Views", "INT64", _int_or_none(row["Video_Views"])),
        bigquery.ScalarQueryParameter("Likes_Count", "INT64", _int_or_none(row["Likes_Count"])),
        bigquery.ScalarQueryParameter(
            "Comments_Count", "INT64", _int_or_none(row["Comments_Count"])
        ),
    ]


def upsert_row(client, schema, row):
    if schema == "staging":
        video_id = row["video_id"]
        params = _staging_params(row)
    else:
        video_id = row["Video_ID"]
        params = _core_params(row)

    sql = f"""
    MERGE {table_ref(client, schema)} AS target
    USING (
        SELECT
            @Video_ID AS Video_ID,
            @Video_Title AS Video_Title,
            @Upload_Date AS Upload_Date,
            @Duration AS Duration,
            {"@Video_Type AS Video_Type," if schema != "staging" else ""}
            @Video_Views AS Video_Views,
            @Likes_Count AS Likes_Count,
            @Comments_Count AS Comments_Count
    ) AS source
    ON target.Video_ID = source.Video_ID
    WHEN MATCHED THEN
        UPDATE SET
            Video_Title = source.Video_Title,
            Upload_Date = source.Upload_Date,
            Duration = source.Duration,
            {"Video_Type = source.Video_Type," if schema != "staging" else ""}
            Video_Views = source.Video_Views,
            Likes_Count = source.Likes_Count,
            Comments_Count = source.Comments_Count
    WHEN NOT MATCHED THEN
        INSERT (
            Video_ID,
            Video_Title,
            Upload_Date,
            Duration,
            {"Video_Type," if schema != "staging" else ""}
            Video_Views,
            Likes_Count,
            Comments_Count
        )
        VALUES (
            source.Video_ID,
            source.Video_Title,
            source.Upload_Date,
            source.Duration,
            {"source.Video_Type," if schema != "staging" else ""}
            source.Video_Views,
            source.Likes_Count,
            source.Comments_Count
        )
    """
    _query(client, sql, params)
    logger.info("Upserted row with Video_ID: %s", video_id)


def insert_rows(client, schema, row):
    upsert_row(client, schema, row)


def update_rows(client, schema, row):
    upsert_row(client, schema, row)


def delete_rows(client, schema, ids_to_delete):
    params = [
        bigquery.ArrayQueryParameter("video_ids", "STRING", list(ids_to_delete)),
    ]
    _query(
        client,
        f"""
        DELETE FROM {table_ref(client, schema)}
        WHERE Video_ID IN UNNEST(@video_ids)
        """,
        params,
    )
    logger.info("Deleted rows with Video_IDs: %s", ids_to_delete)


def upsert_daily_metric(client, row, snapshot_date=None):
    snapshot_date = snapshot_date or date.today()
    params = _core_params(row)
    params.append(bigquery.ScalarQueryParameter("Snapshot_Date", "DATE", snapshot_date))

    _query(
        client,
        f"""
        MERGE {table_ref(client, "core", "yt_video_daily_metrics")} AS target
        USING (
            SELECT
                @Video_ID AS Video_ID,
                @Snapshot_Date AS Snapshot_Date,
                @Video_Title AS Video_Title,
                @Upload_Date AS Upload_Date,
                @Duration AS Duration,
                @Video_Type AS Video_Type,
                @Video_Views AS Video_Views,
                @Likes_Count AS Likes_Count,
                @Comments_Count AS Comments_Count
        ) AS source
        ON target.Video_ID = source.Video_ID
            AND target.Snapshot_Date = source.Snapshot_Date
        WHEN MATCHED THEN
            UPDATE SET
                Video_Title = source.Video_Title,
                Upload_Date = source.Upload_Date,
                Duration = source.Duration,
                Video_Type = source.Video_Type,
                Video_Views = source.Video_Views,
                Likes_Count = source.Likes_Count,
                Comments_Count = source.Comments_Count
        WHEN NOT MATCHED THEN
            INSERT (
                Video_ID,
                Snapshot_Date,
                Video_Title,
                Upload_Date,
                Duration,
                Video_Type,
                Video_Views,
                Likes_Count,
                Comments_Count
            )
            VALUES (
                source.Video_ID,
                source.Snapshot_Date,
                source.Video_Title,
                source.Upload_Date,
                source.Duration,
                source.Video_Type,
                source.Video_Views,
                source.Likes_Count,
                source.Comments_Count
            )
        """,
        params,
    )
    logger.info("Upserted daily metric for Video_ID: %s", row["Video_ID"])
