CREATE OR REPLACE VIEW `video-analytics-prod.core.video_monitor` AS

WITH daily AS (
    SELECT
        Video_ID,
        Snapshot_Date,
        Video_Title,
        Upload_Date,
        Duration,
        Video_Type,
        Video_Views,
        Likes_Count,
        Comments_Count,
        Video_Views - LAG(Video_Views) OVER (
            PARTITION BY Video_ID ORDER BY Snapshot_Date
        ) AS Daily_View_Growth,
        Likes_Count - LAG(Likes_Count) OVER (
            PARTITION BY Video_ID ORDER BY Snapshot_Date
        ) AS Daily_Like_Growth,
        Comments_Count - LAG(Comments_Count) OVER (
            PARTITION BY Video_ID ORDER BY Snapshot_Date
        ) AS Daily_Comment_Growth
    FROM `video-analytics-prod.core.yt_video_daily_metrics`
)
SELECT
    *,
    SAFE_DIVIDE(Likes_Count, Video_Views) AS Like_Rate,
    SAFE_DIVIDE(Comments_Count, Video_Views) AS Comment_Rate
FROM daily;
