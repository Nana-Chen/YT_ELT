import re
from datetime import timezone

import pandas as pd


FEATURE_COLUMNS = [
    "duration_seconds",
    "video_age_days",
    "likes_count",
    "comments_count",
    "is_short",
]


def parse_duration_seconds(duration):
    if pd.isna(duration):
        return 0

    duration = str(duration)
    if re.match(r"^\d{1,2}:\d{2}:\d{2}$", duration):
        hours, minutes, seconds = [int(part) for part in duration.split(":")]
        return hours * 3600 + minutes * 60 + seconds

    values = {"D": 0, "H": 0, "M": 0, "S": 0}
    for value, unit in re.findall(r"(\d+)([DHMS])", duration.replace("P", "").replace("T", "")):
        values[unit] = int(value)

    return (
        values["D"] * 86400
        + values["H"] * 3600
        + values["M"] * 60
        + values["S"]
    )


def normalize_columns(df):
    return df.rename(
        columns={
            "Video_ID": "video_id",
            "Video_Title": "video_title",
            "Upload_Date": "upload_date",
            "Duration": "duration",
            "Video_Type": "video_type",
            "Video_Views": "video_views",
            "Likes_Count": "likes_count",
            "Comments_Count": "comments_count",
            "video_id": "video_id",
            "title": "video_title",
            "publishedAt": "upload_date",
            "duration": "duration",
            "viewCount": "video_views",
            "likeCount": "likes_count",
            "commentCount": "comments_count",
        }
    )


def build_features(df, now=None):
    df = normalize_columns(df.copy())
    now = pd.Timestamp(now or pd.Timestamp.utcnow())
    if now.tzinfo is None:
        now = now.tz_localize(timezone.utc)

    upload_date = pd.to_datetime(df["upload_date"], utc=True, errors="coerce")
    duration_seconds = df["duration"].map(parse_duration_seconds)

    features = pd.DataFrame(
        {
            "duration_seconds": duration_seconds,
            "video_age_days": (now - upload_date).dt.days.clip(lower=0).fillna(0),
            "likes_count": pd.to_numeric(df.get("likes_count"), errors="coerce").fillna(0),
            "comments_count": pd.to_numeric(df.get("comments_count"), errors="coerce").fillna(0),
            "is_short": (duration_seconds <= 60).astype(int),
        }
    )

    return features[FEATURE_COLUMNS]
