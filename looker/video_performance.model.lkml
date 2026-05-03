connection: "bigquery"

include: "*.view.lkml"
include: "*.dashboard.lookml"

constant: bigquery_project {
  value: "video-analytics-prod"
}

explore: video_monitor {
  label: "Video Performance Monitor"
}
