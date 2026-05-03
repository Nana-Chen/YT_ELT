- dashboard: video_performance_monitor
  title: Video Performance Monitor
  layout: newspaper
  preferred_viewer: dashboards-next
  filters:
  - name: Video ID
    title: Video ID
    type: field_filter
    default_value: ""
    allow_multiple_values: false
    required: true
    ui_config:
      type: advanced
      display: popover
    model: video_performance
    explore: video_monitor
    field: video_monitor.video_id
  elements:
  - title: Latest Views
    name: latest_views
    type: single_value
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.latest_views]
    listen:
      Video ID: video_monitor.video_id
    row: 0
    col: 0
    width: 8
    height: 4
  - title: Latest Likes
    name: latest_likes
    type: single_value
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.latest_likes]
    listen:
      Video ID: video_monitor.video_id
    row: 0
    col: 8
    width: 8
    height: 4
  - title: Latest Comments
    name: latest_comments
    type: single_value
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.latest_comments]
    listen:
      Video ID: video_monitor.video_id
    row: 0
    col: 16
    width: 8
    height: 4
  - title: Views Over Time
    name: views_over_time
    type: looker_line
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.snapshot_date, video_monitor.latest_views]
    sorts: [video_monitor.snapshot_date]
    listen:
      Video ID: video_monitor.video_id
    row: 4
    col: 0
    width: 12
    height: 7
  - title: Daily View Growth
    name: daily_view_growth
    type: looker_column
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.snapshot_date, video_monitor.total_daily_view_growth]
    sorts: [video_monitor.snapshot_date]
    listen:
      Video ID: video_monitor.video_id
    row: 4
    col: 12
    width: 12
    height: 7
  - title: Engagement
    name: engagement
    type: looker_line
    model: video_performance
    explore: video_monitor
    fields: [video_monitor.snapshot_date, video_monitor.latest_likes, video_monitor.latest_comments]
    sorts: [video_monitor.snapshot_date]
    listen:
      Video ID: video_monitor.video_id
    row: 11
    col: 0
    width: 24
    height: 7
