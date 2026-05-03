view: video_monitor {
  sql_table_name: `@{bigquery_project}.core.video_monitor` ;;

  dimension: snapshot_key {
    primary_key: yes
    hidden: yes
    type: string
    sql: CONCAT(${TABLE}.Video_ID, '-', CAST(${TABLE}.Snapshot_Date AS STRING)) ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.Video_ID ;;
  }

  dimension: video_title {
    type: string
    sql: ${TABLE}.Video_Title ;;
  }

  dimension_group: snapshot {
    type: time
    timeframes: [raw, date, week, month]
    sql: ${TABLE}.Snapshot_Date ;;
  }

  dimension_group: upload {
    type: time
    timeframes: [raw, date, week, month, year]
    sql: ${TABLE}.Upload_Date ;;
  }

  dimension: video_type {
    type: string
    sql: ${TABLE}.Video_Type ;;
  }

  measure: latest_views {
    type: max
    sql: ${TABLE}.Video_Views ;;
    value_format_name: decimal_0
  }

  measure: latest_likes {
    type: max
    sql: ${TABLE}.Likes_Count ;;
    value_format_name: decimal_0
  }

  measure: latest_comments {
    type: max
    sql: ${TABLE}.Comments_Count ;;
    value_format_name: decimal_0
  }

  measure: total_daily_view_growth {
    type: sum
    sql: ${TABLE}.Daily_View_Growth ;;
    value_format_name: decimal_0
  }

  measure: average_like_rate {
    type: average
    sql: ${TABLE}.Like_Rate ;;
    value_format_name: percent_2
  }

  measure: average_comment_rate {
    type: average
    sql: ${TABLE}.Comment_Rate ;;
    value_format_name: percent_2
  }
}
