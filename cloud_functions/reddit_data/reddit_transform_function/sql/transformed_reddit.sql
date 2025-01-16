config {
  type: "table",
  schema: "sports_data_eu",
  name: "reddit_processed",
  description: "Processed reddit data with match threads and comments",
  bigquery: {
    partitionBy: "match_date"
  },
  tags: ["reddit"]
}

SELECT
  match_id,
  DATE(match_date) as match_date,
  home_team,
  away_team,
  competition,
  ARRAY(
    SELECT AS STRUCT 
      thread.element.thread_type,
      thread.element.thread_id,
      thread.element.title,
      thread.element.body,
      TIMESTAMP_SECONDS(thread.element.created_utc) as created_at,
      thread.element.score,
      thread.element.num_comments,
      ARRAY(
        SELECT AS STRUCT
          comment.element.id,
          comment.element.body,
          comment.element.score,
          comment.element.author,
          TIMESTAMP_SECONDS(comment.element.created_utc) as created_at
        FROM UNNEST(thread.element.comments.list) comment
      ) as comments
    FROM UNNEST(threads.list) thread
  ) as threads
FROM 
  `sports_data_raw_parquet.reddit_parquet`
