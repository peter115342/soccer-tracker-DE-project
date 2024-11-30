config {
    type: "table",
    schema: "sports_data_eu",
    name: "matches_processed",
    description: "Processed soccer matches data with normalized schema",
    bigquery: {
        partitionBy: "DATE(utcDate)"
    }
}

SELECT
  DISTINCT id,
  TIMESTAMP(utcDate) AS utcDate,
  TIMESTAMP_ADD(TIMESTAMP(utcDate), INTERVAL 1 HOUR) AS cetDate,
  status,
  matchday,
  stage,
  TIMESTAMP(lastUpdated) AS lastUpdated,
  STRUCT( homeTeam.id AS id,
    homeTeam.name AS name ) AS homeTeam,
  STRUCT( awayTeam.id AS id,
    awayTeam.name AS name ) AS awayTeam,
  STRUCT( competition.id AS id,
    competition.name AS name ) AS competition,
  STRUCT( score.list[
  OFFSET
    (0)].element.winner AS winner,
    score.list[
  OFFSET
    (0)].element.duration AS duration,
    STRUCT( score.list[
    OFFSET
      (0)].element.fullTime.home AS homeTeam,
      score.list[
    OFFSET
      (0)].element.fullTime.away AS awayTeam ) AS fullTime,
    STRUCT( score.list[
    OFFSET
      (0)].element.halfTime.home AS homeTeam,
      score.list[
    OFFSET
      (0)].element.halfTime.away AS awayTeam ) AS halfTime ) AS score,
  ARRAY(
  SELECT
    STRUCT( element.id AS id,
      element.name AS name,
      element.type AS type,
      element.nationality AS nationality )
  FROM
    UNNEST(referees.list) ref ) AS referees
FROM
  `sports_data_raw_parquet.matches_parquet`
