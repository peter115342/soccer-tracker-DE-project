config {
  type: "table",
  schema: "sports_data_eu",
  name: "matches_processed",
  description: "Processed soccer matches data with normalized schema",
  bigquery: {
    partitionBy: "DATE(utcDate)"
  }
}

SELECT DISTINCT
  id,
  TIMESTAMP(utcDate) as utcDate,
  TIMESTAMP_ADD(TIMESTAMP(utcDate), INTERVAL 1 HOUR) as cetDate,
  status,
  matchday,
  stage,
  TIMESTAMP(lastUpdated) as lastUpdated,
  
  STRUCT(
    homeTeam.id as id,
    homeTeam.name as name
  ) as homeTeam,
  
  STRUCT(
    awayTeam.id as id,
    awayTeam.name as name
  ) as awayTeam,
  
  STRUCT(
    competition.id as id,
    competition.name as name
  ) as competition,
  
  STRUCT(
    score.list[OFFSET(0)].element.winner as winner,
    score.list[OFFSET(0)].element.duration as duration,
    STRUCT(
      score.list[OFFSET(0)].element.fullTime.home as homeTeam,
      score.list[OFFSET(0)].element.fullTime.away as awayTeam
    ) as fullTime,
    STRUCT(
      score.list[OFFSET(0)].element.halfTime.home as homeTeam,
      score.list[OFFSET(0)].element.halfTime.away as awayTeam
    ) as halfTime
  ) as score,

  ARRAY(
    SELECT STRUCT(
      element.id as id,
      element.name as name,
      element.type as type,
      element.nationality as nationality
    )
    FROM UNNEST(referees.list) ref
  ) as referees

FROM ${ref("matches_parquet")}
