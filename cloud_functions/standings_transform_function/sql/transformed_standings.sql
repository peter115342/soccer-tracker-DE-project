config {
  type: "table",
  schema: "sports_data_eu",
  name: "standings_processed",
  description: "Processed soccer standings data with standings for all teams per competition",
  bigquery: {
    partitionBy: "fetchDate"
  },
  tags: ["standings"]
}

SELECT
  DATE(fetchDate) AS fetchDate,
  SAFE_CAST(competitionId AS INT64) AS competitionId,
  season.id AS seasonId,
  season.startDate AS seasonStartDate,
  season.endDate AS seasonEndDate,
  season.currentMatchday AS currentMatchday,
  season.winner AS seasonWinner,
  standingType,
  ARRAY_AGG(
    STRUCT(
      position,
      team.id AS teamId,
      team.name AS teamName,
      team.shortName AS teamShortName,
      team.tla AS teamTLA,
      team.crest AS teamCrest,
      playedGames,
      form,
      won,
      draw,
      lost,
      points,
      goalsFor,
      goalsAgainst,
      goalDifference
    )
    ORDER BY position
  ) AS standings
FROM
  `sports_data_raw_parquet.standings_parquet`
GROUP BY
  fetchDate,
  competitionId,
  seasonId,
  seasonStartDate,
  seasonEndDate,
  currentMatchday,
  seasonWinner,
  standingType
