WITH matches_with_data AS (
  SELECT 
    m.id,
    m.utcDate,
    m.status,
    m.matchday,
    m.stage,
    m.homeTeam,
    m.awayTeam,
    m.competition,
    m.score,
    r.threads,
    w.temperature_2m,
    w.precipitation,
    w.weathercode,
    w.windspeed_10m
  FROM `sports_data_eu.matches_processed` AS m
  JOIN `sports_data_eu.reddit_processed` AS r
    ON CAST(m.id AS STRING) = r.match_id
  LEFT JOIN `sports_data_eu.weather_processed` w 
    ON m.id = w.match_id
),
team_standings AS (
  SELECT 
    s.fetchDate,
    s.competitionId,
    st.teamId,
    st.form,
    st.position
  FROM `sports_data_eu.standings_processed` s,
    UNNEST(standings) st
  WHERE s.standingType = 'TOTAL'
)
SELECT 
  DATE(m.utcDate) AS match_date,
  m.competition.name AS league,
  ARRAY_AGG(STRUCT(
    m.id,
    m.status,
    m.matchday,
    m.stage,
    m.homeTeam,
    m.awayTeam,
    m.score,
    m.threads,
    m.temperature_2m,
    m.precipitation,
    m.weathercode,
    m.windspeed_10m,
    home_form.form as home_team_form,
    away_form.form as away_team_form
  )) AS matches
FROM matches_with_data m
LEFT JOIN (
  SELECT teamId, form, fetchDate
  FROM team_standings ts
  WHERE fetchDate = (
    SELECT MAX(fetchDate)
    FROM team_standings
    WHERE fetchDate < ts.fetchDate
  )
) home_form
  ON m.homeTeam.id = home_form.teamId 
  AND DATE(m.utcDate) > DATE(home_form.fetchDate)
LEFT JOIN (
  SELECT teamId, form, fetchDate
  FROM team_standings ts
  WHERE fetchDate = (
    SELECT MAX(fetchDate)
    FROM team_standings
    WHERE fetchDate < ts.fetchDate
  )
) away_form
  ON m.awayTeam.id = away_form.teamId
  AND DATE(m.utcDate) > DATE(away_form.fetchDate)
GROUP BY match_date, league
ORDER BY match_date, league