SELECT 
    m.id,
    m.utcDate,
    m.status,
    m.homeTeam.id as home_team_id,
    m.homeTeam.name as home_team,
    m.awayTeam.id as away_team_id,
    m.awayTeam.name as away_team,
    m.score.fullTime.homeTeam as home_score,
    m.score.fullTime.awayTeam as away_score,
    w.apparent_temperature,
    w.weathercode,
    w.temperature_2m,
    w.precipitation,
    w.windspeed_10m,
    w.cloudcover,
    w.relativehumidity_2m,
    t.address,
    t.venue,
    t.logo as home_team_logo,
    CAST(SPLIT(t.address, ',')[OFFSET(0)] AS FLOAT64) as lat,
    CAST(SPLIT(t.address, ',')[OFFSET(1)] AS FLOAT64) as lon,
    t2.logo as away_team_logo,
    r.threads,
    l.id as league_id,
    l.name as league_name,
    l.logo as league_logo
FROM `sports_data_eu.matches_processed` m
LEFT JOIN `sports_data_eu.weather_processed` w
    ON m.id = w.match_id
LEFT JOIN `sports_data_eu.teams` t
    ON m.homeTeam.id = t.id
LEFT JOIN `sports_data_eu.teams` t2
    ON m.awayTeam.id = t2.id
LEFT JOIN `sports_data_eu.leagues` l
    ON t.league_id = l.id
LEFT JOIN `sports_data_eu.reddit_processed` r
    ON CAST(m.id AS STRING) = r.match_id
