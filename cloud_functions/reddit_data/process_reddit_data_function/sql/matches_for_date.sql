SELECT 
    CAST(id as STRING) as id,
    utcDate,
    homeTeam.name as home_team,
    awayTeam.name as away_team,
    score.fullTime.homeTeam as home_score,
    score.fullTime.awayTeam as away_score,
    competition.name as competition
FROM `sports_data_eu.matches_processed`
WHERE DATE(utcDate) = DATE(@date)
