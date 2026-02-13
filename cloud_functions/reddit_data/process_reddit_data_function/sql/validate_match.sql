SELECT 
    CAST(id as STRING) as id,
    DATE(utcDate) as match_date,
    homeTeam.name as home_team,
    awayTeam.name as away_team
FROM `sports_data_eu.matches_processed`
WHERE id = @match_id
