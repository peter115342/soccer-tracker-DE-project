SELECT 
    id, 
    homeTeam.name as home_team_name,
    awayTeam.name as away_team_name,
    CASE 
        WHEN score.fullTime.homeTeam IS NOT NULL THEN score.fullTime.homeTeam
        ELSE -1
    END as home_score,
    CASE 
        WHEN score.fullTime.awayTeam IS NOT NULL THEN score.fullTime.awayTeam
        ELSE -1
    END as away_score,
    status,
    utcDate,
    lastUpdated,
    competition.id as competition_id
FROM `sports_data_eu.matches_processed`
WHERE utcDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
