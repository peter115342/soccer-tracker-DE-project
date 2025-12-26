SELECT
    m.id AS match_id,
    m.utcDate AS utcDate,
    m.competition.id AS competition_code,
    m.competition.name AS competition_name,
    m.homeTeam.id AS home_team_id,
    m.homeTeam.name AS home_team_name,
    t.address AS home_team_address
FROM `{project_id}.sports_data_eu.matches_processed` AS m
JOIN `{project_id}.sports_data_eu.teams` AS t
ON m.homeTeam.id = t.id