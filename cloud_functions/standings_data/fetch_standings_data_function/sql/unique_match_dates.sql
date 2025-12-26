SELECT DISTINCT DATE(utcDate) as match_date
FROM `{project_id}.sports_data_eu.matches_processed`
WHERE DATE(utcDate) <= CURRENT_DATE()
ORDER BY match_date DESC