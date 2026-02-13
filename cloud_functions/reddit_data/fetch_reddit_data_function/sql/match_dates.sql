SELECT DISTINCT DATE(utcDate) as match_date
FROM `sports_data_eu.matches_processed`
ORDER BY match_date DESC
