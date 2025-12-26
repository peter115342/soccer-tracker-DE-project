WITH LatestDates AS (
    SELECT 
        competitionId,
        MAX(fetchDate) as latest_date
    FROM `sports_data_eu.standings_processed`
    GROUP BY competitionId
)
SELECT s.*
FROM `sports_data_eu.standings_processed` s
INNER JOIN LatestDates l
    ON s.competitionId = l.competitionId
    AND s.fetchDate = l.latest_date
WHERE s.standingType = 'TOTAL'
