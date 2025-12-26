WITH matches_counts AS (
    SELECT DATE(utcDate) as date, COUNT(*) as count
    FROM `sports_data_eu.matches_processed`
    WHERE utcDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY date
    ORDER BY date
),
weather_counts AS (
    SELECT DATE(timestamp) as date, COUNT(*) as count
    FROM `sports_data_eu.weather_processed`
    WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    GROUP BY date
    ORDER BY date
),
reddit_counts AS (
    SELECT match_date as date, COUNT(*) as count
    FROM `sports_data_eu.reddit_processed`
    WHERE match_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY date
    ORDER BY date
)
SELECT
    COALESCE(m.date, w.date, r.date) as date,
    m.count as matches_count,
    w.count as weather_count,
    r.count as reddit_count
FROM matches_counts m
FULL OUTER JOIN weather_counts w ON m.date = w.date
FULL OUTER JOIN reddit_counts r ON m.date = r.date
ORDER BY date
