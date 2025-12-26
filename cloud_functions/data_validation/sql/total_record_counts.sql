-- Generate a date range covering all dates from the earliest data point to today
WITH all_dates AS (
    SELECT
        GENERATE_DATE_ARRAY(
            (SELECT MIN(DATE(utcDate)) FROM `sports_data_eu.matches_processed`),
            CURRENT_DATE()
        ) AS date_array
),
dates AS (
    SELECT date
    FROM UNNEST((SELECT date_array FROM all_dates)) AS date
),
matches_daily_counts AS (
    SELECT
        DATE(utcDate) AS date,
        COUNT(*) AS daily_count
    FROM `sports_data_eu.matches_processed`
    GROUP BY date
),
matches_cumulative AS (
    SELECT
        d.date,
        SUM(m.daily_count) OVER (ORDER BY d.date) AS cumulative_count
    FROM dates d
    LEFT JOIN matches_daily_counts m ON d.date = m.date
    WHERE d.date <= CURRENT_DATE()
),
weather_daily_counts AS (
    SELECT
        DATE(timestamp) AS date,
        COUNT(*) AS daily_count
    FROM `sports_data_eu.weather_processed`
    GROUP BY date
),
weather_cumulative AS (
    SELECT
        d.date,
        SUM(w.daily_count) OVER (ORDER BY d.date) AS cumulative_count
    FROM dates d
    LEFT JOIN weather_daily_counts w ON d.date = w.date
    WHERE d.date <= CURRENT_DATE()
),
reddit_daily_counts AS (
    SELECT
        match_date AS date,
        COUNT(*) AS daily_count
    FROM `sports_data_eu.reddit_processed`
    GROUP BY date
),
reddit_cumulative AS (
    SELECT
        d.date,
        SUM(r.daily_count) OVER (ORDER BY d.date) AS cumulative_count
    FROM dates d
    LEFT JOIN reddit_daily_counts r ON d.date = r.date
    WHERE d.date <= CURRENT_DATE()
),
-- Combine cumulative counts
combined_counts AS (
    SELECT
        d.date,
        mc.cumulative_count AS matches_count,
        wc.cumulative_count AS weather_count,
        rc.cumulative_count AS reddit_count
    FROM dates d
    LEFT JOIN matches_cumulative mc ON d.date = mc.date
    LEFT JOIN weather_cumulative wc ON d.date = wc.date
    LEFT JOIN reddit_cumulative rc ON d.date = rc.date
    WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    ORDER BY d.date
)
SELECT
    date,
    matches_count,
    weather_count,
    reddit_count
FROM combined_counts
ORDER BY date
