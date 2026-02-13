WITH latest_records AS (
    SELECT
        job_quality_result.score as pass_rate
    FROM `{project_id}.processed_data_zone.{table_suffix}_processed_quality`
    WHERE job_quality_result.score IS NOT NULL
    ORDER BY job_start_time DESC
    LIMIT {limit}
)
SELECT AVG(pass_rate) as avg_pass_rate
FROM latest_records
