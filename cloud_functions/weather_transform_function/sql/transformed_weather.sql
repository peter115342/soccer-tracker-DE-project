config {
    type: "table",
    schema: "sports_data_eu",
    name: "weather_processed",
    description: "Processed weather data with match-time averages",
    bigquery: {
        partitionBy: "DATE(timestamp)"
    }
}

WITH weather_hourly AS (
  SELECT 
    SAFE_CAST(id AS INT64) as match_id,
    latitude,
    longitude,
    time.element as time_str,
    temperature_2m.element as temperature_2m,
    relativehumidity_2m.element as relativehumidity_2m,
    dewpoint_2m.element as dewpoint_2m,
    apparent_temperature.element as apparent_temperature,
    precipitation.element as precipitation,
    rain.element as rain,
    snowfall.element as snowfall,
    snow_depth.element as snow_depth,
    weathercode.element as weathercode,
    pressure_msl.element as pressure_msl,
    cloudcover.element as cloudcover,
    windspeed_10m.element as windspeed_10m,
    winddirection_10m.element as winddirection_10m,
    windgusts_10m.element as windgusts_10m
  FROM sports_data_raw_parquet.weather_parquet,
    UNNEST(hourly.time.list) as time WITH OFFSET pos
    LEFT JOIN UNNEST(hourly.temperature_2m.list) as temperature_2m WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.relativehumidity_2m.list) as relativehumidity_2m WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.dewpoint_2m.list) as dewpoint_2m WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.apparent_temperature.list) as apparent_temperature WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.precipitation.list) as precipitation WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.rain.list) as rain WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.snowfall.list) as snowfall WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.snow_depth.list) as snow_depth WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.weathercode.list) as weathercode WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.pressure_msl.list) as pressure_msl WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.cloudcover.list) as cloudcover WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.windspeed_10m.list) as windspeed_10m WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.winddirection_10m.list) as winddirection_10m WITH OFFSET pos USING(pos)
    LEFT JOIN UNNEST(hourly.windgusts_10m.list) as windgusts_10m WITH OFFSET pos USING(pos)
  WHERE id IS NOT NULL
)

SELECT 
  w.match_id,
  ROUND(w.latitude, 2) as lat,
  ROUND(w.longitude, 2) as lon,
  m.utcDate as timestamp,
  ROUND(AVG(CAST(w.temperature_2m AS FLOAT64)), 2) as temperature_2m,
  ROUND(AVG(CAST(w.relativehumidity_2m AS FLOAT64)), 2) as relativehumidity_2m,
  ROUND(AVG(CAST(w.dewpoint_2m AS FLOAT64)), 2) as dewpoint_2m,
  ROUND(AVG(CAST(w.apparent_temperature AS FLOAT64)), 2) as apparent_temperature,
  ROUND(AVG(CAST(w.precipitation AS FLOAT64)), 2) as precipitation,
  ROUND(AVG(CAST(w.rain AS FLOAT64)), 2) as rain,
  ROUND(AVG(CAST(w.snowfall AS FLOAT64)), 2) as snowfall,
  ROUND(AVG(CAST(w.snow_depth AS FLOAT64)), 2) as snow_depth,
  CAST(AVG(CAST(w.weathercode AS INT64)) AS INT64) as weathercode,
  ROUND(AVG(CAST(w.pressure_msl AS FLOAT64)), 2) as pressure_msl,
  ROUND(AVG(CAST(w.cloudcover AS FLOAT64)), 2) as cloudcover,
  ROUND(AVG(CAST(w.windspeed_10m AS FLOAT64)), 2) as windspeed_10m,
  ROUND(AVG(CAST(w.winddirection_10m AS FLOAT64)), 2) as winddirection_10m,
  ROUND(AVG(CAST(w.windgusts_10m AS FLOAT64)), 2) as windgusts_10m
FROM weather_hourly w
JOIN sports_data_eu.matches_processed m ON w.match_id = m.id
WHERE PARSE_TIMESTAMP('%Y-%m-%dT%H:%M', w.time_str) 
  BETWEEN m.utcDate AND TIMESTAMP_ADD(m.utcDate, INTERVAL 2 HOUR)
GROUP BY w.match_id, w.latitude, w.longitude, m.utcDate
