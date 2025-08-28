-- Query for Temperature Trends
WITH base AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city_name ORDER BY source_date DESC) as day_num
    FROM weather_analytics.processed
)

SELECT 
    source_date,
    city_name,
    AVG(temperature_c) as avg_daily_temp,
    MIN(temperature_c) as min_daily_temp,
    MAX(temperature_c) as max_daily_temp
FROM base
WHERE HOUR(CAST(observation_time as timestamp)) BETWEEN 6 AND 20  -- Daytime hours only
AND day_num <= 7
GROUP BY source_date, city_name
ORDER BY source_date DESC, city_name;