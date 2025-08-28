-- Query for Peak Hour Analysis
WITH base AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city_name ORDER BY source_date DESC) as day_num
    FROM weather_analytics.processed
)

SELECT 
    city_name,
    CAST(hour AS int) as hour,
    AVG(temperature_c) as avg_temp,
    AVG(humidity) as avg_humidity,
    COUNT(*) as readings_count
FROM base
WHERE day_num <= 7
GROUP BY 1, 2
ORDER BY 1, 2;