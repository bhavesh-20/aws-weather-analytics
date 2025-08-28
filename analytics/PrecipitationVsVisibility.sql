-- Query for Daily Precipitation vs. Visibility
WITH base AS (
    SELECT 
        *, 
        DENSE_RANK() OVER (PARTITION BY city_name ORDER BY source_date DESC) as day_num
    FROM 
        weather_analytics.processed
)
SELECT 
    source_date,
    city_name,
    SUM(precipitation_mm) as total_daily_precipitation,
    AVG(visibility_km) as avg_daily_visibility
FROM 
    base
WHERE 
    day_num <= 7
GROUP BY 
    source_date, city_name
ORDER BY 
    city_name, source_date;