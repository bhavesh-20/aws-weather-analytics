CREATE EXTERNAL TABLE processed_weather_db.weather_data (
  city_name STRING,
  region STRING,
  country STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  timezone STRING,
  forecast_date STRING,
  timestamp_epoch BIGINT,
  observation_time STRING,
  temperature_c DOUBLE,
  temperature_f DOUBLE,
  humidity INT,
  pressure_mb DOUBLE,
  wind_speed_kph DOUBLE,
  precipitation_mm DOUBLE,
  cloud_cover INT,
  visibility_km DOUBLE,
  uv_index DOUBLE,
  processing_time TIMESTAMP
)
PARTITIONED BY (source_date STRING, city_id STRING, hour INT)
STORED AS PARQUET
LOCATION 's3://your-processed-bucket/processed/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'projection.enabled'='true',
  'projection.source_date.type'='date',
  'projection.source_date.format'='yyyy-MM-dd',
  'projection.source_date.range'='2024-01-01,NOW',
  'projection.city_id.type'='injected',
  'projection.hour.type'='integer',
  'projection.hour.range'='0,23'
);