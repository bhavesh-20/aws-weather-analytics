# Weather Data Engineering Pipeline

A cloud-native data engineering pipeline that collects, processes, and analyzes weather data from multiple cities using AWS serverless technologies.

## 📊 Architecture Overview
[Weather API] → [AWS Lambda] → [S3 Raw] → [AWS Glue ETL] → [S3 Processed] → [Athena] → [Analytics]


## 🏗️ Infrastructure Components

### Data Ingestion Layer
- **AWS Lambda**: Python-based ingestion function running hourly
- **EventBridge Scheduler**: Triggers Lambda every hour at :05
- **S3 Raw Storage**: JSON files stored with partitioning (`dt=YYYY-MM-DD/city_hour.json`)

### Data Processing Layer  
- **AWS Glue ETL**: Spark-based processing job running daily
- **S3 Processed Storage**: Parquet format with partitioning (`dt=YYYY-MM-DD/city=XXX/hour=XX/`)
- **Glue Data Catalog**: Table definitions for Athena querying

### Analytics Layer
- **Athena**: SQL queries on processed data
- **QuickSight-ready**: Pre-built queries for visualization (optional)

## 📁 Project Structure
```
aws-weather-analytics/
├── extract/
│ ├── ingest_weather_data.py # Lambda ingestion code
│ └── init.py
├── transform/
│ ├── process_weather_data.py # Glue ETL code
│ └── init.py
├── utils/
│ ├── s3_client.py # S3 operations
│ ├── weather_api.py # API client
│ ├── s3_utils.py # S3 utilities
│ └── init.py
├── analytics/ # Athena analytical queries
│ ├── TemperatureTrends.sql
│ ├── PeakHourAnalysis.sql
│ ├── PrecipitationVsVisibility.sql
├── layers/
│ └── ingestion/
│ └── requirements.txt # Lambda layer dependencies
├── scripts/
│ ├── build_ingestion_artifacts.sh
│ └── build_transform_artifacts.sh
├── load/
│ ├── athena_table_definition.sql
│ ├── setup_glue_table.txt
│ └── init.py
├── config.py # Configuration loader
├── weather_ingestion.zip # Lambda deployment package
├── weather_transform.zip # Glue job package
├── layer_ingestion.zip # Lambda layer
└── requirements.txt # Python dependencies
```


## 🔧 Setup & Deployment

### 1. Prerequisites
```bash
# Install AWS CLI and configure credentials
aws configure

# Create S3 buckets
aws s3 mb s3://weather-raw-data-<your-name>
aws s3 mb s3://weather-processed-data-<your-name>
aws s3 mb s3://code-artifacts-<your-name>

# Build deployment package
chmod +x build_ingestion_artifacts.sh
chmod +x build_transform_artifacts.sh
./build_ingestion_artifacts.sh
./build_transform_artifacts.sh


# Upload to S3
# upload weather_ingestion.zip to lambda and add extract.lambda_handler as the handler
# For glue upload weather_transform.zip and transform/process_weather_data.py to code_artifacts bucket glue/ folder
# In Glue advanced options, select the script location from the s3 bucket.
```


## ⚙️ Configuration

### Environment Variables
Set these in your Lambda function configuration:
```ini
WEATHER_API_KEY=your_weatherapi_key
RAW_DATA_BUCKET=weather-raw-data-<your-name>
PROCESSED_BUCKET=weather-processed-data-<your-name>
BASE_URL=http://api.weatherapi.com/v1
CITIES=Hyderabad,London,New York,Tokyo,Sydney
```

Glue Job Parameters
```ini
--RAW_BUCKET=weather-raw-data-<your-name>
--PROCESSED_BUCKET=weather-processed-data-<your-name>
--MAX_DAYS=7
--extra-py-files=s3://code-artifacts-<your-name>/glue/glue_utils.zip
```

## 🔄 Data Flow

### 1. Data Ingestion (Hourly)
Weather API → AWS Lambda → S3 Raw (JSON)

- Trigger: EventBridge Scheduler (cron(5 * * * ? *))
- Frequency: Every hour at :05
- Storage: s3://raw-bucket/historical/dt=YYYY-MM-DD/city_hour.json
- Format: Raw JSON API responses

### 2. Data Processing (Daily)
S3 Raw → AWS Glue ETL → S3 Processed (Parquet)
- Trigger: Manual or scheduled daily run
- Processing: Only unprocessed city-hour combinations
- Storage: s3://processed-bucket/processed/dt=YYYY-MM-DD/city=XXX/hour=XX/
- Format: Apache Parquet with Snappy compression
### 3. Data Analytics (On-Demand)
- S3 Processed → Athena → SQL Queries
Interface: Athena SQL console
- Catalog: Glue Data Catalog table
- Queries: Pre-built analytical queries in /analytics/

## 💰 Cost Optimization
Storage Optimization

- S3 Intelligent Tiering: Automatic cost savings for infrequently accessed data
- Parquet Compression: 70-80% storage reduction vs JSON
- Lifecycle Policies: Auto-delete raw data after 30 days, transition processed data to Glacier after 90 days

Compute Optimization
- Lambda Memory: 256MB (right-sized for API calls)
- Glue Workers: G.1X (2 workers) - most cost-effective for ETL
- Timeout Settings: Lambda: 2min, Glue: 15min (prevent overruns)

Query Optimization
- Partitioning: Reduced Athena scan costs by 90%+
- Columnar Format: Athena only reads required columns
- Predicate Pushdown: Efficient filtering at storage layer

## 🚀 Performance Features
- Ingestion Layer
Batching: Single Lambda invocation processes all cities
- Error Handling: Continue on individual city failures
- Idempotent: Same data can be re-ingested safely

Processing Layer
- Incremental Processing: Only processes new/unprocessed data
- Bulk Operations: Spark processes files in batches
- Memory Efficient: Handles small files efficiently with groupFiles option

Storage Layer
- Columnar Format: Parquet for analytical query performance
- Partitioning: 3-level partitioning (date/city/hour)
- Compression: Snappy compression for optimal space/performance balance

Query Layer
- Partition Projection: Automatic partition discovery without crawlers
- Statistics: Column-level statistics for query optimization
- Caching: Athena result caching for repeated queries

## 📊 Processed Table Schema
The processed data is stored with this schema (defined in load/athena_table_definition.sql)
```sql
CREATE EXTERNAL TABLE weather_analytics.processed_weather (
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
```

Partitioning structure
```sql
PARTITIONED BY (
  source_date STRING,    -- YYYY-MM-DD format
  city_id STRING,        -- lowercase city identifier (e.g., "new_york")
  hour INT               -- 0-23 hour of day
)
```

Storage properties
```sql
STORED AS PARQUET
LOCATION 's3://weather-processed-data-<your-name>/processed/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet',
  'projection.enabled'='true',
  'projection.source_date.type'='date',
  'projection.source_date.format'='yyyy-MM-dd',
  'projection.source_date.range'='2024-01-01,NOW',
  'projection.city_id.type'='injected',
  'projection.hour.type'='integer',
  'projection.hour.range'='0,23'
)
```