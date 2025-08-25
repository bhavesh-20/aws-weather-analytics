import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit, input_file_name, regexp_extract
from datetime import datetime

# Import our utilities
from utils import get_unprocessed_files_dict

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_BUCKET', 'PROCESSED_BUCKET', 'MAX_DAYS'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
RAW_BUCKET = args['RAW_BUCKET']
PROCESSED_BUCKET = args['PROCESSED_BUCKET']
MAX_DAYS = int(args.get('MAX_DAYS', '7'))  # Default to 7 days

def process_files_bulk(unprocessed_files_dict: dict):
    """Process all unprocessed files in bulk using Spark"""
    if not unprocessed_files_dict:
        logger.info("No unprocessed files found. Exiting.")
        return 0, 0
    
    total_files = sum(len(files) for files in unprocessed_files_dict.values())
    logger.info(f"Found {total_files} unprocessed files across {len(unprocessed_files_dict)} dates")
    
    # Collect all file paths
    all_file_paths = []
    for date_str, files in unprocessed_files_dict.items():
        all_file_paths.extend(files)
        logger.info(f"Date {date_str}: {len(files)} files to process")
    
    if not all_file_paths:
        logger.info("No files to process after filtering")
        return 0, 0
    
    try:
        # Read all JSON files at once
        logger.info(f"Reading {len(all_file_paths)} JSON files...")
        raw_df = spark.read.option("multiLine", "true").json(all_file_paths)
        
        file_count = raw_df.count()
        logger.info(f"Successfully read {file_count} records from {len(all_file_paths)} files")
        
        if file_count == 0:
            logger.warning("No data found in the files")
            return 0, len(all_file_paths)
        
        # Extract city_id and hour from file path
        raw_df = raw_df.withColumn(
            "input_file", 
            input_file_name()
        ).withColumn(
            "filename",
            regexp_extract(col("input_file"), r"/([^/]+\.json)$", 1)
        ).withColumn(
            "city_id",
            regexp_extract(col("filename"), r"^(.+)_\d+\.json$", 1)
        ).withColumn(
            "hour",
            regexp_extract(col("filename"), r"_(\d+)\.json$", 1).cast("int")
        ).withColumn(
            "source_date",
            regexp_extract(col("input_file"), r"dt=([^/]+)/", 1)
        )
        
        # Transform the data
        processed_df = raw_df.select(
            col("location.name").alias("city_name"),
            col("location.region").alias("region"),
            col("location.country").alias("country"),
            col("location.lat").alias("latitude"),
            col("location.lon").alias("longitude"),
            col("location.tz_id").alias("timezone"),
            
            # Extract from forecast data
            col("forecast.forecastday").getItem(0).getField("date").alias("forecast_date"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("time_epoch").alias("timestamp_epoch"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("time").alias("observation_time"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("temp_c").alias("temperature_c"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("temp_f").alias("temperature_f"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("humidity").alias("humidity"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("pressure_mb").alias("pressure_mb"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("wind_kph").alias("wind_speed_kph"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("precip_mm").alias("precipitation_mm"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("cloud").alias("cloud_cover"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("vis_km").alias("visibility_km"),
            col("forecast.forecastday").getItem(0).getField("hour").getItem(0).getField("uv").alias("uv_index"),
            
            # Add metadata from filename
            col("city_id"),
            col("hour"),
            col("source_date"),
            current_timestamp().alias("processing_time")
        )
        
        # Write to processed S3 with partitioning
        output_path = f"s3://{PROCESSED_BUCKET}/processed/"
        logger.info(f"Writing processed data to: {output_path}")
        
        processed_df.repartition(col("source_date"), col("city_id"), col("hour")).write \
            .mode("append") \
            .format("parquet") \
            .partitionBy("source_date", "city_id", "hour") \
            .save(output_path)
        
        logger.info("Successfully wrote processed data")
        return file_count, 0
        
    except Exception as e:
        logger.error(f"Error processing files in bulk: {e}")
        import traceback
        traceback.print_exc()
        return 0, len(all_file_paths)

def main():
    """Main processing function"""
    try:
        # Get unprocessed files dictionary
        logger.info(f"Looking for unprocessed files from last {MAX_DAYS} days...")
        unprocessed_files_dict = get_unprocessed_files_dict(RAW_BUCKET, PROCESSED_BUCKET, MAX_DAYS)
        
        # Process all files in bulk
        success_count, failed_count = process_files_bulk(unprocessed_files_dict)
        
        logger.info(f"Processing completed. Successful records: {success_count}, Failed files: {failed_count}")
        
    except Exception as e:
        logger.error(f"Fatal error in main processing: {e}")
        import traceback
        traceback.print_exc()
        raise

main()
job.commit()