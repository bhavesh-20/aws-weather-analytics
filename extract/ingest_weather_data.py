import json
import logging
from datetime import datetime, timezone, timedelta

# Import our modules
from config import load_config
from utils.weather_api import fetch_historical_weather
from utils.s3_client import save_json_to_s3, build_s3_key

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Fetches historical weather data from WeatherAPI.com and stores it in S3.
    Runs every hour to fetch data for the previous hour.
    
    Event Input (for backfill):
    {
        "date": "2025-08-20",  # Optional, defaults to today
        "hour": 10             # Optional, defaults to previous hour
    }
    """
    
    try:
        # 1. Load configuration
        config = load_config()
        logger.info(f"Loaded configuration: {config}")
        
        # 2. Parse the input event to determine target date and hour
        target_date, target_hour = parse_event(event)
        logger.info(f"Fetching historical data for date: {target_date}, hour: {target_hour}")
        
        # 3. Process each city
        success_count = 0
        for city_name in config.cities:
            try:
                process_city(city_name, target_date, target_hour, config)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to process {city_name}: {str(e)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Ingestion completed. Success: {success_count}/{len(config.cities)}')
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Ingestion failed: {str(e)}')
        }

def parse_event(event):
    """Parses the Lambda event to determine target date and hour."""
    now = datetime.now(timezone.utc)
    
    # Default to previous hour for scheduled runs (no event provided)
    target_hour = now.hour
    target_date = now.strftime('%Y-%m-%d')
    
    # If event is provided, both date and hour are mandatory for backfill
    if event and isinstance(event, dict) and event:
        # Check if both date and hour are provided
        if 'date' not in event:
            raise ValueError("'date' parameter is required for backfill")
        if 'hour' not in event:
            raise ValueError("'hour' parameter is required for backfill")
        
        target_date = event['date']
        
        try:
            target_hour = int(event['hour'])
            if target_hour < 0 or target_hour > 23:
                raise ValueError("Hour must be between 0 and 23")
        except (ValueError, TypeError):
            raise ValueError("Hour must be a valid integer between 0 and 23")
    
    return target_date, target_hour

def process_city(city_name, target_date, target_hour, config):
    """Processes a single city: fetches historical data and saves to S3."""
    # Generate city_id from city_name (lowercase, replace spaces with underscores)
    city_id = city_name.lower().replace(' ', '_')
    
    logger.info(f"Processing city: {city_name} (ID: {city_id}) for {target_date} hour {target_hour:02d}")
    
    # Fetch historical data from API
    data = fetch_historical_weather(config.api_key, config.base_url, city_name, target_date, target_hour)
    
    # Build S3 key and save (will replace existing file if it exists)
    s3_key = build_s3_key(city_id, target_date, target_hour)
    save_json_to_s3(data, s3_key, config.raw_data_bucket)
    logger.info(f"Successfully saved data for {city_name} to {s3_key}")