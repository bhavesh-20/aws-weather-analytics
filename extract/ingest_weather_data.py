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
    
    Event Input formats:
    
    # Single date/hour (backward compatible)
    {
      "date": "2024-01-15",
      "hour": 10
    }
    
    # Multiple date/hour pairs
    {
      "jobs": [
        {"date": "2024-01-15", "hour": 10},
        {"date": "2024-01-15", "hour": 11},
        {"date": "2024-01-16", "hour": 9}
      ]
    }
    
    # Scheduled run (empty event) - fetches previous hour
    {}
    """
    
    try:
        # 1. Load configuration
        config = load_config()
        logger.info(f"Loaded configuration: {config}")
        
        # 2. Parse the input event to determine target dates and hours
        jobs = parse_event(event)
        if(len(jobs) > config.max_backfill_events):
            logger.warning(f"Number of jobs ({len(jobs)}) exceeds MAX_BACKFILL_EVENTS ({config.max_backfill_events}). Limiting to max.")
        jobs = jobs[:config.max_backfill_events]  # Limit to max backfill entries
        logger.info(f"Processing {len(jobs)} job(s)")
        
        # 3. Process each job
        total_success = 0
        total_cities = len(config.cities)
        
        for job_index, (target_date, target_hour) in enumerate(jobs, 1):
            logger.info(f"Processing job {job_index}/{len(jobs)}: {target_date} hour {target_hour:02d}")
            
            job_success = 0
            for city_name in config.cities:
                try:
                    process_city(city_name, target_date, target_hour, config)
                    job_success += 1
                except Exception as e:
                    logger.error(f"Failed to process {city_name} for {target_date} {target_hour:02d}: {str(e)}")
            
            total_success += job_success
            logger.info(f"Job {job_index} completed: {job_success}/{total_cities} cities succeeded")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Backfill completed. Total: {total_success}/{len(jobs) * total_cities} successful city-hour combinations')
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Backfill failed: {str(e)}')
        }

def parse_event(event):
    """Parses the Lambda event to determine target dates and hours."""
    now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    jobs = []
    
    # Handle empty event (scheduled run)
    if not event or event == {}:
        # Default to previous hour for scheduled runs
        target_hour = now.hour
        target_date = now.strftime('%Y-%m-%d')
        return [(target_date, target_hour)]
    
    # Handle multiple jobs format
    if 'jobs' in event and isinstance(event['jobs'], list):
        for job in event['jobs']:
            if not isinstance(job, dict):
                raise ValueError("Each job must be a dictionary with 'date' and 'hour'")
            
            if 'date' not in job or 'hour' not in job:
                raise ValueError("Each job must contain both 'date' and 'hour'")
            
            target_date = job['date']
            target_hour = validate_hour(job['hour'])
            jobs.append((target_date, target_hour))
        
        return jobs
    
    # Handle single job format (backward compatible)
    if 'date' in event and 'hour' in event:
        target_date = event['date']
        target_hour = validate_hour(event['hour'])
        return [(target_date, target_hour)]
    
    # Handle invalid format
    raise ValueError("Invalid event format. Use {'date': '...', 'hour': ...} or {'jobs': [{'date': '...', 'hour': ...}, ...]}")

def validate_hour(hour):
    """Validate and convert hour to integer."""
    try:
        target_hour = int(hour)
        if target_hour < 0 or target_hour > 23:
            raise ValueError("Hour must be between 0 and 23")
        return target_hour
    except (ValueError, TypeError):
        raise ValueError("Hour must be a valid integer between 0 and 23")

def process_city(city_name, target_date, target_hour, config):
    """Processes a single city: fetches historical data and saves to S3."""
    # Generate city_id from city_name (lowercase, replace spaces with underscores)
    city_id = city_name.lower().replace(' ', '_')
    
    logger.info(f"Processing city: {city_name} for {target_date} hour {target_hour:02d}")
    
    # Fetch historical data from API
    data = fetch_historical_weather(config.api_key, config.base_url, city_name, target_date, target_hour)
    
    # Build S3 key and save (will replace existing file if it exists)
    s3_key = build_s3_key(city_id, target_date, target_hour)
    save_json_to_s3(data, s3_key, config.raw_data_bucket)
    logger.info(f"Successfully saved data for {city_name} to {s3_key}")