import json
import boto3
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import logging

logger = logging.getLogger()

# Initialize S3 client outside the handler for reuse
s3_client = boto3.client('s3')

def save_json_to_s3(data, s3_key, s3_bucket):
    """
    Saves a JSON-serializable object to S3.
    
    Args:
        data: The data to save (will be converted to JSON)
        s3_key: The full S3 key (path) where the file will be saved
        s3_bucket: The s3 bucket where the data will be saved
    """
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
    except Exception as e:
        raise Exception(f"Failed to save data to S3: {str(e)}") from e

def build_s3_key(city_id, target_date, target_hour):
    """
    Constructs the S3 object key for storing historical data.
    
    Args:
        city_id: The unique identifier for the city
        target_date: The date of the data (YYYY-MM-DD)
        target_hour: The hour of the data (0-23)
    
    Returns:
        str: The S3 key
    """
    return f"historical/dt={target_date}/{city_id}_{target_hour:02d}.json"

def get_s3_client():
    """Get S3 client"""
    return boto3.client('s3')

def get_unprocessed_files_dict(raw_bucket: str, processed_bucket: str, max_days: int = 7) -> Dict[str, List[str]]:
    """
    Get dictionary of date -> list of raw JSON files that don't have processed counterparts
    Returns the newest max_days dates with unprocessed files
    """
    s3 = get_s3_client()
    unprocessed_files_dict = {}
    
    try:
        # Get all dates with raw data (without date filtering first)
        all_raw_dates = set()
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=raw_bucket,
            Prefix="historical/dt=",
            Delimiter='/'
        )
        
        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    path = prefix['Prefix']
                    if 'dt=' in path:
                        date_str = path.split('dt=')[1].rstrip('/')
                        try:
                            # Just collect all dates first, we'll sort and limit later
                            datetime.strptime(date_str, '%Y-%m-%d')  # Validate format
                            all_raw_dates.add(date_str)
                        except ValueError:
                            logger.warning(f"Skipping invalid date format: {date_str}")
                            continue
        
        if not all_raw_dates:
            logger.info("No raw data dates found")
            return {}
        
        # Convert to list and sort by date (newest first)
        sorted_dates = sorted(all_raw_dates, reverse=True)
        logger.info(f"Found {len(sorted_dates)} raw data dates: {sorted_dates[:10]}...")
        
        # Process dates until we reach max_days limit
        processed_count = 0
        for date_str in sorted_dates:
            if processed_count >= max_days:
                logger.info(f"Reached max_days limit ({max_days}), stopping processing")
                break
                
            try:
                # Get all raw JSON files for this date
                raw_files = []
                paginator = s3.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=raw_bucket,
                    Prefix=f"historical/dt={date_str}/"
                )
                
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if obj['Key'].endswith('.json'):
                                raw_files.append(obj['Key'])
                
                if not raw_files:
                    logger.info(f"No JSON files found for date {date_str}")
                    continue
                
                # Get processed city-hour combinations for this date
                processed_combinations = set()
                try:
                    # Check if processed date directory exists
                    s3.head_object(Bucket=processed_bucket, Key=f"processed/dt={date_str}/")
                    
                    # List processed city-hour combinations
                    city_paginator = s3.get_paginator('list_objects_v2')
                    city_iterator = city_paginator.paginate(
                        Bucket=processed_bucket,
                        Prefix=f"processed/dt={date_str}/",
                        Delimiter='/'
                    )
                    
                    for city_page in city_iterator:
                        if 'CommonPrefixes' in city_page:
                            for city_prefix in city_page['CommonPrefixes']:
                                if 'city=' in city_prefix['Prefix']:
                                    city_id = city_prefix['Prefix'].split('city=')[1].rstrip('/')
                                    
                                    # List hours for this city
                                    hour_paginator = s3.get_paginator('list_objects_v2')
                                    hour_iterator = hour_paginator.paginate(
                                        Bucket=processed_bucket,
                                        Prefix=city_prefix['Prefix'],
                                        Delimiter='/'
                                    )
                                    
                                    for hour_page in hour_iterator:
                                        if 'CommonPrefixes' in hour_page:
                                            for hour_prefix in hour_page['CommonPrefixes']:
                                                if 'hour=' in hour_prefix['Prefix']:
                                                    try:
                                                        hour_str = hour_prefix['Prefix'].split('hour=')[1].rstrip('/')
                                                        hour_value = int(hour_str)
                                                        processed_combinations.add((city_id, hour_value))
                                                    except ValueError:
                                                        continue
                except:
                    # No processed data exists for this date yet
                    logger.info(f"No processed data found for date {date_str}")
                    pass
                
                # Filter raw files - keep only those without processed counterparts
                unprocessed_files = []
                for raw_file in raw_files:
                    try:
                        # Extract city_id and hour from filename: historical/dt=2024-01-15/london_10.json
                        filename = raw_file.split('/')[-1]
                        if filename.endswith('.json'):
                            base_name = filename[:-5]  # Remove .json
                            parts = base_name.split('_')
                            
                            if len(parts) >= 2:
                                city_id = "_".join(parts[:-1])
                                hour_value = int(parts[-1])
                                
                                if (city_id, hour_value) not in processed_combinations:
                                    unprocessed_files.append(f"s3://{raw_bucket}/{raw_file}")
                    except (ValueError, IndexError):
                        logger.warning(f"Skipping invalid filename: {raw_file}")
                        continue
                
                if unprocessed_files:
                    unprocessed_files_dict[date_str] = unprocessed_files
                    processed_count += 1
                    logger.info(f"Date {date_str}: {len(unprocessed_files)} unprocessed files")
                else:
                    logger.info(f"Date {date_str}: All files already processed")
                
            except Exception as e:
                logger.error(f"Error processing date {date_str}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error getting unprocessed files: {e}")
        raise
    
    logger.info(f"Found unprocessed files in {len(unprocessed_files_dict)} dates: {list(unprocessed_files_dict.keys())}")
    return unprocessed_files_dict