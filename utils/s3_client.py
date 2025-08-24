import json
import boto3
from datetime import datetime, timezone

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