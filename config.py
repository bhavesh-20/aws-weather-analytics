import os
import logging

logger = logging.getLogger()

class Config:
    """Loads and validates all environment variables at once."""
    
    def __init__(self):
        self.api_key = os.environ.get('WEATHER_API_KEY')
        self.base_url = os.environ.get('BASE_URL', 'http://api.weatherapi.com/v1').strip('/')
        self.raw_data_bucket = os.environ.get('RAW_DATA_BUCKET')
        self.cities = self._parse_cities(os.environ.get('CITIES'))
        self.max_backfill_events = int(os.environ.get('MAX_BACKFILL_EVENTS', '24'))
        
        self._validate()
    
    def _parse_cities(self, cities_string):
        """Parse cities from comma-separated string environment variable."""
        if not cities_string:
            return []
        
        # Split by comma, strip whitespace, and filter out empty strings
        cities = [city.strip() for city in cities_string.split(',') if city.strip()]
        return cities
    
    def _validate(self):
        """Validate that all required configuration is present."""
        errors = []
        
        if not self.api_key:
            errors.append("WEATHER_API_KEY environment variable is required")
        if not self.raw_data_bucket:
            errors.append("RAW_DATA_BUCKET environment variable is required")
        if not self.cities:
            errors.append("CITIES environment variable is required and must contain at least one city")
        
        if errors:
            error_msg = "Configuration errors:\n- " + "\n- ".join(errors)
            logger.error(error_msg)
            raise ValueError(error_msg)

def load_config():
    """Convenience function to load and return configuration."""
    return Config()