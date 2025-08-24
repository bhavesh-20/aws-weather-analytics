import requests

def call_weather_api(api_url, api_key, params):
    """
    Makes a generic call to the Weather API.
    
    Args:
        api_url: API to hit
        api_key: API key to use.
        params: Dictionary of query parameters
    
    Returns:
        dict: The JSON response from the API
    """
    # Ensure the API key is in the params
    final_params = params.copy()
    final_params['key'] = api_key
    
    try:
        response = requests.get(api_url, params=final_params, timeout=30)
        response.raise_for_status()  # Raises an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        # Log detailed error information
        if hasattr(e, 'response') and e.response is not None:
            error_msg = f"API Error: {e.response.status_code} - {e.response.text}"
        else:
            error_msg = f"API Error: {str(e)}"
        raise Exception(error_msg) from e

def fetch_historical_weather(api_key, base_url, location, date, hour, include_aqi=True):
    """Fetches historical weather data for a location, date, and hour."""
    url = f"{base_url}/history.json"
    params = {
        'q': location,
        'dt': date,
        'hour': hour,
        'aqi': 'yes' if include_aqi else 'no'
    }
    
    return call_weather_api(url, api_key, params)