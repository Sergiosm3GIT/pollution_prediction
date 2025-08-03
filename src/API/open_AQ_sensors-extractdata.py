import requests
import json
import time
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from openAQ_measurenments_structure import flatten_measurement

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY") 
URL_BASE = "https://api.openaq.org/v3"

INPUT_FILE = "pollution-prediction/data/raw/sensors_metadata.json"
os.chdir(Path(__file__).resolve().parent.parent.parent.parent)
with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    sensor_data = json.load(f)


def FetchMeasurements(headers,params,API_URL):
    """Get measurements for a specific sensor and parameter"""    
    response = requests.get(API_URL, headers=headers, params=params)
    return response.json()

def FetchSensorData(
        PARAMETERS=["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"],
        start_date="2025-08-01T00:00:00Z",  # Adjust start date as needed
        end_date="2025-08-02T00:00:00Z",  # Adjust end date as needed
        limit=10000,
        output_file = "pollution-prediction/data/raw/sensors_measurements.csv"
):
    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY
    }
    df_list = []
    for i, station in enumerate(sensor_data["sensors"]):
        location_id = station.get("id", None)
        location_sensors = len(station['sensors'])
        location_name = station.get('name', 'Unnamed')
        print(f"\nProcessing station {i+1}/ Location {location_id} /  {location_sensors} sensors: {location_name}")
        
        available_measuremnts = { value :measurement.get("id", None)
            for measurement in station["sensors"]
            for key, value in measurement.get("parameter", {}).items()
            if key == 'name' and value in PARAMETERS
        }
        print(f"Available measurements: {list(available_measuremnts.keys())}")
        for parameter in available_measuremnts:
            sensor_id = available_measuremnts[parameter]
            print(sensor_id)
            API_MEASUREMENTS_URL = f"{URL_BASE}/sensors/{sensor_id}/measurements"

            params = {
                "datetime_from": "2025-08-01T00:00:00Z",
                "datetime_to": "2025-08-02T00:00:00Z",
                "limit":  1000  
             }
            try:
                data = FetchMeasurements(headers, params, API_MEASUREMENTS_URL)
                print(f"Fetched {len(data['results'])} records for sensor {sensor_id} ({parameter}) between {start_date} and {end_date}.")
                flattened = [flatten_measurement(m) for m in data["results"]]
                df = pd.DataFrame(flattened)
                df_list.append(df)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for sensor {sensor_id} ({parameter}): {e}")
            except KeyError as e:
                print(f"KeyError for sensor {sensor_id} ({parameter}): {e}")
            time.sleep(0.5)  # Sleep to avoid hitting API rate limits
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")            


if __name__ == "__main__":
    print("Fetching sensor data from OpenAQ...")
    PARAMETERS=["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"]
    data = FetchSensorData(PARAMETERS = PARAMETERS)
    
