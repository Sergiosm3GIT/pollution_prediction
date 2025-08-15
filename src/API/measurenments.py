import requests
import json
import time
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.API.measurenments_structure import flatten_measurement
from prefect import flow, task

project_root = Path(__file__).resolve().parent.parent.parent
dotenv_path = project_root / '.env'
load_dotenv(dotenv_path=dotenv_path, override=True)
API_KEY = os.getenv("OPENAQ_API_KEY") 
URL_BASE = "https://api.openaq.org/v3"

os.chdir(Path(__file__).resolve().parent.parent.parent.parent)

def load_sensor_data(input_file):
    """Load sensor metadata from a JSON file."""
    with open(input_file, 'r', encoding='utf-8') as f:
        return json.load(f)

@task(retries=3,retry_delay_seconds=2,log_prints=True)
def FetchMeasurements(headers,params,API_URL):
    """Get measurements for a specific sensor and parameter"""    
    all_results = []
    page = 1
    while True:
        params['page'] = page
        response = requests.get(API_URL, headers=headers, params=params)
        data = response.json()
        results = data["results"]
        all_results.extend(results)
        if not results:
            return all_results
        page += 1
        print(f"Fetched page {page} with {len(results)} records.")

@task
def FetchSensorData(
        PARAMETERS=["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"],
        start_date="2025-08-01T00:00:00Z",  # Adjust start date as needed
        end_date="2025-08-02T00:00:00Z",  # Adjust end date as needed
        limit=1000,
        INPUT_FILE="pollution_prediction/data/raw/sensors_metadata.json",
        output_file = f"pollution_prediction/data/raw/sensors_measurements_{datetime.now().strftime('%y%m%d%H%M%S')}.parquet",
        allowed_locations=[25,45,846,852,967,2845837,2845838],
        allowed_sensors = [9213887,9213897,1047,4445,4489,4533,4549,1044,2271,3190,4099,4100,9213871,9213876,
        9213861,9213880,4643,9213884,9213893,9213881,9213889]
):
    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY
    }
    sensor_data = load_sensor_data(INPUT_FILE)
    df_list = []
    for i, station in enumerate(sensor_data["sensors"]):
        location_id = station.get("id", None)
        location_sensors = len(station['sensors'])
        location_name = station.get('name', 'Unnamed')
        print(f"\nProcessing station {i+1}/ Location {location_id} /  {location_sensors} sensors: {location_name}")
        if location_id not in allowed_locations:
            print(f"Skipping station {location_id}.")
            continue

        available_measuremnts = { value :measurement.get("id", None)
            for measurement in station["sensors"]
            for key, value in measurement.get("parameter", {}).items()
            if key == 'name' and value in PARAMETERS
        }
        print(f"Available measurements: {list(available_measuremnts.keys())}")
        for parameter in available_measuremnts:
            sensor_id = available_measuremnts[parameter]
            print(sensor_id)
            if sensor_id not in allowed_sensors:
                print(f"Skipping station {sensor_id}.")
                continue
            
            API_MEASUREMENTS_URL = f"{URL_BASE}/sensors/{sensor_id}/measurements"
            df_sensor_metadata = pd.DataFrame({'sensor_id': sensor_id, 'parameter': parameter, 'location_id': location_id, 'location_name': location_name}, index=[0]) 
            params = {
                "datetime_from": start_date,
                "datetime_to": end_date,
                "limit": limit,
             }
            try:
                results = FetchMeasurements(headers, params, API_MEASUREMENTS_URL)
                print(f"Fetched {len(results)} records for sensor {sensor_id} ({parameter}) between {start_date} and {end_date}.")
                flattened = [flatten_measurement(m) for m in results]
                df = pd.DataFrame(flattened)
                if not df.empty:
                    df = pd.concat([pd.concat([df_sensor_metadata] * len(flattened), ignore_index=True),df], axis=1)
                    df_list.append(df)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for sensor {sensor_id} ({parameter}): {e}")
            except KeyError as e:
                print(f"KeyError for sensor {sensor_id} ({parameter}): {e}")
            time.sleep(0.5)  # Sleep to avoid hitting API rate limits
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.to_parquet(output_file, index=False)
        print(f"Data saved to {output_file}")            
        return combined_df

if __name__ == "__main__":
    print("Fetching sensor data from OpenAQ...")
    PARAMETERS=["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"]
    data = FetchSensorData(PARAMETERS = PARAMETERS)
    
