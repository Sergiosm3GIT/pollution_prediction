import os
import requests
import json
import time
import fsspec
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from prefect import task
from src.api.measurements_structure import flatten_measurement  # tu helper

project_root = Path(__file__).resolve().parents[2]
dotenv_path = project_root / ".env"
load_dotenv(dotenv_path=dotenv_path, override=True)

API_KEY = os.getenv("OPENAQ_API_KEY") 
URL_BASE = "https://api.openaq.org/v3"

def load_sensor_data(input_file: str):
    """Load sensor metadata from a JSON file."""
    with fsspec.open(input_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

@task(retries=3,retry_delay_seconds=2,log_prints=True)
def FetchMeasurements(headers,params,API_URL):
    """Get measurements for a specific sensor and parameter"""    
    all_results = []
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(API_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"Fetched page {page} with {len(results)} records.")
        page += 1
        time.sleep(0.25)  # cuida rate limit
    return all_results

@task
def FetchSensorData(
        PARAMETERS = None , #["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"],
        start_date: str | None = None, #"2025-08-01T00:00:00Z",  
        end_date: str | None = None, #"2025-08-02T00:00:00Z",  
        limit: int =1000,
        INPUT_FILE: str = project_root / "data/raw/sensors_metadata.json",
        output_file:  str | None = None, # f"pollution_prediction/data/raw/sensors_measurements_{datetime.now().strftime('%y%m%d%H%M%S')}.parquet",
        allowed_locations=[25,45,846,852,967,2845837,2845838],
        allowed_sensors = [9213887,9213897,1047,4445,4489,4533,4549,1044,2271,3190,4099,4100,9213871,9213876,
        9213861,9213880,4643,9213884,9213893,9213881,9213889],
        API_KEY_OVERRIDE: str | None = None,
):
    """
    Lee sensors del JSON (FindSensors), itera por parámetro y sensor permitido,
    trae mediciones y devuelve un DataFrame. Si 'output_file' se pasa, guarda parquet.
    """
    if PARAMETERS is None:
        # default multi-parámetro
        PARAMETERS = [
            "pm25", "pm10", "pm1", "no2", "o3", "so2", "co",
            "relativehumidity", "temperature", "um003",
        ]
    # ventana por defecto (últimas 24h) si no pasan fechas
    if end_date is None:
        end_date = _iso_now()
    if start_date is None:
        start_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00")) - timedelta(hours=24)
        start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY_OVERRIDE or API_KEY,
    }
    
    sensor_data = load_sensor_data(INPUT_FILE)
    df_list = []
    
    for i, station in enumerate(sensor_data["sensors"]):
        location_id = station.get("id", None)
        location_sensors = len(station['sensors'])
        location_name = station.get('name', 'Unnamed')
        print(f"\nProcessing station {i+1}/ Location {location_id} /  {location_sensors} sensors: {location_name}")
        if allowed_locations and location_id not in allowed_locations:
            print(f"Skipping station {location_id}.")
            continue

        available_measurements = { 
            value :measurement.get("id", None)
            for measurement in station["sensors"]
            for key, value in measurement.get("parameter", {}).items()
            if key == 'name' and value in PARAMETERS
        }

        if not available_measurements:
            print("No requested parameters at this station.")
            continue

        print(f"Available measurements: {list(available_measurements.keys())}")
        
        for parameter in available_measurements:
            sensor_id = available_measurements[parameter]
            print(sensor_id)
            if allowed_sensors and sensor_id not in allowed_sensors:
                print(f"Skipping station {sensor_id}.")
                continue
            
            api_measurements_url = f"{URL_BASE}/sensors/{sensor_id}/measurements"
            df_sensor_metadata = pd.DataFrame({'sensor_id': sensor_id, 'parameter_meta': parameter, 'location_id': location_id, 'location_name': location_name}, index=[0]) 
            params = {
                "datetime_from": start_date,
                "datetime_to": end_date,
                "limit": limit,
             }
            try:
                results = FetchMeasurements(headers, params, api_measurements_url)
                print(f"Fetched {len(results)} records for sensor {sensor_id} ({parameter}) between {start_date} and {end_date}.")
                if not results:
                    continue
                    
                flattened = [flatten_measurement(m) for m in results]
                df = pd.DataFrame(flattened)
                if df.empty:
                    continue

                df = pd.concat([pd.concat([df_sensor_metadata] * len(flattened), ignore_index=True),df], axis=1)
                df_list.append(df)
            except requests.exceptions.RequestException as e:
                print(f"Request error sensor {sensor_id} ({parameter}): {e}")
            except KeyError as e:
                print(f"KeyError for sensor {sensor_id} ({parameter}): {e}")
            time.sleep(0.25)  # Sleep to avoid hitting API rate limits
    
    if not df_list:
        print("No data fetched.")
        return pd.DataFrame()
    
    combined_df = pd.concat(df_list, ignore_index=True)
    
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(output_path, index=False)
        print(f"Data saved to {output_file}")            
    
    return combined_df

if __name__ == "__main__":
    print("Fetching sensor data from OpenAQ...")
    PARAMETERS=["pm25", "pm10","pm1", "no2", "o3", "so2", "co","relativehumidity", "temperature","um003"]
    start_date= "2025-01-01T00:00:00Z",  
    end_date= "2025-08-16T00:00:00Z",  
    output_file = f"data/raw/sensors_measurements_20250101_20250816.parquet"
    data = FetchSensorData(PARAMETERS = PARAMETERS, start_date = start_date, end_date = end_date, output_file=output_file) 
    
