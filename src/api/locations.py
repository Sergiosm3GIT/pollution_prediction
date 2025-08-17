import os
import json
import time
import requests
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from prefect import task

project_root = Path(__file__).resolve().parents[2]
dotenv_path = project_root / ".env"
load_dotenv(dotenv_path=dotenv_path, override=True)

API_KEY = os.getenv("OPENAQ_API_KEY") 
API_URL = "https://api.openaq.org/v3/locations"

@task
def FindSensors(
    COORDINATES: tuple = (-33.4489, -70.6693),  # Default to Plaza de Armas coordinates
    RADIUS_METERS: int = 25000,  # Default to 25km radius around center
    OUTPUT_FILE: str = "pollution_prediction/data/raw/sensors_metadata.json",
    LOCATION_LABEL: str = "Santiago, Chile",
    API_KEY_OVERRIDE: str | None = None,
):
    """
    Devuelve la lista de 'locations' (sensors) cerca de COORDINATES.
    Si OUTPUT_FILE es None, no escribe a disco (solo retorna).
    """
    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY
    }
    
    sensors_list = []
    page = 1
    has_more = True
    
    print(f"Fetching sensor metadata within {RADIUS_METERS/1000}km of {LOCATION_LABEL}...")
    while has_more:
        params = {
            "coordinates": f"{COORDINATES[0]},{COORDINATES[1]}",
            "radius": RADIUS_METERS,
            "limit": 1000,  # Max allowed per page
            "page": page,
        }
        
        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            sensors_list.extend(data["results"])
            has_more = data["meta"]["found"] > page * data["meta"]["limit"]
            page += 1
            
            print(f"Page {page-1}: Found {len(data['results'])} sensors (Total: {len(sensors_list)})")
            time.sleep(0.1)  # Respect rate limits
            
        except Exception as e:
            print(f"Error fetching page {page}: {str(e)}")
            break

    result = {
        "metadata": {
            "generated_at": datetime.now().isoformat()+ "Z",
            "api_version": "v3",
            "location": LOCATION_LABEL,
            "center_coordinates": COORDINATES,
            "radius_meters": RADIUS_METERS,
            "total_sensors": len(sensors_list)
        },
        "sensors": sensors_list
    }

    if OUTPUT_FILE:
        out_path = Path(OUTPUT_FILE)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"Saved metadata for {len(sensors_list)} sensors → {OUTPUT_FILE}")

    return sensors_list


if __name__ == "__main__":
    COORDINATES = (-33.4489, -70.6693)  # Plaza de Armas coordinates
    RADIUS_METERS = 25000  # 25km radius around center
    OUTPUT_FILE = "data/raw/sensors_metadata.json"
    FindSensors(COORDINATES = COORDINATES, RADIUS_METERS = RADIUS_METERS, OUTPUT_FILE = OUTPUT_FILE)






