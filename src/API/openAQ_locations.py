import os
import json
import time
import requests
from pathlib import Path
from openaq import OpenAQ
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY") 
API_URL = "https://api.openaq.org/v3/locations"

def FindSensors(
    COORDINATES: tuple = (-33.4489, -70.6693),  # Default to Plaza de Armas coordinates
    RADIUS_METERS: int = 25000,  # Default to 25km radius around center
    OUTPUT_FILE: str = "pollution-prediction/data/raw/sensors_metadata.json"
):
    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY
    }
    
    # Step 1: Get all locations within radius
    sensors_list = []
    page = 1
    has_more = True
    
    print(f"Fetching sensor metadata within {RADIUS_METERS/1000}km of Santiago center...")
    while has_more:
        params = {
            "coordinates": f"{COORDINATES[0]},{COORDINATES[1]}",
            "radius": RADIUS_METERS,
            "limit": 1000,  # Max allowed per page
            "page": page,
            #"order_by": "lastUpdated",  # Get most recently updated first
            #"sort": "desc"
        }
        
        try:
            response = requests.get(API_URL, headers=headers, params=params)
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
            "generated_at": datetime.now().isoformat(),
            "api_version": "v3",
            "location": "Santiago, Chile",
            "center_coordinates": COORDINATES,
            "radius_meters": RADIUS_METERS,
            "total_sensors": len(sensors_list)
        },
        "sensors": sensors_list
    }

    os.chdir(Path(__file__).resolve().parent.parent.parent.parent)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\nSuccessfully saved metadata for {len(sensors_list)} sensors to {OUTPUT_FILE}")
    return sensors_list


if __name__ == "__main__":
    COORDINATES = (-33.4489, -70.6693)  # Plaza de Armas coordinates
    RADIUS_METERS = 25000  # 25km radius around center
    OUTPUT_FILE = "pollution-prediction/data/raw/sensors_metadata.json"
    FindSensors()






