import requests
import json
import time
import os
import re
import ast
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from measurements_structure import flatten_measurement

project_root = Path(__file__).resolve().parent.parent.parent
dotenv_path = project_root / '.env'
load_dotenv(dotenv_path=dotenv_path, override=True)
API_KEY = os.getenv("OPENAQ_API_KEY") 
URL_BASE = "https://api.openaq.org/v3"
INPUT_FILE = "pollution-prediction/data/raw/sensors_metadata.json"

os.chdir(Path(__file__).resolve().parent.parent.parent.parent)

with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    sensor_data = json.load(f)

for i, sensor in enumerate(sensor_data["sensors"]):
    print(f"\nProcessing sensor {i+1}/{len(sensor['sensors'])}: {sensor.get('name', 'Unnamed')}")
    for p in sensor['sensors']:
        print(p.get('name', 'Unnamed'))
        print(p.get('parameter', {}))

sensor_data["sensors"]

sensor_id = 1044
API_MEASUREMENTS_URL = f"{URL_BASE}/sensors/{sensor_id}/measurements"
print(API_MEASUREMENTS_URL)


headers = {
    "Accept": "application/json",
    "X-API-Key": API_KEY
}

params = {
    "datetime_from": "2025-08-01T00:00:00Z",
    "datetime_to": "2025-08-02T00:00:00Z",
    "limit":  1000  
}

response = requests.get(API_MEASUREMENTS_URL, headers=headers, params=params)
print(response.status_code)
data = response.json()

OUTPUT_FILE = "pollution-prediction/data/raw/sensors_measurements.json"
os.chdir(Path(__file__).resolve().parent.parent.parent.parent)
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)


flattened = [flatten_measurement(m) for m in data["results"]]
df = pd.DataFrame(flattened)
df.to_csv("pollution-prediction/data/raw/sensors_measurements_test.csv", index=False)
print(df.head())