from src.API.openAQ_locations import FindSensors
from src.API.open_AQ_sensors_extractdata import FetchSensorData
from prefect import flow, task
import pandas as pd
from datetime import datetime

@flow(log_prints=True)
def DataExtractionFlow(
    COORDINATES=(-33.4489, -70.6693),  # Default to Plaza de Armas coordinates
    RADIUS_METERS=25000,  # Default to 25km radius around center
    OUTPUT_FILE_SENSORS="pollution-prediction/data/raw/sensors_metadata.json",
    start_date="2025-08-01T00:00:00Z",  # Adjust start date as needed
    end_date="2025-08-02T00:00:00Z",  # Adjust end date as needed
    output_file_measurements ="pollution-prediction/data/raw/sensors_measurements.parquet"  # Default output file for measurements
):
    # Extract sensor metadata
    sensors_list = FindSensors(
        COORDINATES=COORDINATES,
        RADIUS_METERS=RADIUS_METERS,
        OUTPUT_FILE=OUTPUT_FILE_SENSORS
    )

    df =  FetchSensorData(
        start_date=start_date,  # Adjust start date as needed
        end_date=end_date,  # Adjust end date as needed
        output_file = output_file_measurements
    )
    
if __name__ == "__main__":
    COORDINATES=(-33.4489, -70.6693) # Default to Plaza de Armas coordinates
    RADIUS_METERS=25000  # Default to 25km radius around center
    OUTPUT_FILE_SENSORS="pollution-prediction/data/raw/sensors_metadata.json"
    start_date="2025-01-01T00:00:00Z"  # Adjust start date as needed
    end_date="2025-08-01T00:00:00Z"  # Adjust end date as needed
    #OUTPUT_FILE_MEASUREMENTS=f"pollution-prediction/data/raw/sensors_measurements_{datetime.now().strftime('%y%m%d%H%M%S')}.parquet"
    OUTPUT_FILE_MEASUREMENTS=f"pollution-prediction/data/raw/sensors_measurements_2025_01_08.parquet"

    DataExtractionFlow(
        COORDINATES=COORDINATES,  # Default to Plaza de Armas coordinates
        RADIUS_METERS=RADIUS_METERS,  # Default to 25km radius around center
        OUTPUT_FILE_SENSORS=OUTPUT_FILE_SENSORS,
        start_date=start_date,  # Adjust start date as needed
        end_date=end_date,  # Adjust end date as needed
        output_file_measurements=OUTPUT_FILE_MEASUREMENTS  # Default output file for measurements
    )