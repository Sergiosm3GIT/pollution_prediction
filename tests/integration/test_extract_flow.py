import os
from unittest.mock import patch
import pandas as pd
from src.data.extract import DataExtractionFlow

@patch.dict(os.environ, {
    "CITY": "TestCity",
    "COORDINATES": "-33.4,-70.6",
    "RADIUS_M": "5000",
    "PARAMETERS": "pm25,no2",
    "GCS_BUCKET": "",  # escribe local
    "STATE_BLOB": "state/test_state.json",
    "BOOTSTRAP_HOURS": "1",
    "SAFETY_OVERLAP_MIN": "1",
}, clear=False)
@patch("src.api.locations.FindSensors.fn", return_value=[{"id": 1, "name": "L1", "sensors": []}])
@patch("src.api.measurements.FetchSensorData.fn")
def test_flow_runs_and_writes_local(mock_fetch, _find):
    # simula un dataframe pequeño
    mock_fetch.return_value = pd.DataFrame(
        {"timestamp": ["2025-08-01T00:00:00Z"], "parameter": ["pm25"], "value": [12]}
    )
    # no debe lanzar excepción
    DataExtractionFlow()
