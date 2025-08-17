from unittest.mock import patch, Mock
import pandas as pd
from src.api.measurements import FetchSensorData

SENSORS_DOC = {
    "sensors": [
        {
            "id": 123, "name": "Loc A",
            "sensors": [
                {"id": 9001, "parameter": {"name": "pm25"}},
                {"id": 9002, "parameter": {"name": "no2"}},
            ],
        }
    ]
}

def _mk_resp(rows):
    m = Mock()
    m.json.return_value = {"results": rows}
    m.raise_for_status.return_value = None
    return m

@patch("src.api.measurements.load_sensor_data", return_value=SENSORS_DOC)
@patch("requests.get")
def test_fetch_sensor_data_multi_param(mock_get, _load):
    # two pages then stop for pm25
    pm25_p1 = _mk_resp([
        {"date": {"utc": "2025-08-01T00:00:00Z"}, "value": 10, "unit": "µg/m³", "parameter": "pm25"}
    ])
    pm25_p2 = _mk_resp([])

    # one page then stop for no2
    no2_p1  = _mk_resp([
        {"date": {"utc": "2025-08-01T00:05:00Z"}, "value": 20, "unit": "µg/m³", "parameter": "no2"}
    ])
    no2_p2 = _mk_resp([])

    mock_get.side_effect = [pm25_p1, pm25_p2, no2_p1, no2_p2]

    df = FetchSensorData.fn(
        PARAMETERS=["pm25", "no2"],
        start_date="2025-08-01T00:00:00Z",
        end_date="2025-08-01T01:00:00Z",
        limit=1000,
        INPUT_FILE="ignored.json",
        output_file=None,
        allowed_locations=None,
        allowed_sensors=None,
        API_KEY_OVERRIDE="fake",
    )
    assert isinstance(df, pd.DataFrame)
    assert set(df["parameter"].unique()) == {"pm25", "no2"}
    assert {"sensor_id", "parameter", "location_id", "timestamp"}.issubset(df.columns)
