import json
from unittest.mock import patch, Mock
from src.api.locations import FindSensors

def _fake_resp(results, found=2, limit=1000):
    return {
        "results": results,
        "meta": {"found": found, "limit": limit}
    }

@patch("requests.get")
def test_find_sensors_paginates_and_returns_list(mock_get):
    # page 1
    r1 = Mock()
    r1.json.return_value = _fake_resp([{"id": 1}, {"id": 2}], found=2, limit=1000)
    r1.raise_for_status.return_value = None
    # page 2 (empty)
    r2 = Mock()
    r2.json.return_value = _fake_resp([], found=2, limit=1000)
    r2.raise_for_status.return_value = None
    mock_get.side_effect = [r1, r2]

    sensors = FindSensors.fn(
        COORDINATES=(-33.4, -70.6),
        RADIUS_METERS=25000,
        OUTPUT_FILE=None,
        LOCATION_LABEL="Test City",
        API_KEY_OVERRIDE="fake",
    )
    assert isinstance(sensors, list)
    assert len(sensors) == 2
    assert sensors[0]["id"] == 1
