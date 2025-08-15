from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import fsspec
from src.utils.config import SETTINGS

UTC = timezone.utc

@dataclass
class ExtractState:
    last_success_utc: datetime | None

    @staticmethod
    def default(hours_back: int = 24) -> "ExtractState":
        return ExtractState(last_success_utc=datetime.now(UTC) - timedelta(hours=hours_back))

def _state_path() -> str:
    return f"gs://{SETTINGS.gcs_bucket}/{SETTINGS.state_blob}"

def load_state() -> ExtractState:
    try:
        with fsspec.open(_state_path(), "r") as f:
            data = json.load(f)
        ts = data.get("last_success_utc")
        if ts:
            return ExtractState(last_success_utc=datetime.fromisoformat(ts.replace("Z", "+00:00")))
    except FileNotFoundError:
        pass
    return ExtractState.default()

def save_state(state: ExtractState) -> None:
    payload = {
        "last_success_utc": state.last_success_utc.astimezone(UTC).isoformat().replace("+00:00", "Z")
    }
    with fsspec.open(_state_path(), "w") as f:
        json.dump(payload, f)

def compute_window(now: datetime, cadence_hours: int, safety_overlap_min: int, state: ExtractState) -> tuple[str, str]:
    start_ts = (state.last_success_utc or now) - timedelta(minutes=safety_overlap_min)
    end_ts = now
    to_iso = lambda dt: dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    return to_iso(start_ts), to_iso(end_ts)
