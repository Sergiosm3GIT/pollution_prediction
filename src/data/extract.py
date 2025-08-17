# src/data/extract.py
from __future__ import annotations
import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
import fsspec
from prefect import flow

from src.api.locations import FindSensors
from src.api.measurements import FetchSensorData
from src.utils.io_generic import build_path, read_json, write_json, write_parquet

UTC = timezone.utc

# ----------------------------
# Config vía variables de entorno
# ----------------------------
CITY = os.getenv("CITY", "Santiago")
COORDINATES_ENV = os.getenv("COORDINATES", "-33.4489,-70.6693")  # "lat,lon"
RADIUS_M = int(os.getenv("RADIUS_M", "25000"))
PARAMETERS = [
    p.strip()
    for p in os.getenv(
        "PARAMETERS",
        "pm25,pm10,pm1,no2,o3,so2,co,relativehumidity,temperature,um003",
    ).split(",")
    if p.strip()
]

# GCS: si está definido, escribimos a GCS; si no, a disco local
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()

# Estado incremental (checkpoint)
STATE_BLOB = os.getenv("STATE_BLOB", "state/openaq_extract_state.json")
SAFETY_OVERLAP_MIN = int(os.getenv("SAFETY_OVERLAP_MIN", "15"))
BOOTSTRAP_HOURS = int(os.getenv("BOOTSTRAP_HOURS", "24"))  # si no hay estado previo

# Local fallbacks (cuando no hay GCS)
LOCAL_RAW_DIR = Path("data/raw")


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _run_date_str(now: datetime) -> str:
    return now.strftime("%Y-%m-%d")


def _run_ts_str(now: datetime) -> str:
    return now.strftime("%Y%m%dT%H%M%S")


def _coordinates_tuple() -> tuple[float, float]:
    lat_s, lon_s = COORDINATES_ENV.split(",")
    return float(lat_s), float(lon_s)


# ----------------------------
# Checkpoint en GCS o Local
# ----------------------------
def _state_path() -> str:
    """Ruta del checkpoint (GCS si hay bucket, si no local)."""
    return build_path(STATE_BLOB, GCS_BUCKET or None)


def load_state() -> datetime:
    """Devuelve last_success_utc o un valor por defecto (now-BOOTSTRAP_HOURS)."""
    path = _state_path()
    try:
        data = read_json(path)
        ts = data.get("last_success_utc")
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)
    except FileNotFoundError:
        return _now_utc() - timedelta(hours=BOOTSTRAP_HOURS)
    except Exception:
        # Si el estado está corrupto, arrancamos con bootstrap
        return _now_utc() - timedelta(hours=BOOTSTRAP_HOURS)


def save_state(last_success_utc: datetime) -> None:
    payload = {
        "last_success_utc": last_success_utc.astimezone(UTC).isoformat().replace("+00:00", "Z")
    }
    path = _state_path()
    write_json(payload, path)


# ----------------------------
# Escritura flexible (GCS o local)
# ----------------------------
def _write_json(obj, rel_key: str) -> str:
    if GCS_BUCKET:
        path = build_path(rel_key, GCS_BUCKET)
        return write_json(obj, path)
    else:
        out = LOCAL_RAW_DIR / rel_key
        return write_json(obj, str(out))


def _write_parquet(df: pd.DataFrame, rel_key: str) -> str:
    if GCS_BUCKET:
        path = build_path(rel_key, GCS_BUCKET)
        return write_parquet(df, path)
    else:
        out = LOCAL_RAW_DIR / rel_key
        return write_parquet(df, str(out))


# ----------------------------
# Flow principal
# ----------------------------
@flow(name="extract-openaq", log_prints=True)
def DataExtractionFlow():
    now = _now_utc()
    run_dt = _run_date_str(now)
    run_ts = _run_ts_str(now)

    # 1) Ventana incremental
    last_success = load_state()
    start_date = (last_success - timedelta(minutes=SAFETY_OVERLAP_MIN)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[extract] window: {start_date} → {end_date}")
    print(f"[extract] city={CITY} coords={COORDINATES_ENV} radius_m={RADIUS_M}")
    print(f"[extract] parameters={PARAMETERS}")
    print(f"[extract] bucket={'gs://'+GCS_BUCKET if GCS_BUCKET else '(local)'}")

    # 2) Sensores
    sensors_list = FindSensors.fn(
        COORDINATES=_coordinates_tuple(),
        RADIUS_METERS=RADIUS_M,
        OUTPUT_FILE=None,
        LOCATION_LABEL=f"{CITY}, CL",
        API_KEY_OVERRIDE=os.getenv("OPENAQ_API_KEY", ""),
    )
    sensors_doc = {
        "metadata": {
            "generated_at": now.isoformat().replace("+00:00", "Z"),
            "city": CITY,
            "center_coordinates": _coordinates_tuple(),
            "radius_meters": RADIUS_M,
            "total_sensors": len(sensors_list),
        },
        "sensors": sensors_list,
    }
    sensors_key = f"openaq/{CITY}/dt={run_dt}/sensors_metadata.json"
    sensors_path = _write_json(sensors_doc, sensors_key)
    print(f"[extract] sensors → {sensors_path}")

    # 3) Mediciones multiparámetro
    df = FetchSensorData.fn(
        PARAMETERS=PARAMETERS,
        start_date=start_date,
        end_date=end_date,
        limit=1000,
        INPUT_FILE=sensors_path,
        output_file=None,  # guardamos nosotros
        API_KEY_OVERRIDE=os.getenv("OPENAQ_API_KEY", ""),
    )

    if df is None or df.empty:
        print("[extract] no data fetched; keeping previous checkpoint")
        return

    # 4) Escritura particionada (append-only)
    measurements_key = f"openaq/{CITY}/dt={run_dt}/measurements_{run_ts}.parquet"
    out_path = _write_parquet(df, measurements_key)
    print(f"[extract] wrote {len(df)} rows → {out_path}")

    # 5) Actualiza checkpoint SOLO si todo salió bien
    save_state(now)
    print(f"[extract] state updated to {now.isoformat().replace('+00:00','Z')}")


if __name__ == "__main__":
    DataExtractionFlow()
