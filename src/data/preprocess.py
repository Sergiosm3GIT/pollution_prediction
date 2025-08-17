# src/data/preprocess.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

from src.utils.io import build_path, write_parquet, read_json, write_json  # helper genérico GCS/local
import fsspec

UTC = timezone.utc

# ----------------------------
# Config vía envs
# ----------------------------
CITY = os.getenv("CITY", "Santiago")
GCS_BUCKET = os.getenv("GCS_BUCKET", "").strip()

# Partición a procesar; si no se pasa, usa hoy (UTC)
PROC_DATE = os.getenv("PROC_DATE", datetime.now(UTC).strftime("%Y-%m-%d"))

# Parámetros a mantener/esperados
PARAMETERS = [
    p.strip()
    for p in os.getenv(
        "PARAMETERS",
        "pm25,pm10,pm1,no2,o3,so2,co,relativehumidity,temperature,um003",
    ).split(",")
    if p.strip()
]

# Target (umbral PM2.5, µg/m³)
THRESHOLD_PM25 = float(os.getenv("THRESHOLD_PM25", "25"))

# Reglas de unidades – normalizamos todo a éstas
CANONICAL_UNITS = {
    "pm25": "µg/m³",
    "pm10": "µg/m³",
    "pm1": "µg/m³",
    "no2": "µg/m³",
    "o3": "µg/m³",
    "so2": "µg/m³",
    "co": "mg/m³",             # si llega en µg/m³ convertimos a mg/m³
    "relativehumidity": "%",
    "temperature": "°C",
    "um003": "count/cm³",      # ajusta según documentación
}

# Conversión simple de unidades – extiende si necesitas más
def _convert_units(series: pd.Series, src_unit: str | None, parameter: str) -> pd.Series:
    target = CANONICAL_UNITS.get(parameter)
    if src_unit is None or target is None or src_unit == target:
        return series
    if parameter == "co" and src_unit in ("µg/m³", "ug/m3") and target == "mg/m³":
        # 1000 µg/m³ = 1 mg/m³
        return series / 1000.0
    # si no conocemos la conversión, dejamos como está
    return series

def _is_gcs() -> bool:
    return bool(GCS_BUCKET)

def _raw_partition_path() -> str:
    # gs://<bucket>/openaq/<CITY>/dt=<PROC_DATE>  (o ruta local si no hay bucket)
    rel = f"openaq/{CITY}/dt={PROC_DATE}"
    if GCS_BUCKET:
        bucket = GCS_BUCKET.replace("gs://", "").strip("/")
        return f"gs://{bucket}/{rel}"
    return str(Path(rel))

def _processed_partition_path() -> str:
    rel = f"openaq/{CITY}/processed/dt={PROC_DATE}"
    if GCS_BUCKET:
        bucket = GCS_BUCKET.replace("gs://", "").strip("/")
        return f"gs://{bucket}/{rel}"
    return str(Path(rel))

def _features_partition_path() -> str:
    rel = f"openaq/{CITY}/features/dt={PROC_DATE}"
    if GCS_BUCKET:
        bucket = GCS_BUCKET.replace("gs://", "").strip("/")
        return f"gs://{bucket}/{rel}"
    return str(Path(rel))

def _list_measurement_files() -> list[str]:
    base = _raw_partition_path()  # ej: gs://bucket/openaq/Santiago/dt=2025-08-17
    pattern = f"{base}/measurements_*.parquet"
    print(f"[preprocess] GCS_BUCKET={GCS_BUCKET!r}")
    print(f"[preprocess] base={base}")
    print(f"[preprocess] pattern={pattern}")

    if base.startswith("gs://"):
        fs = fsspec.filesystem("gcs")
        try:
            files = fs.glob(pattern)
            print(f"[preprocess] glob found: {files}")
            if not files:
                # si el glob vino vacío, listamos el prefijo para ver qué hay
                parent = base.rstrip("/")
                try:
                    listing = fs.ls(parent)
                    print(f"[preprocess] ls({parent}) → {[e.get('name', e) for e in listing]}")
                except Exception as e:
                    print(f"[preprocess] warning: ls({parent}) falló: {e}")
            return files
        except Exception as e:
            print(f"[preprocess] error: glob({pattern}) falló: {e}")
            return []
    else:
        return [str(p) for p in Path(base).glob("measurements_*.parquet")]

def _load_concat_measurements(files: list[str]) -> pd.DataFrame:
    dfs = []
    for p in sorted(files):
        try:
            # fuerza pyarrow y deja que gcsfs se encargue de gs://
            dfs.append(pd.read_parquet(p, engine="pyarrow"))
        except Exception as e:
            print(f"[preprocess] warning: no se pudo leer {p}: {e}")
    if not dfs:
        return pd.DataFrame()
    # concatena aunque tengan columnas distintas (outer join por columnas)
    return pd.concat(dfs, ignore_index=True, sort=False)

def _basic_qc(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()

    # Esperamos estas columnas del extract:
    # ['value','parameter','unit','date_utc','location_id','sensor_id', ...]
    if "value" in df.columns:
        # descarta negativos en contaminantes
        mask_pollutant = df["parameter"].isin(["pm25","pm10","pm1","no2","o3","so2","co"])
        df = df[~(mask_pollutant & (df["value"] < 0))]
    if "date_utc" in df.columns:
        df = df[~df["date_utc"].isna()]
    after = len(df)
    print(f"[preprocess] QC: {before} -> {after} filas")
    return df

def _normalize_units(df: pd.DataFrame) -> pd.DataFrame:
    if not {"parameter","unit","value"}.issubset(df.columns):
        return df
    out = []
    for param, grp in df.groupby("parameter"):
        src_units = grp["unit"].mode().iloc[0] if not grp["unit"].isna().all() else None
        grp = grp.copy()
        grp["value"] = _convert_units(grp["value"], src_units, param)
        grp["unit"] = CANONICAL_UNITS.get(param, src_units)
        out.append(grp)
    return pd.concat(out, ignore_index=True) if out else df

def _resample_hourly_pivot(df: pd.DataFrame) -> pd.DataFrame:
    # 1) quedarse con parámetros de interés
    df = df[df["parameter"].isin(PARAMETERS)].copy()

    # 2) timestamp a datetime (UTC)
    df["ts"] = pd.to_datetime(df["date_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    # 3) para cada (parameter, ts), agregamos por promedio
    agg = df.groupby([pd.Grouper(key="ts", freq="1H"), "parameter"], observed=True)["value"].mean().reset_index()

    # 4) pivot ancho: una columna por parámetro
    wide = agg.pivot(index="ts", columns="parameter", values="value").sort_index()

    # 5) rellenos suaves para meteo
    for p in ["temperature", "relativehumidity"]:
        if p in wide.columns:
            wide[p] = wide[p].interpolate(limit=2).ffill().bfill()

    # 6) nombre claro al índice
    wide.index.name = "timestamp_utc"
    return wide.reset_index()

def _compute_stats(df_wide: pd.DataFrame) -> dict:
    stats = {}
    if not df_wide.empty:
        cols = [c for c in df_wide.columns if c != "timestamp_utc"]
        stats["na_ratio"] = {c: float(df_wide[c].isna().mean()) for c in cols}
        stats["min"] = {c: float(np.nanmin(df_wide[c])) for c in cols if not df_wide[c].isna().all()}
        stats["max"] = {c: float(np.nanmax(df_wide[c])) for c in cols if not df_wide[c].isna().all()}
    return stats

def _save_processed(df_wide: pd.DataFrame, stats: dict):
    out_dir = _processed_partition_path()
    main_path = f"{out_dir}/preprocessed.parquet"
    write_parquet(df_wide, main_path)

    meta = {
        "city": CITY,
        "proc_date": PROC_DATE,
        "rows": int(len(df_wide)),
        "columns": list(df_wide.columns),
        "parameters": PARAMETERS,
        "stats": stats,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00","Z"),
        "artifact": "processed",
        "version": "v1",
    }
    meta_path = f"{out_dir}/_SUCCESS.json"
    write_json(meta, meta_path)

    print(f"[preprocess] processed → {main_path}")
    print(f"[preprocess] metadata  → {meta_path}")

# ----------------------------
# Features mínimos
# ----------------------------
def _add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    ts = pd.to_datetime(out["timestamp_utc"], utc=True)

    out["hour"] = ts.dt.hour.astype("int16")
    out["dow"] = ts.dt.weekday.astype("int8")      # 0=Mon
    out["is_weekend"] = (out["dow"] >= 5).astype("int8")
    return out

def _add_lags_and_rolls(df: pd.DataFrame) -> pd.DataFrame:
    out = df.set_index(pd.to_datetime(df["timestamp_utc"], utc=True)).copy()

    # Lags para principales parámetros
    lag_params = ["pm25", "no2", "temperature", "relativehumidity"]
    for p in lag_params:
        if p in out.columns:
            for L in [1, 2, 3, 6]:
                out[f"{p}_lag{L}"] = out[p].shift(L)

    # Rolling means (horarias)
    if "pm25" in out.columns:
        for W in [3, 6]:
            out[f"pm25_roll{W}_mean"] = out["pm25"].rolling(W, min_periods=1).mean()

    if "no2" in out.columns:
        for W in [3]:
            out[f"no2_roll{W}_mean"] = out["no2"].rolling(W, min_periods=1).mean()

    out.reset_index(drop=False, inplace=True)
    out.rename(columns={"index": "timestamp_utc"}, inplace=True)
    return out

def _add_target(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # target: ¿pm25 de la próxima hora supera el umbral?
    if "pm25" in out.columns:
        out["pm25_next_hour"] = out["pm25"].shift(-1)
        out["target_polluted_next_hour"] = (out["pm25_next_hour"] > THRESHOLD_PM25).astype("Int8")
    else:
        out["pm25_next_hour"] = np.nan
        out["target_polluted_next_hour"] = pd.Series(dtype="Int8")
    return out

def _save_features(df_feat: pd.DataFrame):
    out_dir = _features_partition_path()
    main_path = f"{out_dir}/features.parquet"
    write_parquet(df_feat, main_path)

    cols = [c for c in df_feat.columns if c != "timestamp_utc"]
    meta = {
        "city": CITY,
        "proc_date": PROC_DATE,
        "rows": int(len(df_feat)),
        "columns": list(df_feat.columns),
        "feature_set": "minimal_v1",
        "threshold_pm25": THRESHOLD_PM25,
        "label": "target_polluted_next_hour",
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00","Z"),
        "artifact": "features",
        "version": "v1",
        "feature_stats": {
            "na_ratio": {c: float(df_feat[c].isna().mean()) for c in cols},
        }
    }
    meta_path = f"{out_dir}/_SUCCESS.json"
    write_json(meta, meta_path)

    print(f"[preprocess] features  → {main_path}")
    print(f"[preprocess] metadata  → {meta_path}")

# ----------------------------
# Entry principal
# ----------------------------
def run_preprocess():
    print(f"[preprocess] city={CITY} date={PROC_DATE} bucket={'gs://'+GCS_BUCKET if _is_gcs() else '(local)'}")
    raw_dir = _raw_partition_path()

    # 1) leer mediciones del día
    files = _list_measurement_files()
    if not files:
        print(f"[preprocess] no hay mediciones en {raw_dir}")
        empty = pd.DataFrame(columns=["timestamp_utc"] + PARAMETERS)
        _save_processed(empty, {"note": "no data"})
        # también escribimos features vacíos por idempotencia
        _save_features(_add_target(_add_lags_and_rolls(_add_calendar_features(empty))))
        return

    df_raw = _load_concat_measurements(files)
    if df_raw.empty:
        print(f"[preprocess] mediciones vacías en {raw_dir}")
        empty = pd.DataFrame(columns=["timestamp_utc"] + PARAMETERS)
        _save_processed(empty, {"note": "empty"})
        _save_features(_add_target(_add_lags_and_rolls(_add_calendar_features(empty))))
        return

    # 2) limpieza y normalización
    df_qc = _basic_qc(df_raw)
    df_norm = _normalize_units(df_qc)

    # 3) resample + pivot (tabla limpia por hora)
    df_wide = _resample_hourly_pivot(df_norm)

    # 4) stats + guardar processed
    stats = _compute_stats(df_wide)
    _save_processed(df_wide, stats)

    # 5) construir features mínimos
    df_feat = _add_calendar_features(df_wide)
    df_feat = _add_lags_and_rolls(df_feat)
    df_feat = _add_target(df_feat)

    # 6) guardar features
    _save_features(df_feat)

if __name__ == "__main__":
    run_preprocess()
