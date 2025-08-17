from __future__ import annotations
import fsspec
import pandas as pd
import json
from src.utils.config import SETTINGS

def gcs_path(*parts: str) -> str:
    clean = "/".join(p.strip("/") for p in parts)
    return f"gs://{SETTINGS.gcs_bucket}/{clean}"

def write_json_gcs(obj, key: str) -> str:
    path = gcs_path(key)
    with fsspec.open(path, "w") as f:
        json.dump(obj, f)
    return path

def write_parquet_gcs(df: pd.DataFrame, key: str) -> str:
    path = gcs_path(key)
    df.to_parquet(path, index=False)
    return path
