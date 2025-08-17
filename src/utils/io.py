# src/utils/io_generic.py
from __future__ import annotations
from pathlib import Path
import json
import fsspec
import pandas as pd


def is_gcs_path(path: str) -> bool:
    return isinstance(path, str) and path.startswith("gs://")


def build_path(rel_key: str, gcs_bucket: str | None = None) -> str:
    """
    Devuelve una ruta absoluta. Si se entrega gcs_bucket => gs://<bucket>/<rel_key>,
    de lo contrario, usa una ruta local (creando directorios padre si hiciera falta).
    """
    if gcs_bucket:
        rel = "/".join(p.strip("/") for p in str(rel_key).split("/"))
        return f"gs://{gcs_bucket}/{rel}"
    else:
        p = Path(rel_key)
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)


# -----------------------
# JSON
# -----------------------
def read_json(path: str) -> dict:
    """Lee JSON desde gs:// o local."""
    with fsspec.open(path, "r") as f:
        return json.load(f)


def write_json(obj: dict, path: str) -> str:
    """Escribe JSON en gs:// o local (pretty-print)."""
    if is_gcs_path(path):
        with fsspec.open(path, "w") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# -----------------------
# Parquet
# -----------------------
def write_parquet(df: pd.DataFrame, path: str) -> str:
    """Escribe Parquet en gs:// (vía gcsfs) o local."""
    if is_gcs_path(path):
        df.to_parquet(path, index=False)
    else:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
    return path
