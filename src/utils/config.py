from __future__ import annotations
from pydantic import BaseModel, field_validator
import os

class Settings(BaseModel):
    # OpenAQ
    openaq_api_key: str = os.getenv("OPENAQ_API_KEY", "")
    city: str = os.getenv("CITY", "Santiago")
    coordinates: str = os.getenv("COORDINATES", "-33.4489,-70.6693")  # "lat,lon"
    radius_m: int = int(os.getenv("RADIUS_M", "25000"))
    parameters: list[str] = []

    # Incremental extract
    cadence_hours: int = int(os.getenv("CADENCE_HOURS", "3"))
    safety_overlap_min: int = int(os.getenv("SAFETY_OVERLAP_MIN", "15"))
    state_blob: str = os.getenv("STATE_BLOB", "state/openaq_extract_state.json")

    # Storage
    gcs_bucket: str = os.getenv("GCS_BUCKET", "")

    @field_validator("parameters", mode="before")
    @classmethod
    def _parse_params(cls, v):
        if not v:
            v = os.getenv(
                "PARAMETERS",
                "pm25,pm10,pm1,no2,o3,so2,co,relativehumidity,temperature,um003",
            )
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

SETTINGS = Settings()
