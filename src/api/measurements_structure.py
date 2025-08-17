# src/api/measurements_structure.py

from __future__ import annotations

def flatten_measurement(measurement: dict) -> dict:
    # parameter puede ser dict o string
    param = measurement.get("parameter")
    if isinstance(param, dict):
        param_name = param.get("name")
        param_units = param.get("units")
    else:
        # string u otro tipo → usa 'unit' si viene plano
        param_name = param
        param_units = measurement.get("unit")

    # date puede venir como {"utc": "..."} o no estar
    date = measurement.get("date") or {}
    ts_utc = date.get("utc") if isinstance(date, dict) else None
    ts_local = date.get("local") if isinstance(date, dict) else None

    # period/coverage pueden faltar: usa .get() encadenado con defaults
    period = measurement.get("period") or {}
    coverage = measurement.get("coverage") or {}

    # Helpers anidados seguros
    def _get(d, *keys, default=None):
        cur = d
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k)
        return cur if cur is not None else default

    return {
        "value": measurement.get("value"),
        "parameter": param_name,
        "unit": param_units,
        "timestamp": ts_utc,               # UTC preferido
        "timestamp_local": ts_local,

        # Periodo (opcionales)
        "period_label": period.get("label"),
        "period_interval": period.get("interval"),
        "datetime_from_utc": _get(period, "datetimeFrom", "utc"),
        "datetime_from_local": _get(period, "datetimeFrom", "local"),
        "datetime_to_utc": _get(period, "datetimeTo", "utc"),
        "datetime_to_local": _get(period, "datetimeTo", "local"),

        # Coverage (opcionales)
        "coverage_expectedCount": coverage.get("expectedCount"),
        "coverage_expectedInterval": coverage.get("expectedInterval"),
        "coverage_observedCount": coverage.get("observedCount"),
        "coverage_observedInterval": coverage.get("observedInterval"),
        "coverage_percentComplete": coverage.get("percentComplete"),
        "coverage_percentCoverage": coverage.get("percentCoverage"),
        "coverage_datetime_from_utc": _get(coverage, "datetimeFrom", "utc"),
        "coverage_datetime_from_local": _get(coverage, "datetimeFrom", "local"),
        "coverage_datetime_to_utc": _get(coverage, "datetimeTo", "utc"),
        "coverage_datetime_to_local": _get(coverage, "datetimeTo", "local"),
    }
