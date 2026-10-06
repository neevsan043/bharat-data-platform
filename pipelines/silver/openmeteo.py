"""Silver cleaning for Open-Meteo weather (SQL-driven, typed)."""

from __future__ import annotations

import json
import logging

import pyarrow as pa

from pipelines.silver import base
from pipelines.silver.base import silver_table_path, write_rejects, write_silver

LOGGER = logging.getLogger(__name__)


def _reject(source: str, table: str, reason: str, record: dict) -> dict:
    return {"reject_reason": reason, "record": json.dumps(record, default=str)}

def build_openmeteo(con, settings) -> dict[str, int]:
    """Clean weather: archive only, dedup by (city, date), temp sanity checks."""
    run_id = base.make_run_id()
    rows = con.execute("""
        SELECT city, latitude, longitude, date, temperature_2m_max, temperature_2m_min,
               precipitation_sum, wind_speed_10m_max
        FROM read_parquet('data/bronze/openmeteo/**/*.parquet')
        WHERE forecast_made_on IS NULL
        ORDER BY city, date
    """).fetchall()
    clean, rejects, seen = [], [], set()
    for city, lat, lon, day, t_max, t_min, precip, wind in rows:
        record = {"city": city, "date": str(day), "t_max": t_max, "t_min": t_min}
        key = (city, day)
        if key in seen:
            rejects.append(_reject("openmeteo", "weather_daily", "duplicate (city, date)", record))
            continue
        seen.add(key)
        if t_max is not None and t_min is not None and t_min > t_max:
            rejects.append(_reject("openmeteo", "weather_daily", "temp_min above temp_max", record))
            continue
        if t_max is not None and not -20 <= t_max <= 60:
            rejects.append(_reject("openmeteo", "weather_daily", "temp_max out of plausible range", record))
            continue
        clean.append(
            {
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "date": day,
                "temperature_2m_max": t_max,
                "temperature_2m_min": t_min,
                "precipitation_sum": precip,
                "wind_speed_10m_max": wind,
            }
        )
    schema = pa.schema(
        [
            ("city", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("date", pa.date32()),
            ("temperature_2m_max", pa.float64()),
            ("temperature_2m_min", pa.float64()),
            ("precipitation_sum", pa.float64()),
            ("wind_speed_10m_max", pa.float64()),
        ]
    )
    written = write_silver(
        silver_table_path(settings, "openmeteo", "weather_daily"),
        pa.Table.from_pylist(clean, schema=schema),
    )
    n_rejects = write_rejects(settings, rejects, source="openmeteo", table="weather_daily", run_id=run_id)
    LOGGER.info("silver/openmeteo: %d clean rows, %d rejects", written, n_rejects)
    return {"rows": written, "rejects": n_rejects}
