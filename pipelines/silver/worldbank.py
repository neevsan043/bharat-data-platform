"""Silver cleaning for World Bank indicators (SQL-driven, typed)."""

from __future__ import annotations

import json
import logging

import pyarrow as pa

from pipelines.silver import base
from pipelines.silver.base import silver_table_path, write_rejects, write_silver

LOGGER = logging.getLogger(__name__)


def _reject(source: str, table: str, reason: str, record: dict) -> dict:
    return {"reject_reason": reason, "record": json.dumps(record, default=str)}

def build_worldbank(con, settings) -> dict[str, int]:
    """Clean WB indicators: types, drop duplicate (indicator, year) pairs."""
    run_id = base.make_run_id()
    rows = con.execute("""
        SELECT indicator_id, indicator_name, country_iso3, year, value
        FROM read_parquet('data/bronze/worldbank/*.parquet')
        ORDER BY indicator_id, year
    """).fetchall()
    clean, rejects, seen = [], [], set()
    for ind_id, ind_name, iso3, year, value in rows:
        record = {"indicator_id": ind_id, "year": year, "value": value}
        key = (ind_id, year)
        if key in seen:
            rejects.append(_reject("worldbank", "indicators", "duplicate (indicator, year)", record))
            continue
        seen.add(key)
        if not 1900 <= year <= 2100:
            rejects.append(_reject("worldbank", "indicators", "year out of range", record))
            continue
        if value is not None and not -1e15 < value < 1e15:
            rejects.append(_reject("worldbank", "indicators", "value magnitude implausible", record))
            continue
        clean.append(
            {
                "indicator_id": ind_id,
                "indicator_name": ind_name,
                "country_iso3": iso3 or "IND",
                "year": year,
                "value": value,
            }
        )
    schema = pa.schema(
        [
            ("indicator_id", pa.string()),
            ("indicator_name", pa.string()),
            ("country_iso3", pa.string()),
            ("year", pa.int32()),
            ("value", pa.float64()),
        ]
    )
    written = write_silver(
        silver_table_path(settings, "worldbank", "indicators"),
        pa.Table.from_pylist(clean, schema=schema),
    )
    n_rejects = write_rejects(settings, rejects, source="worldbank", table="indicators", run_id=run_id)
    LOGGER.info("silver/worldbank: %d clean rows, %d rejects", written, n_rejects)
    return {"rows": written, "rejects": n_rejects}


