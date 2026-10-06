"""Silver cleaning for NPCI UPI monthly stats (SQL-driven, typed)."""

from __future__ import annotations

import json
import logging
from datetime import date

import pyarrow as pa

from pipelines.silver import base
from pipelines.silver.base import silver_table_path, write_rejects, write_silver

LOGGER = logging.getLogger(__name__)


def _reject(source: str, table: str, reason: str, record: dict) -> dict:
    return {"reject_reason": reason, "record": json.dumps(record, default=str)}

def build_npci(con, settings) -> dict[str, int]:
    """Clean UPI monthly stats; flag partial current month; dedup by month."""
    run_id = base.make_run_id()
    src = "data/bronze/npci/*.parquet"
    today = date.today()
    rows = con.execute(f"""
        SELECT month_start, fy, volume_mn, avg_daily_volume_mn, value_cr,
               avg_daily_value_cr, source_file
        FROM read_parquet('{src}')
        ORDER BY month_start
    """).fetchall()

    clean, rejects, seen = [], [], set()
    for month_start, fy, vol, avg_vol, val, avg_val, source_file in rows:
        record = {
            "month_start": str(month_start), "fy": fy, "volume_mn": vol,
            "value_cr": val, "source_file": source_file,
        }
        if month_start in seen:
            rejects.append(_reject("npci", "upi_monthly", "duplicate month_start", record))
            continue
        seen.add(month_start)
        if vol is None or val is None or vol <= 0 or val <= 0:
            rejects.append(_reject("npci", "upi_monthly", "non-positive or null volume/value", record))
            continue
        is_partial = month_start.year == today.year and month_start.month == today.month
        # The known Phase-0 finding: the current month is a partial export, so its
        # avg-daily/volume consistency check would false-positive - flag instead.
        if (
            not is_partial
            and avg_vol is not None
            and abs(avg_vol * 30 - vol) / max(vol, 1) > 0.6
        ):
            rejects.append(
                _reject("npci", "upi_monthly", "avg_daily inconsistent with volume (>60% off)", record)
            )
            continue
        clean.append(
            {
                "month_start": month_start,
                "fy": fy,
                "volume_mn": float(vol),
                "avg_daily_volume_mn": float(avg_vol) if avg_vol is not None else None,
                "value_cr": float(val),
                "avg_daily_value_cr": float(avg_val) if avg_val is not None else None,
                "is_partial_month": is_partial,
                "source_file": source_file,
            }
        )
    schema = pa.schema(
        [
            ("month_start", pa.date32()),
            ("fy", pa.string()),
            ("volume_mn", pa.float64()),
            ("avg_daily_volume_mn", pa.float64()),
            ("value_cr", pa.float64()),
            ("avg_daily_value_cr", pa.float64()),
            ("is_partial_month", pa.bool_()),
            ("source_file", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(clean, schema=schema)
    written = write_silver(silver_table_path(settings, "npci", "upi_monthly"), table)
    n_rejects = write_rejects(settings, rejects, source="npci", table="upi_monthly", run_id=run_id)
    LOGGER.info("silver/npci: %d clean rows, %d rejects", written, n_rejects)
    return {"rows": written, "rejects": n_rejects}


