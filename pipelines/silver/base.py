"""Shared silver helpers: DuckDB connection, atomic writers, reject records."""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.config import Settings

LOGGER = logging.getLogger(__name__)

REJECTS_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("table", pa.string()),
        ("reject_reason", pa.string()),
        ("record", pa.string()),  # JSON of the offending bronze row
        ("run_id", pa.string()),
        ("rejected_at", pa.string()),
    ]
)


def connect(settings: Settings | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    _ = settings
    return con


def silver_table_path(settings: Settings, source: str, table: str) -> Path:
    return settings.data_dir / "silver" / source / f"{table}.parquet"


def write_silver(path: Path, table: pa.Table) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".parquet.tmp")
    os.close(fd)
    tmp = Path(tmp_name)
    try:
        pq.write_table(table, tmp)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return pq.read_metadata(path).num_rows


def write_rejects(
    settings: Settings,
    rejects: list[dict[str, Any]],
    *,
    source: str,
    table: str,
    run_id: str,
) -> int:
    """Replace this run's rejects file for the table; empty list removes stale rows."""
    path = silver_table_path(settings, source, f"rejects_{table}")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    records = [
        {
            "source": source,
            "table": table,
            "reject_reason": str(row.get("reject_reason", "unspecified")),
            "record": row.get("record", "{}"),
            "run_id": run_id,
            "rejected_at": now,
        }
        for row in rejects
    ]
    write_silver(path, pa.Table.from_pylist(records, schema=REJECTS_SCHEMA))
    return len(records)


def make_run_id() -> str:
    return uuid.uuid4().hex[:12]
