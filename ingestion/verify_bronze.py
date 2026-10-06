"""Verify bronze with DuckDB: row counts, date ranges and 5 sample rows per source.

    python -m ingestion.verify_bronze
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

LOGGER = logging.getLogger(__name__)

# source -> date column used for the range summary (None = no date column)
DATE_COLUMNS: dict[str, str | None] = {
    "worldbank": "year",
    "openmeteo": "date",
    "gridindia": "report_date",
    "npci": "month_start",
    "trai": "report_date",
    "opencellid": None,
}

SAMPLES = 5


def bronze_parquet_files(bronze_dir: Path, source: str) -> list[Path]:
    source_dir = bronze_dir / source
    if not source_dir.exists():
        return []
    return [
        path
        for path in sorted(source_dir.rglob("*.parquet"))
        if "_raw" not in path.parts and "_manifest" not in path.parts
    ]


def summarise(con: duckdb.DuckDBPyConnection, source: str, files: list[Path]) -> dict[str, Any]:
    relation = con.read_parquet([str(path) for path in files])
    summary: dict[str, Any] = {
        "files": len(files),
        "rows": relation.aggregate("count(*)").fetchone()[0],
    }
    date_column = DATE_COLUMNS.get(source)
    if date_column and date_column in relation.columns:
        low, high = relation.aggregate(f"min({date_column}), max({date_column})").fetchone()
        summary["range"] = f"{low} .. {high}"
        summary["date_column"] = date_column
        if "forecast_made_on" in relation.columns:
            archive = relation.aggregate(
                f"min({date_column}), max({date_column}) FILTER (forecast_made_on IS NULL)"
            ).fetchone()
            summary["archive_range"] = f"{archive[0]} .. {archive[1]}"
    summary["columns"] = list(relation.columns)
    summary["samples"] = relation.limit(SAMPLES).to_arrow_table().to_pylist()
    return summary


def main(bronze_dir: Path | None = None) -> None:
    from ingestion.config import load_settings

    settings = load_settings()
    target = bronze_dir or settings.bronze_dir
    con = duckdb.connect()
    # Respect the 8 GB budget even in verification (AGENTS.md rule 3).
    con.execute("SET memory_limit='1GB'")
    con.execute("SET threads=2")
    for source in DATE_COLUMNS:
        files = bronze_parquet_files(target, source)
        print(f"\n=== {source} ===")
        if not files:
            print("  no bronze data")
            continue
        summary = summarise(con, source, files)
        print(f"  files: {summary['files']}  rows: {summary['rows']}")
        if "range" in summary:
            print(f"  {summary['date_column']} range: {summary['range']}")
        if "archive_range" in summary:
            print(f"  archive rows (forecast_made_on IS NULL): {summary['archive_range']}")
        print(f"  columns: {', '.join(summary['columns'])}")
        print(f"  {SAMPLES} sample rows:")
        for row in summary["samples"]:
            formatted = {key: value for key, value in row.items() if value is not None}
            print(f"    {formatted}")


if __name__ == "__main__":
    main()
