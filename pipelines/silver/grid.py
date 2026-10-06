"""Silver cleaning for Grid-India: structure the PSP report sheet rows.

The bronze `sheet_row` cells from the NLDC Power Supply Position workbooks are
parsed into a typed daily region table. Known quirks handled: the source's own
"Shoratge" typo, Hindi sheet noise (ignored - English sheets are parsed), and
value cells like "71173.0" / "( 1,583)" / "-".
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date

import pyarrow as pa

from pipelines.silver import base
from pipelines.silver.base import silver_table_path, write_rejects, write_silver

LOGGER = logging.getLogger(__name__)

REGION_MAP = {
    "NR": "Northern",
    "WR": "Western",
    "SR": "Southern",
    "ER": "Eastern",
    "NER": "North-Eastern",
    "TOTAL": "All India",
}

_METRICS = {
    "demand_met_evening_peak_mw": re.compile(r"demand met \(mw\) during evening peak", re.I),
    "shortage_evening_peak_mw": re.compile(r"sh(ortage|oratge) \(mw\) during evening peak", re.I),
    "energy_met_mu": re.compile(r"energy met \(mu\)", re.I),
}


def parse_number(raw: str) -> float | None:
    """'( 1,583)' -> -1583.0; '71173.0' -> 71173.0; '-' / '' -> None."""
    text = raw.strip()
    if text in ("", "-"):
        return None
    negative = text.startswith("(") and text.endswith(")")
    cleaned = re.sub(r"[\s,()\[\]{}|!*]", "", text)
    if cleaned in ("", "-"):
        return None
    try:
        value = float(cleaned)
    except ValueError as exc:
        raise ValueError(f"unparsable number {raw!r}") from exc
    return -value if negative else value


def parse_psp_rows(rows: list[list[str]]) -> tuple[list[dict], list[dict]]:
    """Walk one report's sheet rows; return (records, rejects)."""
    records: list[dict] = []
    rejects: list[dict] = []
    region_columns: dict[int, str] = {}
    for row in rows:
        # The metric label and its regional values share one row
        # (e.g. ["Demand Met (MW) during Evening Peak hr ...", "", "71173.0", ...]).
        joined = " ".join(row)
        metric = next((name for name, rx in _METRICS.items() if rx.search(joined)), None)
        if metric:
            for idx, region_code in region_columns.items():
                if idx >= len(row):
                    continue
                raw = row[idx]
                try:
                    value = parse_number(raw)
                except ValueError:
                    rejects.append(
                        {
                            "reject_reason": f"unparsable {metric} value {raw!r}",
                            "record": json.dumps({"row": row}, default=str),
                        }
                    )
                    continue
                if value is None:
                    continue
                records.append(
                    {
                        "metric": metric,
                        "region_code": region_code,
                        "region_name": REGION_MAP[region_code],
                        "value": value,
                    }
                )
            continue
        for idx, cell in enumerate(row):
            upper = cell.strip().upper()
            if upper in REGION_MAP and idx not in region_columns:
                region_columns[idx] = upper
    return records, rejects


def build_grid(con, settings) -> dict[str, int]:
    run_id = base.make_run_id()
    rows = con.execute("""
        SELECT report_date, report_title, page, row_idx, cells_json
        FROM read_parquet('data/bronze/gridindia/DAILY_PSP_REPORT/*.parquet')
        WHERE kind = 'sheet_row' AND page = 1
        ORDER BY report_date, row_idx
    """).fetchall()

    by_report: dict[str, dict[int, list[str]]] = {}
    for report_date, _title, _page, row_idx, cells_json in rows:
        by_report.setdefault(str(report_date), {})[row_idx] = json.loads(cells_json)

    clean: list[dict] = []
    rejects: list[dict] = []
    for report_date, row_map in sorted(by_report.items()):
        ordered = [row_map[key] for key in sorted(row_map)]
        records, rejects_part = parse_psp_rows(ordered)
        for reject in rejects_part:
            reject["record"] = json.dumps({**json.loads(reject["record"]), "report_date": report_date})
            rejects.append(reject)
        for record in records:
            record["report_date"] = date.fromisoformat(report_date)
            clean.append(record)

    schema = pa.schema(
        [
            ("report_date", pa.date32()),
            ("metric", pa.string()),
            ("region_code", pa.string()),
            ("region_name", pa.string()),
            ("value", pa.float64()),
        ]
    )
    written = write_silver(
        silver_table_path(settings, "gridindia", "supply_daily"),
        pa.Table.from_pylist(clean, schema=schema),
    )
    n_rejects = write_rejects(settings, rejects, source="gridindia", table="supply_daily", run_id=run_id)
    LOGGER.info("silver/gridindia: %d clean rows across %d report days, %d rejects", written, len(by_report), n_rejects)
    return {"rows": written, "rejects": n_rejects}
