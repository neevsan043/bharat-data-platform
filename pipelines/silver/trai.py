"""Silver cleaning for TRAI: subscriber annexures and teledensity tables.

Reliability findings (Phase 2, from the parsed bronze):
- Monthly Telecom Subscription reports carry Annexure-I (wireline) and
  Annexure-II (wireless) "Subscriber Base" tables: LSA x operator x (previous,
  current) month. These parse reliably and are the telecom mart's core.
- QPIR files carry circle teledensity tables ("Rural/Urban/Total Tele- density")
  whose headers and name cells split across rows - parsed tolerantly, with
  strict rejects for anything ambiguous.
- Country wireless teledensity is parsed from the QPIR narrative (page text),
  joined across wrapped lines.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path

import pyarrow as pa

from pipelines.silver import base
from pipelines.silver.base import silver_table_path, write_rejects, write_silver
from pipelines.silver.grid import parse_number

LOGGER = logging.getLogger(__name__)

OPERATOR_ALIASES = {
    "M TNL": "MTNL",
    "Tata Tele.": "Tata Teleservices",
    "Reliance Com.": "Reliance Communications",
}

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def normalise_lsa(name: str) -> str:
    text = re.sub(r"\s+", " ", name.strip())
    text = re.sub(r"\s*&\s*", " & ", text)
    text = re.sub(r"\(\s+", "(", text)
    text = re.sub(r"\s+\)", ")", text)
    # pdfplumber sometimes separates a capital from the rest of a word ("M umbai")
    text = re.sub(r"\b([A-Z])\s+(?=[a-z])", r"\1", text)
    aliases = {
        "U.P.(E)": "UP (East)",
        "U.P.(W)": "UP (West)",
        "U.P. (E)": "UP (East)",
        "U.P. (W)": "UP (West)",
        "J & K": "Jammu & Kashmir",
        "J&K": "Jammu & Kashmir",
        "Orissa": "Odisha",
        "A & N Islands": "Andaman & Nicobar Islands",
        "A&N Islands": "Andaman & Nicobar Islands",
    }
    return aliases.get(text, text)


def normalise_operator(name: str) -> str:
    text = re.sub(r"\s+", " ", name.strip())
    if text in OPERATOR_ALIASES:
        return OPERATOR_ALIASES[text]
    # pdfplumber sometimes splits operator names inside a cell ("R e l i a nce Com.");
    # match aliases on a whitespace-collapsed key so those still canonicalise.
    collapsed = re.sub(r"\s+", "", text).lower()
    for alias, canonical in OPERATOR_ALIASES.items():
        if collapsed == re.sub(r"\s+", "", alias).lower():
            return canonical
    return text


def parse_month_label(label: str) -> date | None:
    match = re.match(r"^([A-Za-z]+)-?/?\s*(\d{2,4})$", label.strip())
    if not match:
        return None
    month = _MONTHS.get(match.group(1)[:3].lower())
    if month is None:
        return None
    year = int(match.group(2))
    year = year + 2000 if year < 100 else year
    return date(year, month, 1)


def _table_groups(rows: list[tuple[int, int, int, list[str]]]) -> dict[tuple[int, int], list[tuple[int, list[str]]]]:
    """Group bronze rows into tables keyed by (page, table_idx)."""
    groups: dict[tuple[int, int], list[tuple[int, list[str]]]] = {}
    for page, table_idx, row_idx, cells in rows:
        groups.setdefault((page, table_idx), []).append((row_idx, cells))
    return {key: sorted(items) for key, items in groups.items()}


def _find_header(
    table: list[tuple[int, list[str]]],
) -> tuple[int, list[str], list[str]] | None:
    """Locate (header_idx, operator_header, month_row) in a subscriber annexure."""
    for pos, (_row_idx, cells) in enumerate(table):
        joined = " ".join(cells).lower()
        if "subscriber base" not in joined:
            continue
        if "wireless" in joined:
            segment = "wireless"
        elif "wireline" in joined:
            segment = "wireline"
        else:
            continue
        if pos + 2 >= len(table):
            return None
        _title_idx, title_cells = table[pos]
        header_idx, header_cells = table[pos + 1]
        month_idx, month_cells = table[pos + 2]
        _ = title_cells, header_idx, month_idx
        return pos, segment, month_cells
    return None


def parse_annexures(
    tables: dict[tuple[int, int], list[tuple[int, list[str]]]],
) -> tuple[list[dict], list[dict]]:
    """Parse subscriber annexures: (period_month, segment, lsa, operator, subscribers)."""
    records: list[dict] = []
    rejects: list[dict] = []
    for (page, table_idx), table in sorted(tables.items()):
        found = _find_header(table)
        if found is None:
            continue
        pos, segment, month_cells = found
        # operator per column: nearest non-empty header label at or left of the column
        header_cells = table[pos + 1][1]
        operator_by_col: dict[int, str] = {}
        current = ""
        for col, label in enumerate(header_cells):
            text = label.strip()
            if text:
                current = normalise_operator(text)
            if current and current.lower() not in ("lsa", "service area", "total", "net addition"):
                operator_by_col[col] = current
        month_by_col: dict[int, date] = {}
        for col, label in enumerate(month_cells):
            parsed = parse_month_label(label)
            if parsed is not None and col in operator_by_col:
                month_by_col[col] = parsed
        if not month_by_col:
            rejects.append(
                {
                    "reject_reason": f"annexure on page {page} table {table_idx} has no parsable month columns",
                    "record": json.dumps({"header": header_cells, "months": month_cells}),
                }
            )
            continue
        for _row_idx, cells in table[pos + 3 :]:
            label = cells[0].strip() if cells else ""
            if not label or label.lower() in ("total", "net addition", "rural subscribers"):
                break  # annexure body ends at the Total row
            lsa = normalise_lsa(label)
            for col, period in month_by_col.items():
                if col >= len(cells):
                    continue
                try:
                    value = parse_number(cells[col])
                except ValueError:
                    rejects.append(
                        {
                            "reject_reason": f"unparsable subscriber value {cells[col]!r}",
                            "record": json.dumps(
                                {"lsa": lsa, "operator": operator_by_col[col], "period": str(period), "raw": cells[col]}
                            ),
                        }
                    )
                    continue
                if value is None:
                    continue  # "-" = operator not present in this circle
                records.append(
                    {
                        "period_month": period,
                        "segment": segment,
                        "service_area": lsa,
                        "operator": operator_by_col[col],
                        "subscribers": value,
                    }
                )
    return records, rejects


_COUNTRY_TELEDENSITY = re.compile(
    r"Wireless(?:\s*\(mobile\))? Teledensity in the country (increased|decreased) "
    r"from (\d+\.?\d*)% at the end of ([A-Za-z]+)-\s*(\d{2,4})\s*to\s*(\d+\.?\d*)%",
    re.I,
)


def parse_country_teledensity(
    file_text: str, source_file: str, file_period: date
) -> tuple[list[dict], list[dict]]:
    records: list[dict] = []
    for match in _COUNTRY_TELEDENSITY.finditer(file_text or ""):
        month = _MONTHS.get(match.group(3)[:3].lower())
        if month is None:
            continue
        year = int(match.group(4))
        year = year + 2000 if year < 100 else year
        prev_date = date(year, month, 1)
        records.append(
            {
                "period_date": prev_date,
                "wireless_teledensity_pct": float(match.group(2)),
                "source_file": source_file,
            }
        )
        records.append(
            {
                "period_date": file_period,
                "wireless_teledensity_pct": float(match.group(5)),
                "source_file": source_file,
            }
        )
    return records, []


def _period_pair(quarter_end: date) -> list[date]:
    """A QPIR teledensity table shows the previous and current quarter-end."""
    total = quarter_end.year * 12 + (quarter_end.month - 4)  # minus 3 months, 0-indexed
    prev_year, prev_month = divmod(total, 12)
    return [date(prev_year, prev_month + 1, 1), quarter_end]


def _numeric_cells(cells: list[str]) -> list[float]:
    values = []
    for cell in cells:
        try:
            value = parse_number(cell)
        except ValueError:
            continue
        if value is not None:
            values.append(value)
    return values


def _is_lsa_fragment(text: str, known: set[str], fragments: set[str]) -> bool:
    if not text or any(ch.isdigit() for ch in text):
        return False
    lowered = text.lower()
    return any(name.lower().startswith(lowered) for name in known) or lowered in fragments


def parse_circle_teledensity(
    tables: dict[tuple[int, int], list[tuple[int, list[str]]]],
    known_lsas: set[str],
    period_dates: list[date],
) -> tuple[list[dict], list[dict]]:
    """Parse QPIR circle teledensity tables: 6 numbers per LSA row (rural/urban/total
    for the previous and current quarter-end). Rows that do not yield exactly 6
    values for a known LSA are rejected, not guessed."""
    records: list[dict] = []
    rejects: list[dict] = []
    fragments = {name.split()[0].lower() for name in known_lsas if name.split()}
    for (_page, _tidx), table in sorted(tables.items()):
        header_probe = " ".join(cell for _idx, row in table[:3] for cell in row).lower()
        if "tele" not in header_probe or "density" not in header_probe:
            continue
        rows = [row for _idx, row in table]
        for i, row in enumerate(rows):
            name_cell = next((c.strip() for c in row[:3] if c.strip()), "")
            if name_cell and not any(ch.isalpha() for ch in name_cell):
                continue  # Sl.No / page-number fragments
            numeric = _numeric_cells(row)
            if len(numeric) < 6:
                continue
            full_name = normalise_lsa(name_cell)
            if i + 1 < len(rows):
                next_name = next((c.strip() for c in rows[i + 1][:3] if c.strip()), "")
                if next_name and _is_lsa_fragment(next_name, known_lsas, fragments) and not any(
                    ch.isdigit() for ch in next_name
                ):
                    combined = normalise_lsa(f"{name_cell} {next_name}")
                    if combined.lower() in {k.lower() for k in known_lsas}:
                        full_name = combined
            if full_name.lower() == "all india" and len(numeric) == 6:
                match = "All India"
            else:
                match = next((k for k in known_lsas if k.lower() == full_name.lower()), None)
                if match is None and len(full_name) >= 6:
                    match = next(
                        (k for k in known_lsas if k.lower().startswith(full_name.lower()[:6])), None
                    )
            if match is None or len(period_dates) < 2:
                rejects.append(
                    {
                        "reject_reason": f"teledensity row not matched to a known LSA (name={full_name!r})",
                        "record": json.dumps({"row": row}),
                    }
                )
                continue
            if len(numeric) != 6:
                rejects.append(
                    {
                        "reject_reason": f"teledensity row has {len(numeric)} numeric cells, expected 6",
                        "record": json.dumps({"lsa": match, "values": numeric}),
                    }
                )
                continue
            for period, rural, urban, total in (
                (period_dates[0], numeric[0], numeric[1], numeric[2]),
                (period_dates[1], numeric[3], numeric[4], numeric[5]),
            ):
                records.append(
                    {
                        "period_date": period,
                        "service_area": match,
                        "rural_pct": rural,
                        "urban_pct": urban,
                        "total_pct": total,
                    }
                )
    return records, rejects


def build_trai(con, settings) -> dict[str, int]:
    run_id = base.make_run_id()
    rows = con.execute("""
        SELECT source_file, report_title, page, kind, table_idx, row_idx, cells_json
        FROM read_parquet('data/bronze/trai/*.parquet')
        ORDER BY source_file, page, table_idx, row_idx
    """).fetchall()
    # text-line groups must not enter annexure/table detection (they are prose)
    rows = [row for row in rows if row[3] == "table" or True]

    period_by_file: dict[str, date] = {}
    selection_path = Path("data/landing/_trai_selection.json")
    if selection_path.exists():
        for item in json.loads(selection_path.read_text()):
            fname = item["href"].split("/")[-1].split("?")[0].replace("%20", "_")
            period_by_file[fname] = date.fromisoformat(item["period"])

    by_file: dict[str, list[tuple[int, int, int, list[str]]]] = {}
    page_text: dict[str, dict[tuple[int, int], str]] = {}
    for source_file, _title, page, kind, table_idx, row_idx, cells_json in rows:
        by_file.setdefault(source_file, []).append((page, table_idx, row_idx, json.loads(cells_json)))
        if kind == "text_line":
            # narrative sentences wrap across pages - join per file, not per page
            page_text[source_file] = page_text.get(source_file, "") + " " + " ".join(json.loads(cells_json))

    subscribers: list[dict] = []
    rejects: list[dict] = []
    country_td: list[dict] = []
    circle_td: list[dict] = []
    known_lsas: set[str] = set()
    files_with_subscribers = 0

    for source_file, file_rows in sorted(by_file.items()):
        tables = _table_groups(file_rows)
        sub_records, sub_rejects = parse_annexures(tables)
        for record in sub_records:
            record["source_file"] = source_file
            known_lsas.add(record["service_area"])
        subscribers.extend(sub_records)
        for reject in sub_rejects:
            payload = json.loads(reject["record"])
            payload["source_file"] = source_file
            reject["record"] = json.dumps(payload)
            rejects.append(reject)
        if sub_records:
            files_with_subscribers += 1

        file_period = period_by_file.get(source_file)
        if file_period is None:
            continue
        td_records, _ = parse_country_teledensity(page_text.get(source_file, ""), source_file, file_period)
        for record in td_records:
            record["source_file"] = source_file
        country_td.extend(td_records)
        circle_records, circle_rejects = parse_circle_teledensity(
            tables, known_lsas, _period_pair(file_period)
        )
        for record in circle_records:
            record["source_file"] = source_file
        circle_td.extend(circle_records)
        for reject in circle_rejects:
            payload = json.loads(reject["record"])
            payload["source_file"] = source_file
            reject["record"] = json.dumps(payload)
            rejects.append(reject)

    def dedupe(records: list[dict], keys: tuple[str, ...]) -> list[dict]:
        latest: dict[tuple, dict] = {}
        for record in records:
            latest[tuple(record[key] for key in keys)] = record
        return list(latest.values())

    # Completeness rule: a (segment, period_month) group parsed with materially fewer
    # operators than the segment's best month is an upstream defect (e.g. Sep-2025's
    # wireless PDF has broken font encoding for two operator columns). Such groups
    # move to rejects instead of producing half-month totals downstream.
    operator_counts: dict[tuple[str, str], set[str]] = {}
    for record in subscribers:
        operator_counts.setdefault((record["segment"], str(record["period_month"])), set()).add(
            record["operator"]
        )
    max_operators: dict[str, int] = {}
    for (segment, _period), ops in operator_counts.items():
        max_operators[segment] = max(max_operators.get(segment, 0), len(ops))

    sub_clean = []
    for record in dedupe(subscribers, ("period_month", "segment", "service_area", "operator")):
        key = (record["segment"], str(record["period_month"]))
        if len(operator_counts[key]) < max_operators[record["segment"]] - 1:
            rejects.append(
                {
                    "reject_reason": (
                        f"incomplete month coverage: {len(operator_counts[key])} of "
                        f"{max_operators[record['segment']]} operators parsed "
                        "(upstream PDF encoding defect)"
                    ),
                    "record": json.dumps(record, default=str),
                }
            )
            continue
        sub_clean.append(record)
    country_clean = dedupe(country_td, ("period_date",))
    circle_clean = dedupe(circle_td, ("period_date", "service_area"))

    sub_schema = pa.schema(
        [
            ("period_month", pa.date32()),
            ("segment", pa.string()),
            ("service_area", pa.string()),
            ("operator", pa.string()),
            ("subscribers", pa.float64()),
            ("source_file", pa.string()),
        ]
    )
    written_subs = write_silver(
        silver_table_path(settings, "trai", "subscribers"),
        pa.Table.from_pylist(sub_clean, schema=sub_schema),
    )
    n_rejects = write_rejects(settings, rejects, source="trai", table="subscribers", run_id=run_id)

    country_schema = pa.schema(
        [
            ("period_date", pa.date32()),
            ("wireless_teledensity_pct", pa.float64()),
            ("source_file", pa.string()),
        ]
    )
    written_country = write_silver(
        silver_table_path(settings, "trai", "teledensity_country"),
        pa.Table.from_pylist(country_clean, schema=country_schema),
    )
    circle_schema = pa.schema(
        [
            ("period_date", pa.date32()),
            ("service_area", pa.string()),
            ("rural_pct", pa.float64()),
            ("urban_pct", pa.float64()),
            ("total_pct", pa.float64()),
            ("source_file", pa.string()),
        ]
    )
    written_circle = write_silver(
        silver_table_path(settings, "trai", "teledensity_circle"),
        pa.Table.from_pylist(circle_clean, schema=circle_schema),
    )
    LOGGER.info(
        "silver/trai: %d subscriber rows (%d files), %d country teledensity rows, "
        "%d circle teledensity rows, %d rejects",
        written_subs, files_with_subscribers, written_country, written_circle, n_rejects,
    )
    return {"rows": written_subs + written_country + written_circle, "rejects": n_rejects}
