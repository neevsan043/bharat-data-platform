"""Report-table extraction for Grid-India and TRAI: PDF (page-wise) and legacy XLS (sheet-wise).

Both paths produce the same bronze schema. Pages/sheets are processed one at a time
(never the whole document in memory). PDFs also capture raw text lines, so bronze
keeps content even when table detection fails - reliability is reported, not assumed.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path

import pyarrow as pa

from ingestion.base import IngestionError

LOGGER = logging.getLogger(__name__)

REPORT_ROWS_SCHEMA = pa.schema(
    [
        ("report_type", pa.string()),
        ("report_title", pa.string()),
        ("report_date", pa.date32()),
        ("page", pa.int32()),  # PDF page number / XLS sheet index (1-based)
        ("kind", pa.string()),  # "table" | "text_line" | "sheet_row"
        ("table_idx", pa.int32()),
        ("row_idx", pa.int32()),
        ("cells_json", pa.string()),
        ("source_file", pa.string()),
    ]
)

REPORT_ROWS_COLUMNS = tuple(field.name for field in REPORT_ROWS_SCHEMA)


def _base_row(
    report_type: str, report_title: str, report_date: date, page: int, kind: str,
    table_idx: int, row_idx: int, cells: list[str], source_file: str,
) -> dict[str, object]:
    return {
        "report_type": report_type,
        "report_title": report_title,
        "report_date": report_date,
        "page": page,
        "kind": kind,
        "table_idx": table_idx,
        "row_idx": row_idx,
        "cells_json": json.dumps(cells, ensure_ascii=False),
        "source_file": source_file,
    }


def extract_pdf_rows(
    pdf_path: Path,
    *,
    report_type: str,
    report_title: str,
    report_date: date,
    source_label: str,
    max_pages: int = 40,
) -> pa.Table:
    """Extract one bronze row per ruled-table row and per text line, page by page."""
    rows: list[dict[str, object]] = []
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        page_count = 0
        n_table_rows = 0
        for page_no, page in enumerate(pdf.pages[:max_pages], start=1):
            page_count = page_no
            text = page.extract_text() or ""
            for line_no, line in enumerate(text.splitlines()):
                line = line.strip()
                if line:
                    rows.append(
                        _base_row(
                            report_type, report_title, report_date, page_no,
                            "text_line", -1, line_no, [line], source_label,
                        )
                    )
            for table_idx, table in enumerate(page.extract_tables() or []):
                for row_idx, row in enumerate(table):
                    cells = ["" if cell is None else " ".join(str(cell).split()) for cell in row]
                    if any(cells):
                        rows.append(
                            _base_row(
                                report_type, report_title, report_date, page_no,
                                "table", table_idx, row_idx, cells, source_label,
                            )
                        )
                        n_table_rows += 1
    n_text_rows = len(rows) - n_table_rows
    if not rows:
        raise IngestionError(
            f"{source_label}: PDF yielded no text or tables in the first {max_pages} pages "
            "(likely a scanned document) - not ingesting an empty file"
        )
    LOGGER.info(
        "%s: %d pages -> %d table rows, %d text lines", source_label, page_count, n_table_rows, n_text_rows
    )
    return pa.Table.from_pylist(rows, schema=REPORT_ROWS_SCHEMA)


def extract_xls_rows(
    xls_path: Path,
    *,
    report_type: str,
    report_title: str,
    report_date: date,
    source_label: str,
) -> pa.Table:
    """Extract legacy .xls sheets row by row via xlrd (BIFF-only since xlrd 2.x)."""
    import xlrd

    book = xlrd.open_workbook(str(xls_path))
    rows: list[dict[str, object]] = []
    for sheet_idx in range(book.nsheets):
        sheet = book.sheet_by_index(sheet_idx)
        for row_idx in range(sheet.nrows):
            cells = [str(sheet.cell_value(row_idx, col_idx)) for col_idx in range(sheet.ncols)]
            if any(cell.strip() for cell in cells):
                rows.append(
                    _base_row(
                        report_type, report_title, report_date, sheet_idx + 1,
                        "sheet_row", sheet_idx, row_idx, cells, source_label,
                    )
                )
    if not rows:
        raise IngestionError(f"{source_label}: XLS workbook has no non-empty cells")
    LOGGER.info(
        "%s: %d sheets -> %d sheet rows", source_label, book.nsheets, len(rows)
    )
    return pa.Table.from_pylist(rows, schema=REPORT_ROWS_SCHEMA)
