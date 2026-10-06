"""Unit tests for Grid-India listing parsing and page-wise PDF extraction.

Real values come from the live listing captured in Phase 0 pre-flight and the
downloaded PSP sample PDF; no network is used.
"""

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.base import IngestionError
from ingestion.gridindia import parse_listing_payload, pick_per_date, slugify
from ingestion.report_tables import extract_pdf_rows

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
PSP_PDF = SAMPLES / "gridindia_nldc_psp_04-10-2026.pdf"

# Entry shape and values copied verbatim from the live listing API response
# (POST webapi.grid-india.in/api/v1/file, 2026-10-05).
LIVE_ENTRY = {
    "Title_": "04.10.26_NLDC_PSP",
    "Field2": "2026-10-05",
    "MimeType": "application/pdf",
    "FilePath": "files/grdw/2026/10/04.10.26_NLDC_PSP_639.pdf",
    "SizeInKb": 1630,
    "FileType": "DAILY_PSP_REPORT",
}


def test_slugify() -> None:
    assert slugify("Frequency 04.10.26") == "Frequency_04.10.26"
    assert slugify("04.10.26_NLDC_PSP") == "04.10.26_NLDC_PSP"


def test_parse_listing_sorts_newest_first() -> None:
    older = dict(LIVE_ENTRY, Title_="03.10.26_NLDC_PSP", Field2="2026-10-04")
    payload = {"retData": [older, LIVE_ENTRY], "dataLength": 2}
    entries = parse_listing_payload(payload, "DAILY_PSP_REPORT")
    assert entries[0]["Title_"] == "04.10.26_NLDC_PSP"


def test_parse_listing_bad_date_fails_loudly() -> None:
    payload = {"retData": [dict(LIVE_ENTRY, Field2="05-10-2026")]}
    with pytest.raises(IngestionError, match="unparsable Field2"):
        parse_listing_payload(payload, "DAILY_PSP_REPORT")


def test_pick_per_date_prefers_xls() -> None:
    pdf = dict(LIVE_ENTRY, FilePath="files/grdw/2026/10/04.10.26_NLDC_PSP_639.pdf")
    xls = dict(LIVE_ENTRY, FilePath="files/grdw/2026/10/04.10.26_NLDC_PSP_696.xls")
    older = dict(LIVE_ENTRY, Title_="03.10.26_NLDC_PSP", Field2="2026-10-04")
    picked = pick_per_date([pdf, xls, older], count=2)
    assert [entry["FilePath"].endswith(".xls") for entry in picked] == [True, False]
    assert len(picked) == 2


@pytest.mark.skipif(not PSP_PDF.exists(), reason="sample not present (git-ignored data/)")
def test_psp_pdf_extraction_page_wise() -> None:
    table = extract_pdf_rows(
        PSP_PDF,
        report_type="DAILY_PSP_REPORT",
        report_title="04.10.26_NLDC_PSP",
        report_date=date(2026, 10, 4),
        source_label="04.10.26_NLDC_PSP",
    )
    assert table.num_rows > 100  # 6 pages of text at minimum
    assert set(table.column("kind").to_pylist()) <= {"table", "text_line"}
    assert set(table.column("report_date").to_pylist()) == {date(2026, 10, 4)}
    # every row's cells_json parses back to a non-empty list
    first_cells = json.loads(table.column("cells_json").to_pylist()[0])
    assert isinstance(first_cells, list) and first_cells
