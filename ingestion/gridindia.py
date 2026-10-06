"""Grid-India ingester: keyless JSON listing API + CDN downloads, page-wise parsing.

Verified endpoints (2026-10-05/06):
  POST https://webapi.grid-india.in/api/v1/file  body: {"_source":"GRDW","_type":<FileType>,...}
  files live on https://webcdn.grid-india.in/<FilePath>
Daily PSP reports exist as PDF **and** legacy .xls (saved by the NLDC control room);
the .xls is preferred because its cell values are machine-readable. The listing API
occasionally returns truncated responses - runs are idempotent, so a later run picks
up anything missed.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa

from ingestion.base import Ingester, IngestionError, PartitionPlan
from ingestion.report_tables import (
    REPORT_ROWS_COLUMNS,
    extract_pdf_rows,
    extract_xls_rows,
)

LOGGER = logging.getLogger(__name__)

LISTING_URL = "https://webapi.grid-india.in/api/v1/file"
CDN_BASE = "https://webcdn.grid-india.in/"

REPORT_TYPES: ClassVar[dict[str, str]] = {
    "psp": "DAILY_PSP_REPORT",
    "vre": "DAILY_VRE_REPORT",
    "frequency": "DAILY_FREQUENCY_PROFILE",
}

_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")
_PDF_MIME = ("application/pdf", "attachment/pdf")


def slugify(value: str) -> str:
    return _SLUG_RE.sub("_", value).strip("_")[:80]


def parse_listing_payload(payload: dict[str, Any], report_type: str) -> list[dict[str, Any]]:
    """Validate the listing envelope; return entries sorted by report date, deduped by file."""
    entries = payload.get("retData")
    if not isinstance(entries, list):
        raise IngestionError(
            f"gridindia/{report_type}: listing payload has no retData list: {str(payload)[:200]}"
        )
    dated = []
    for entry in entries:
        raw_date = entry.get("Field2") or ""
        try:
            report_date = date.fromisoformat(raw_date)
        except ValueError as exc:
            raise IngestionError(
                f"gridindia/{report_type}: entry {entry.get('Title_')!r} has unparsable "
                f"Field2 date {raw_date!r}"
            ) from exc
        dated.append((report_date, entry))
    dated.sort(key=lambda pair: pair[0], reverse=True)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for _, entry in dated:
        file_path = entry.get("FilePath", "")
        if file_path and file_path not in seen:
            seen.add(file_path)
            unique.append(entry)
    return unique


def pick_per_date(entries: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    """For the newest ``count`` report dates, prefer the .xls file over the PDF."""
    per_date: dict[date, dict[str, Any]] = {}
    order: list[date] = []
    for entry in entries:
        report_date = date.fromisoformat(entry["Field2"])
        if report_date not in per_date:
            per_date[report_date] = entry
            order.append(report_date)
        elif entry["FilePath"].endswith(".xls") and not per_date[report_date]["FilePath"].endswith(".xls"):
            per_date[report_date] = entry
    return [per_date[d] for d in sorted(order, reverse=True)[:count]]


class GridIndiaIngester(Ingester):
    """One bronze partition per report: bronze/gridindia/<TYPE>/<slug>.parquet."""

    source = "gridindia"
    required_columns = REPORT_ROWS_COLUMNS

    def plan(self, *, report: str = "psp", latest: int = 2, **_: Any) -> list[PartitionPlan]:
        if report not in REPORT_TYPES:
            raise IngestionError(f"unknown report {report!r}; known: {sorted(REPORT_TYPES)}")
        report_type = REPORT_TYPES[report]
        entries = pick_per_date(self._list_files(report_type), latest)
        if not entries:
            raise IngestionError(f"gridindia/{report_type}: listing returned no entries")
        return [
            PartitionPlan(f"{report_type}/{slugify(e['Title_'])}", self._build(report_type, e))
            for e in entries
        ]

    def _list_files(self, report_type: str) -> list[dict[str, Any]]:
        resp = self.request(
            "POST",
            LISTING_URL,
            json={"_source": "GRDW", "_type": report_type, "_fileDate": "", "_month": "00"},
        )
        resp.raise_for_status()
        entries = parse_listing_payload(resp.json(), report_type)
        LOGGER.info("gridindia/%s: %d unique files listed, newest %s", report_type, len(entries), entries[0]["Title_"])
        return entries

    def _build(self, report_type: str, entry: dict[str, Any]) -> Callable[[pa.Table | None], pa.Table]:
        def build(_existing: pa.Table | None) -> pa.Table:
            file_path: str = entry["FilePath"]
            title = entry["Title_"]
            slug = slugify(title)
            report_date = date.fromisoformat(entry["Field2"])
            mime = entry.get("MimeType", "")
            if file_path.endswith(".xls"):
                if "xls" not in mime and "excel" not in mime:
                    LOGGER.warning("gridindia: %s is .xls but MimeType is %r - proceeding", title, mime)
                url = CDN_BASE + file_path
                raw_path = self.settings.bronze_source_dir("gridindia") / "_raw" / report_type / f"{slug}.xls"
                self._download(url, raw_path)
                return extract_xls_rows(
                    raw_path, report_type=report_type, report_title=title,
                    report_date=report_date, source_label=title,
                )
            if file_path.endswith(".pdf") and any(m in mime for m in _PDF_MIME):
                url = CDN_BASE + file_path
                raw_path = self.settings.bronze_source_dir("gridindia") / "_raw" / report_type / f"{slug}.pdf"
                self._download(url, raw_path)
                return extract_pdf_rows(
                    raw_path, report_type=report_type, report_title=title,
                    report_date=report_date, source_label=title,
                )
            raise IngestionError(
                f"gridindia/{report_type}: file {title!r} ({file_path}, MimeType {mime!r}) "
                "is neither a PDF nor a legacy .xls report"
            )

        return build

    def _download(self, url: str, raw_path: Path) -> None:
        resp = self.request("GET", url, stream=True)
        if resp.status_code != 200:
            raise IngestionError(f"gridindia: CDN download failed HTTP {resp.status_code} for {url}")
        checksum = self.atomic_write_stream(raw_path, resp.iter_content(chunk_size=1 << 20))
        LOGGER.info("gridindia: raw report saved %s (sha256 %s…)", raw_path, checksum[:12])
