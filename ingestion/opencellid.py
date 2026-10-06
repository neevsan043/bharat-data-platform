"""OpenCelliD ingester: token-gated CSV exports, streamed in chunks, filtered to India.

Verified mechanism (2026-10-06): POST the token to https://opencellid.org/downloads.php
to authorise the session, then GET https://opencellid.org/ocid/downloads with the token.
Downloads are limited to 2 per file per day.

VERIFIED DATA FACT (2026-10-06): the worldwide dump (cell_towers.csv.gz, 5,497,643 rows)
contains ZERO rows for MCC 404/405/406 and the downloads page offers no 404/405/406
country export - OpenCelliD currently holds no Indian cell towers. This ingester is
fully implemented and will produce real India data if the upstream ever publishes it;
today a full run fails loudly with that finding instead of writing empty data.
"""

from __future__ import annotations

import csv
import gzip
import logging
import os
import re
import tempfile
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.base import Ingester, IngestionError, RunResult, SchemaChangedError, sha256_file
from ingestion.config import Settings

LOGGER = logging.getLogger(__name__)

DOWNLOADS_PAGE = "https://opencellid.org/downloads.php"
DOWNLOAD_URL = "https://opencellid.org/ocid/downloads"
FULL_DUMP_FILE = "cell_towers.csv.gz"
RAW_DIR_NAME = "_raw"

INDIA_MCCS: ClassVar[tuple[int, ...]] = (404, 405)  # per ITU; 406 is also India, absent upstream
BATCH_ROWS = 250_000

EXPECTED_COLUMNS: ClassVar[tuple[str, ...]] = (
    "radio",
    "mcc",
    "net",
    "area",
    "cell",
    "unit",
    "lon",
    "lat",
    "range",
    "samples",
    "changeable",
    "created",
    "updated",
    "averageSignal",
)

SCHEMA = pa.schema(
    [
        ("radio", pa.string()),
        ("mcc", pa.int16()),
        ("net", pa.int16()),
        ("area", pa.int32()),
        ("cell", pa.int64()),
        ("lon", pa.float64()),
        ("lat", pa.float64()),
        ("range_m", pa.int32()),
        ("samples", pa.int32()),
        ("changeable", pa.int8()),
        ("created_unix", pa.int64()),
        ("updated_unix", pa.int64()),
        ("average_signal", pa.int16()),
    ]
)

_DOWNLOAD_LINK_RE = re.compile(r'href="https://opencellid\.org/ocid/downloads\?[^"]*type=full[^"]*"')


def _to_int(value: str) -> int | None:
    return int(value) if value.strip() else None


def _to_float(value: str) -> float | None:
    return float(value) if value.strip() else None


def map_row(row: list[str]) -> dict[str, Any]:
    """Map one CSV row (header order per EXPECTED_COLUMNS) to the bronze schema."""
    # header: radio,mcc,net,area,cell,unit,lon,lat,range,samples,changeable,created,updated,averageSignal
    return {
        "radio": row[0] or None,
        "mcc": _to_int(row[1]),
        "net": _to_int(row[2]),
        "area": _to_int(row[3]),
        "cell": _to_int(row[4]),
        "lon": _to_float(row[6]),
        "lat": _to_float(row[7]),
        "range_m": _to_int(row[8]),
        "samples": _to_int(row[9]),
        "changeable": _to_int(row[10]),
        "created_unix": _to_int(row[11]),
        "updated_unix": _to_int(row[12]),
        "average_signal": _to_int(row[13]),
    }


def iter_india_batches(
    gz_path: Path, mccs: tuple[int, ...] = INDIA_MCCS, batch_rows: int = BATCH_ROWS
) -> Iterator[pa.Table]:
    """Stream a gzipped world CSV in constant memory, yielding India-only Arrow batches."""
    wanted = set(mccs)
    with gzip.open(gz_path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != list(EXPECTED_COLUMNS):
            raise SchemaChangedError(
                f"opencellid: CSV header changed - expected {list(EXPECTED_COLUMNS)}, got {header}"
            )
        batch: list[dict[str, Any]] = []
        scanned = 0
        for row in reader:
            scanned += 1
            if len(row) != len(EXPECTED_COLUMNS):
                raise SchemaChangedError(
                    f"opencellid: row {scanned} has {len(row)} fields, expected {len(EXPECTED_COLUMNS)}"
                )
            if int(row[1]) in wanted:
                batch.append(map_row(row))
                if len(batch) >= batch_rows:
                    yield pa.Table.from_pylist(batch, schema=SCHEMA)
                    batch = []
        LOGGER.info("opencellid: scanned %d worldwide rows", scanned)
        if batch:
            yield pa.Table.from_pylist(batch, schema=SCHEMA)


class OpenCelliDIngester(Ingester):
    """Overrides ``run``: the source streams a multi-hundred-MB gz in fixed batches."""

    source = "opencellid"
    required_columns = tuple(field.name for field in SCHEMA)

    def __init__(self, settings: Settings | None = None) -> None:
        super().__init__(settings)
        if not self.settings.opencellid_token:
            raise IngestionError(
                "OPENCELLID_TOKEN is not set - add it to .env (see .env.example). "
                "Tokens come from a free account at https://my.opencellid.org/register"
            )

    def plan(self, **_: Any) -> list[Any]:  # interface compat - run() is overridden
        return []

    def _authorised_download(self, url: str, dest: Path) -> str:
        """Stream a download to dest via tmp+rename; surface OpenCelliD's JSON errors."""
        resp = self.request(
            "GET",
            url,
            stream=True,
            headers={"Referer": DOWNLOADS_PAGE},
            timeout=(self.settings.request_timeout_s, 600),
        )
        chunks = resp.iter_content(chunk_size=1 << 20)
        head = bytearray()
        for chunk in chunks:
            head.extend(chunk)
            if len(head) >= 512:
                break
        if head.lstrip().startswith(b"{"):
            raise IngestionError(f"opencellid: download rejected: {bytes(head)[:200]!r}")
        if not head.startswith(b"\x1f\x8b"):
            raise IngestionError(f"opencellid: unexpected file header {bytes(head[:8])!r} (not gzip)")

        def body() -> Iterator[bytes]:
            yield bytes(head)
            for chunk in chunks:
                if chunk:
                    yield chunk

        return self.atomic_write_stream(dest, body())

    def run(
        self,
        plans: Any = None,
        *,
        overwrite: bool = False,
        only: Any = None,
        mccs: tuple[int, ...] = INDIA_MCCS,
    ) -> list[RunResult]:
        _ = plans, only
        run_id = uuid.uuid4().hex[:12]
        token = self.settings.opencellid_token
        assert token is not None  # enforced in __init__

        snapshot = date.today()
        partition = f"mcc={'-'.join(str(m) for m in mccs)}/snapshot={snapshot}"
        path = self.partition_path(partition)
        raw_path = self.settings.bronze_source_dir(self.source) / RAW_DIR_NAME / FULL_DUMP_FILE

        if not raw_path.exists() or overwrite:
            LOGGER.info("opencellid: authorising session and streaming %s", FULL_DUMP_FILE)
            self.request("POST", DOWNLOADS_PAGE, data={"token": token})
            url = f"{DOWNLOAD_URL}?token={token}&type=full&file={FULL_DUMP_FILE}"
            checksum = self._authorised_download(url, raw_path)
            LOGGER.info("opencellid: raw dump saved %s (sha256 %s…)", raw_path, checksum[:12])
        else:
            LOGGER.info("opencellid: raw dump %s already present", raw_path)

        fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
        if path.exists() and not overwrite:
            LOGGER.info("opencellid/%s: exists, skipping (use overwrite=True to force)", partition)
            return [
                RunResult(
                    self.source, partition, str(path), pq.read_metadata(path).num_rows,
                    sha256_file(path), fetched_at, run_id, "skipped-existing",
                )
            ]

        writer: pa.parquet.ParquetWriter | None = None
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".parquet.tmp")
        os.close(fd)
        tmp = Path(tmp_name)
        total_rows = 0
        try:
            for batch in iter_india_batches(raw_path, mccs):
                total_rows += batch.num_rows
                if writer is None:
                    writer = pq.ParquetWriter(tmp, SCHEMA)
                writer.write_table(batch)
            if writer is not None:
                writer.close()
            if total_rows == 0:
                raise IngestionError(
                    "opencellid: 0 India (MCC 404/405) rows in the full worldwide dump - "
                    "verified 2026-10-06: OpenCelliD currently contains no Indian cell towers "
                    "and offers no 404/405/406 country export. Nothing written to bronze."
                )
            os.replace(tmp, path)
            result = RunResult(
                self.source, partition, str(path), total_rows, sha256_file(path),
                fetched_at, run_id, "written",
            )
            self.append_manifest(result)
            LOGGER.info("opencellid/%s: wrote %d rows -> %s", partition, total_rows, path)
            return [result]
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        finally:
            if writer is not None and writer.is_open:
                writer.close()
