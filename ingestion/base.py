"""Shared ingestion machinery: retrying HTTP, JSON guard, atomic Parquet writes,
run manifest, and the idempotent partition runner every source builds on."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import os
import random
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ingestion.config import Settings, load_settings

LOGGER = logging.getLogger(__name__)


class IngestionError(RuntimeError):
    """A source could not be ingested; the message is safe to show the user."""


class SchemaChangedError(IngestionError):
    """The upstream layout no longer matches the validated schema."""


@dataclass(frozen=True)
class PartitionPlan:
    """One idempotent write unit: a deterministic partition key and a table builder.

    ``build(existing)`` receives the current partition table when ``merge=True``
    (``None`` otherwise) and returns the full replacement table.
    """

    partition: str
    build: Callable[[pa.Table | None], pa.Table]
    merge: bool = False


@dataclass(frozen=True)
class RunResult:
    source: str
    partition: str
    path: str
    row_count: int
    checksum: str
    fetched_at: str
    run_id: str
    status: str  # "written" | "skipped-existing" | "skipped-up-to-date"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Ingester(ABC):
    """Base class for every source: plan -> build -> validate -> atomic write -> manifest."""

    source: ClassVar[str]
    required_columns: ClassVar[tuple[str, ...]] = ()

    retries: ClassVar[int] = 4
    backoff_base_s: ClassVar[float] = 1.5

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or load_settings()
        self._session = requests.Session()
        self._session.headers["User-Agent"] = self.settings.user_agent

    # ------------------------------------------------------------- planning
    @abstractmethod
    def plan(self, **opts: Any) -> list[PartitionPlan]:
        """Return the partitions to materialise for one run."""

    # ------------------------------------------------------------- HTTP
    def request(self, method: str, url: str, *, stream: bool = False, **kwargs: Any) -> requests.Response:
        """HTTP with exponential backoff on transport errors, 5xx and 429.

        Authoritative 4xx answers are returned to the caller untouched so sources
        can surface their own error payloads (e.g. OpenCelliD RATE_LIMITED).
        """
        kwargs.setdefault("timeout", self.settings.request_timeout_s)
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self._session.request(method, url, stream=stream, **kwargs)
                if resp.status_code < 400 or resp.status_code not in (429,) + tuple(range(500, 600)):
                    return resp
                last_error = IngestionError(
                    f"{method} {url} -> HTTP {resp.status_code}: {resp.text[:160]!r}"
                )
            except requests.RequestException as exc:
                last_error = exc
            if attempt < self.retries:
                delay = self.backoff_base_s * 2 ** (attempt - 1) + random.uniform(0, 0.5)
                LOGGER.warning("%s (attempt %d/%d) - retrying in %.1fs", last_error, attempt, self.retries, delay)
                time.sleep(delay)
        assert last_error is not None
        raise IngestionError(str(last_error)) from last_error

    def get_json(self, url: str, *, params: Any = None, attempts: int = 5, **kwargs: Any) -> Any:
        """GET and parse JSON, retrying when an origin serves HTML error pages
        (observed on api.worldbank.org: HTTP 502/200 with an HTML body)."""
        last_error: IngestionError | None = None
        for attempt in range(1, attempts + 1):
            resp = self.request("GET", url, params=params, **kwargs)
            content_type = resp.headers.get("content-type", "")
            if "json" not in content_type.lower():
                last_error = IngestionError(
                    f"expected JSON from {url}, got content-type {content_type!r} "
                    f"status {resp.status_code}: {resp.text[:120]!r}"
                )
            else:
                try:
                    return resp.json()
                except json.JSONDecodeError as exc:
                    last_error = IngestionError(f"unparseable JSON from {url}: {exc}; body: {resp.text[:120]!r}")
            if attempt < attempts:
                delay = self.backoff_base_s * 2 ** (attempt - 1) + random.uniform(0, 0.5)
                LOGGER.warning("%s (attempt %d/%d) - retrying in %.1fs", last_error, attempt, attempts, delay)
                time.sleep(delay)
        assert last_error is not None
        raise last_error

    # ------------------------------------------------------------- validation
    def validate(self, table: pa.Table, partition: str) -> None:
        missing = [col for col in self.required_columns if col not in table.column_names]
        if missing:
            raise SchemaChangedError(
                f"{self.source}/{partition}: missing expected columns {missing}; "
                f"got {table.column_names}"
            )
        if table.num_rows == 0:
            raise IngestionError(f"{self.source}/{partition}: produced 0 rows")

    # ------------------------------------------------------------- writes
    def partition_path(self, partition: str) -> Path:
        """Partition keys use '/' for directories and '=' for readability, e.g.
        ``city=delhi/archive/2024-06`` -> data/bronze/openmeteo/city=delhi/archive/2024-06.parquet"""
        return self.settings.bronze_source_dir(self.source) / f"{partition}.parquet"

    def atomic_write_table(self, path: Path, table: pa.Table) -> str:
        """Write Parquet to a temp file in the same directory, then os.replace."""
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
        return sha256_file(path)

    def atomic_write_stream(self, path: Path, chunks: Iterable[bytes]) -> str:
        """Stream bytes (a download) to a temp file, then os.replace."""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=path.parent)
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            with tmp.open("wb") as handle:
                for chunk in chunks:
                    handle.write(chunk)
            os.replace(tmp, path)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        return sha256_file(path)

    # ------------------------------------------------------------- manifest
    def append_manifest(self, result: RunResult) -> None:
        path = self.settings.manifest_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dataclasses.asdict(result), sort_keys=True) + "\n")

    # ------------------------------------------------------------- runner
    def run(
        self,
        plans: Sequence[PartitionPlan] | None = None,
        *,
        overwrite: bool = False,
        only: Sequence[str] | None = None,
    ) -> list[RunResult]:
        run_id = uuid.uuid4().hex[:12]
        to_run = list(plans) if plans is not None else self.plan()
        if only:
            wanted = set(only)
            to_run = [p for p in to_run if p.partition in wanted]
        results: list[RunResult] = []

        def record(partition: str, path: Path, rows: int, checksum: str, status: str) -> RunResult:
            result = RunResult(
                source=self.source,
                partition=partition,
                path=str(path),
                row_count=rows,
                checksum=checksum,
                fetched_at=utc_now_iso(),
                run_id=run_id,
                status=status,
            )
            self.append_manifest(result)
            return result

        for plan in to_run:
            path = self.partition_path(plan.partition)
            if plan.merge:
                # ParquetFile.read() avoids pyarrow's hive inference on key=value dirs.
                existing = pq.ParquetFile(path).read() if path.exists() else None
                table = plan.build(existing)
                if existing is not None and table.num_rows == existing.num_rows:
                    LOGGER.info(
                        "%s/%s: up to date (%d rows), skipping write",
                        self.source, plan.partition, existing.num_rows,
                    )
                    results.append(
                        record(plan.partition, path, existing.num_rows, sha256_file(path), "skipped-up-to-date")
                    )
                    continue
            elif path.exists() and not overwrite:
                LOGGER.info("%s/%s: exists, skipping (use overwrite=True to force)", self.source, plan.partition)
                results.append(record(plan.partition, path, 0, sha256_file(path), "skipped-existing"))
                continue
            else:
                table = plan.build(None)
            self.validate(table, plan.partition)
            checksum = self.atomic_write_table(path, table)
            result = record(plan.partition, path, table.num_rows, checksum, "written")
            LOGGER.info("%s/%s: wrote %d rows -> %s", self.source, plan.partition, result.row_count, path)
            results.append(result)
        return results
