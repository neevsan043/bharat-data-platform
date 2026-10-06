"""Open-Meteo ingester: daily weather for 10 Indian cities.

Archive (ERA5) is backfilled/incrementally merged into month partitions with
date-level dedup; the 7-day forecast snapshot is stored once per run date. One
API call per city per mode; responses are split into month partitions locally.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.base import Ingester, IngestionError, RunResult, SchemaChangedError, sha256_file

LOGGER = logging.getLogger(__name__)

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API = "https://api.open-meteo.com/v1/forecast"

# Population-major Indian cities; coordinates are the standard city-centre pairs
# accepted verbatim by the Open-Meteo API (echoed back in every response).
CITIES: ClassVar[dict[str, tuple[float, float]]] = {
    "delhi": (28.6139, 77.2090),
    "mumbai": (19.0760, 72.8777),
    "kolkata": (22.5726, 88.3639),
    "chennai": (13.0827, 80.2707),
    "bengaluru": (12.9716, 77.5946),
    "hyderabad": (17.3850, 78.4867),
    "ahmedabad": (23.0225, 72.5714),
    "pune": (18.5204, 73.8567),
    "jaipur": (26.9124, 75.7873),
    "lucknow": (26.8467, 80.9462),
    "guwahati": (26.1445, 91.7362),
}

DAILY_VARS: ClassVar[tuple[str, ...]] = (
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
)

# Aligns the weather history with the NPCI UPI history (starts June 2021).
DEFAULT_START = date(2021, 6, 1)
# ERA5 archive lags a few days behind real time; stay safely behind the edge.
ARCHIVE_LAG_DAYS = 6

SCHEMA = pa.schema(
    [
        ("city", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("date", pa.date32()),
        ("temperature_2m_max", pa.float64()),
        ("temperature_2m_min", pa.float64()),
        ("precipitation_sum", pa.float64()),
        ("wind_speed_10m_max", pa.float64()),
        ("forecast_made_on", pa.date32()),
    ]
)


def parse_daily_payload(
    city: str, lat: float, lon: float, payload: dict[str, Any], made_on: date | None
) -> list[dict[str, Any]]:
    """Map an Open-Meteo daily payload to bronze rows (nulls kept as nulls)."""
    daily = payload.get("daily") or {}
    missing = [key for key in ("time", *DAILY_VARS) if key not in daily]
    if missing:
        raise SchemaChangedError(
            f"openmeteo/{city}: daily payload missing {missing}; keys={sorted(daily)}"
        )
    times = daily["time"]
    n = len(times)
    for var in DAILY_VARS:
        if len(daily[var]) != n:
            raise SchemaChangedError(
                f"openmeteo/{city}: {var} has {len(daily[var])} values for {n} dates"
            )
    records: list[dict[str, Any]] = []
    for i, iso_day in enumerate(times):
        try:
            day = date.fromisoformat(iso_day)
        except ValueError as exc:
            raise SchemaChangedError(f"openmeteo/{city}: bad date {iso_day!r}") from exc
        record: dict[str, Any] = {
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "date": day,
            "forecast_made_on": made_on,
        }
        for var in DAILY_VARS:
            record[var] = daily[var][i]
        records.append(record)
    return records


class OpenMeteoIngester(Ingester):
    """Overrides ``run`` because one city fetch feeds many month partitions."""

    source = "openmeteo"
    required_columns = ("city", "date")

    def plan(self, **_: Any) -> list[Any]:  # interface compat - run() is overridden
        return []

    # ------------------------------------------------------------- fetching
    def _fetch_archive(self, city: str, start: date, end: date) -> list[dict[str, Any]]:
        if end < start:
            return []
        lat, lon = CITIES[city]
        payload = self.get_json(
            ARCHIVE_API,
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "daily": ",".join(DAILY_VARS),
                "timezone": "Asia/Kolkata",
            },
        )
        return parse_daily_payload(city, lat, lon, payload, made_on=None)

    def _fetch_forecast(self, city: str) -> list[dict[str, Any]]:
        lat, lon = CITIES[city]
        payload = self.get_json(
            FORECAST_API,
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": ",".join(DAILY_VARS),
                "forecast_days": 7,
                "timezone": "Asia/Kolkata",
            },
        )
        return parse_daily_payload(city, lat, lon, payload, made_on=date.today())

    # ------------------------------------------------------------- helpers
    def _archive_dir(self, city: str) -> Path:
        return self.partition_path(f"city={city}/archive/x").parent

    def _latest_archive_date(self, city: str) -> date | None:
        month_files = sorted(self._archive_dir(city).glob("*.parquet"), reverse=True)
        for month_file in month_files:
            table = pq.ParquetFile(month_file).read(columns=["date"])
            if table.num_rows:
                dates = table.column("date").to_pylist()
                return max(d for d in dates if d is not None)
        return None

    @staticmethod
    def _month_key(day: date) -> str:
        return f"{day.year:04d}-{day.month:02d}"

    def _existing_months(self, city: str) -> set[str]:
        return {p.stem for p in self._archive_dir(city).glob("*.parquet")}

    def _merge_month_rows(
        self, existing: pa.Table | None, new_rows: list[dict[str, Any]]
    ) -> pa.Table | None:
        """Append only dates not already stored - re-runs never duplicate rows."""
        if existing is not None:
            have = set(existing.column("date").to_pylist())
            new_rows = [row for row in new_rows if row["date"] not in have]
        if not new_rows:
            return None
        new_table = pa.Table.from_pylist(new_rows, schema=SCHEMA)
        if existing is None:
            return new_table
        return pa.concat_tables([existing, new_table]).sort_by([("date", "ascending")])

    # ------------------------------------------------------------- runner
    def run(  # noqa: PLR0912 - linear per-mode flow, kept explicit on purpose
        self,
        plans: Any = None,
        *,
        overwrite: bool = False,
        only: Any = None,
        mode: str = "incremental",
        start: date | None = None,
        end: date | None = None,
        cities: list[str] | None = None,
        with_forecast: bool = True,
    ) -> list[RunResult]:
        _ = plans, only  # interface compat: partitions are derived here, not planned
        run_id = uuid.uuid4().hex[:12]
        cities = cities or sorted(CITIES)
        unknown = [c for c in cities if c not in CITIES]
        if unknown:
            raise IngestionError(f"unknown cities {unknown}; known: {sorted(CITIES)}")
        today = date.today()
        results: list[RunResult] = []

        if mode in ("incremental", "backfill", "archive"):
            results.extend(
                self._run_archive(run_id, cities, mode=mode, start=start, end=end, overwrite=overwrite)
            )
        if with_forecast and mode in ("incremental", "forecast"):
            results.extend(self._run_forecast(run_id, cities, today, overwrite=overwrite))
        return results

    def _run_archive(
        self,
        run_id: str,
        cities: list[str],
        *,
        mode: str,
        start: date | None,
        end: date | None,
        overwrite: bool,
    ) -> list[RunResult]:
        archive_end = end or (date.today() - timedelta(days=ARCHIVE_LAG_DAYS))
        results: list[RunResult] = []
        for city in cities:
            if mode == "incremental" and start is None:
                latest = self._latest_archive_date(city)
                fetch_start = latest + timedelta(days=1) if latest else DEFAULT_START
            else:
                fetch_start = start or DEFAULT_START
            rows = self._fetch_archive(city, fetch_start, archive_end)
            LOGGER.info(
                "openmeteo/%s: %d archive rows for %s..%s", city, len(rows), fetch_start, archive_end
            )
            by_month: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                by_month.setdefault(self._month_key(row["date"]), []).append(row)
            months = sorted(set(by_month) | self._existing_months(city))
            for month in months:
                partition = f"city={city}/archive/{month}"
                path = self.partition_path(partition)
                fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
                month_rows = by_month.get(month, [])
                if not month_rows and path.exists() and not overwrite:
                    results.append(self._record(run_id, partition, path, run_id_status="skipped-existing"))
                    continue
                existing = pq.ParquetFile(path).read() if path.exists() else None
                table = self._merge_month_rows(existing, month_rows)
                if table is None:
                    LOGGER.info("openmeteo/%s: %s up to date", city, partition)
                    results.append(self._record(run_id, partition, path, run_id_status="skipped-up-to-date"))
                    continue
                self.validate(table, partition)
                checksum = self.atomic_write_table(path, table)
                result = RunResult(
                    self.source, partition, str(path), table.num_rows, checksum, fetched_at, run_id, "written"
                )
                self.append_manifest(result)
                LOGGER.info("openmeteo/%s: wrote %d rows -> %s", city, table.num_rows, path)
                results.append(result)
        return results

    def _run_forecast(
        self, run_id: str, cities: list[str], today: date, *, overwrite: bool
    ) -> list[RunResult]:
        results: list[RunResult] = []
        for city in cities:
            partition = f"city={city}/forecast/{today}"
            path = self.partition_path(partition)
            fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
            if path.exists() and not overwrite:
                LOGGER.info("openmeteo/%s: forecast %s exists, skipping", city, partition)
                results.append(self._record(run_id, partition, path, run_id_status="skipped-existing"))
                continue
            rows = self._fetch_forecast(city)
            table = pa.Table.from_pylist(rows, schema=SCHEMA)
            self.validate(table, partition)
            checksum = self.atomic_write_table(path, table)
            result = RunResult(
                self.source, partition, str(path), table.num_rows, checksum, fetched_at, run_id, "written"
            )
            self.append_manifest(result)
            LOGGER.info("openmeteo/%s: wrote %d forecast rows -> %s", city, table.num_rows, path)
            results.append(result)
        return results

    def _record(self, run_id: str, partition: str, path: Path, *, run_id_status: str) -> RunResult:
        rows = pq.read_metadata(path).num_rows if path.exists() else 0
        result = RunResult(
            self.source,
            partition,
            str(path),
            rows,
            sha256_file(path) if path.exists() else "",
            datetime.now(UTC).isoformat(timespec="seconds"),
            run_id,
            run_id_status,
        )
        self.append_manifest(result)
        return result
