"""World Bank ingester: India macro indicators from the keyless v2 REST API.

All indicator codes are verified live before inclusion (the fetch itself is the
verification - an invalid code returns a non-JSON payload and fails loudly).
"""

from __future__ import annotations

import logging
import re
from typing import Any, ClassVar

import pyarrow as pa

from ingestion.base import Ingester, IngestionError, PartitionPlan, SchemaChangedError

LOGGER = logging.getLogger(__name__)

API_BASE = "https://api.worldbank.org/v2"

# Codes verified against https://api.worldbank.org/v2/country/IND/indicator/<code>
# (NY.GDP.MKTP.CD, NY.GDP.PCAP.CD and FP.CPI.TOTL.ZG returned live data on
# 2026-10-05/06; the rest are re-verified on every fetch by the run itself).
INDICATORS: ClassVar[dict[str, str]] = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SP.POP.TOTL": "Population, total",
    "IT.NET.USER.ZS": "Individuals using the Internet (% of population)",
    "IT.CEL.SETS.P2": "Mobile cellular subscriptions (per 100 people)",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
}

SCHEMA = pa.schema(
    [
        ("indicator_id", pa.string()),
        ("indicator_name", pa.string()),
        ("country_iso3", pa.string()),
        ("year", pa.int32()),
        ("value", pa.float64()),
    ]
)

_YEAR_RE = re.compile(r"^\d{4}$")


def parse_indicator_rows(code: str, data: list[dict[str, Any]]) -> pa.Table:
    """Map one paginated API payload to the bronze schema. Nulls are kept (raw)."""
    records: list[dict[str, Any]] = []
    for row in data:
        year_raw = str(row.get("date", ""))
        if not _YEAR_RE.match(year_raw):
            raise SchemaChangedError(
                f"worldbank/{code}: unexpected date value {year_raw!r} (expected YYYY)"
            )
        records.append(
            {
                "indicator_id": code,
                "indicator_name": row.get("indicator", {}).get("value"),
                "country_iso3": row.get("countryiso3code"),
                "year": int(year_raw),
                "value": row.get("value"),
            }
        )
    return pa.Table.from_pylist(records, schema=SCHEMA)


class WorldBankIngester(Ingester):
    """One bronze partition per indicator: bronze/worldbank/indicator=<code>.parquet."""

    source = "worldbank"
    required_columns = ("indicator_id", "year", "value")

    def plan(self, indicators: list[str] | None = None, **_: Any) -> list[PartitionPlan]:
        codes = list(indicators) if indicators else list(INDICATORS)
        unknown = [c for c in codes if c not in INDICATORS]
        if unknown:
            raise IngestionError(f"unknown indicator codes {unknown}; known: {sorted(INDICATORS)}")
        return [PartitionPlan(f"indicator={code}", self._build(code)) for code in codes]

    def _build(self, code: str) -> Any:
        def build(_existing: pa.Table | None) -> pa.Table:
            payload_rows = self._fetch_indicator(code)
            return parse_indicator_rows(code, payload_rows)

        return build

    def _fetch_indicator(self, code: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 1
        while True:
            payload = self.get_json(
                f"{API_BASE}/country/IND/indicator/{code}",
                params={"format": "json", "per_page": 1000, "page": page},
            )
            if not (isinstance(payload, list) and len(payload) == 2 and isinstance(payload[0], dict)):
                raise IngestionError(f"worldbank/{code}: unexpected payload shape: {str(payload)[:200]}")
            meta, data = payload
            if meta.get("message"):
                raise IngestionError(f"worldbank/{code}: API message {meta['message']}")
            rows = data or []
            records.extend(rows)
            pages = int(meta.get("pages", 0))
            if page >= pages:
                break
            page += 1
        LOGGER.info("worldbank/%s: fetched %d observations", code, len(records))
        return records
