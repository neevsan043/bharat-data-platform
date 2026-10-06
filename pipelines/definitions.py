"""Dagster asset graph for the bronze layer (Phase 1).

    dagster dev -m pipelines.definitions
"""

from __future__ import annotations

from typing import Any

import dagster as dg

from ingestion.drops import NpciIngester, TraiPdfIngester
from ingestion.gridindia import GridIndiaIngester
from ingestion.opencellid import OpenCelliDIngester
from ingestion.openmeteo import OpenMeteoIngester
from ingestion.worldbank import WorldBankIngester


def _materialize(ingester: Any, **opts: Any) -> dg.MaterializeResult:
    results = ingester.run(**opts)
    written = sum(r.row_count for r in results if r.status == "written")
    return dg.MaterializeResult(
        metadata={
            "partitions": len(results),
            "rows_written": written,
            "statuses": ", ".join(sorted({r.status for r in results})) or "no-op",
        }
    )


@dg.asset(description="World Bank India macro indicators -> data/bronze/worldbank")
def worldbank_bronze() -> dg.MaterializeResult:
    ingester = WorldBankIngester()
    return _materialize(ingester.run(ingester.plan()))


@dg.asset(description="Open-Meteo daily weather (archive merge + forecast snapshot) -> data/bronze/openmeteo")
def openmeteo_bronze() -> dg.MaterializeResult:
    return _materialize(OpenMeteoIngester())


@dg.asset(description="Grid-India daily PSP reports -> data/bronze/gridindia")
def gridindia_bronze() -> dg.MaterializeResult:
    ingester = GridIndiaIngester()
    return _materialize(ingester.run(ingester.plan(report="psp", latest=2)))


@dg.asset(description="NPCI UPI monthly stats from landing zone -> data/bronze/npci")
def npci_bronze() -> dg.MaterializeResult:
    return _materialize(NpciIngester())


@dg.asset(description="TRAI PDF reports from landing zone -> data/bronze/trai")
def trai_bronze() -> dg.MaterializeResult:
    return _materialize(TraiPdfIngester())


@dg.asset(description="OpenCelliD India towers (MCC 404/405) -> data/bronze/opencellid")
def opencellid_bronze() -> dg.MaterializeResult:
    return _materialize(OpenCelliDIngester())


defs = dg.Definitions(
    assets=[
        worldbank_bronze,
        openmeteo_bronze,
        gridindia_bronze,
        npci_bronze,
        trai_bronze,
        opencellid_bronze,
    ]
)
