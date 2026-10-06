"""Ingestion CLI.

    python -m ingestion.cli list-sources
    python -m ingestion.cli fetch <source> [options]
    python -m ingestion.cli backfill <source> --start YYYY-MM-DD --end YYYY-MM-DD
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from typing import Any

from ingestion.base import IngestionError
from ingestion.drops import NpciIngester, TraiPdfIngester
from ingestion.gridindia import REPORT_TYPES, GridIndiaIngester
from ingestion.opencellid import INDIA_MCCS, OpenCelliDIngester
from ingestion.openmeteo import OpenMeteoIngester
from ingestion.worldbank import WorldBankIngester

LOGGER = logging.getLogger("ingestion")

SOURCES: dict[str, dict[str, str]] = {
    "worldbank": {
        "class": "WorldBankIngester",
        "pattern": "API pull (keyless JSON), one partition per indicator",
        "status": "PASS",
    },
    "openmeteo": {
        "class": "OpenMeteoIngester",
        "pattern": "API micro-batch; archive merged into month partitions + daily forecast snapshot",
        "status": "PASS",
    },
    "gridindia": {
        "class": "GridIndiaIngester",
        "pattern": "JSON listing API + CDN PDF download, page-wise table extraction",
        "status": "PASS",
    },
    "opencellid": {
        "class": "OpenCelliDIngester",
        "pattern": "tokened gz download, streamed + filtered to MCC 404/405",
        "status": "CANNOT INGEST: upstream holds no Indian towers (verified 2026-10-06)",
    },
    "npci": {
        "class": "NpciIngester",
        "pattern": "landing-zone Excel drop (data/landing/npci/), checksum-registered",
        "status": "PASS (manual drop)",
    },
    "trai": {
        "class": "TraiPdfIngester",
        "pattern": "landing-zone PDF drop (data/landing/trai/), page-wise extraction",
        "status": "PASS (manual drop; no files yet)",
    },
}


def _print_results(results: list[Any]) -> None:
    print(f"{'partition':50s} {'status':20s} {'rows':>9s}  path")
    for result in results:
        print(f"{result.partition:50s} {result.status:20s} {result.row_count:9d}  {result.path}")


def cmd_fetch(args: argparse.Namespace) -> int:
    if args.source == "worldbank":
        ingester = WorldBankIngester()
        results = ingester.run(ingester.plan(indicators=args.indicators), overwrite=args.overwrite)
    elif args.source == "openmeteo":
        results = OpenMeteoIngester().run(
            overwrite=args.overwrite, mode="incremental", cities=args.cities or None
        )
    elif args.source == "gridindia":
        ingester = GridIndiaIngester()
        results = ingester.run(
            ingester.plan(report=args.report, latest=args.latest), overwrite=args.overwrite
        )
    elif args.source == "opencellid":
        results = OpenCelliDIngester().run(overwrite=args.overwrite, mccs=tuple(args.mcc))
    elif args.source == "npci":
        results = NpciIngester().run()
    elif args.source == "trai":
        results = TraiPdfIngester().run()
    else:  # pragma: no cover - argparse choices prevent this
        raise IngestionError(f"unknown source {args.source}")
    _print_results(results)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    if args.source == "openmeteo":
        results = OpenMeteoIngester().run(
            mode="backfill",
            start=args.start,
            end=args.end,
            overwrite=args.overwrite,
            with_forecast=False,
        )
    elif args.source == "worldbank":
        ingester = WorldBankIngester()
        results = ingester.run(ingester.plan(indicators=args.indicators), overwrite=True)
    else:
        raise IngestionError(
            f"backfill applies to date-based sources (openmeteo, worldbank), not {args.source}"
        )
    _print_results(results)
    return 0


def cmd_list_sources(_: argparse.Namespace) -> int:
    print(f"{'source':12s} {'status':12s} pattern")
    for name, info in SOURCES.items():
        status = info["status"] if " " not in info["status"] else info["status"]
        print(f"{name:12s} {status:12s} {info['pattern']}")
        if " " in info["status"]:
            print(f"{'':12s} {'':12s} status: {info['status']}")
    return 0


def add_shared(p: argparse.ArgumentParser) -> None:
    p.add_argument("--overwrite", action="store_true", help="re-fetch and replace existing partitions")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m ingestion.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    list_p = sub.add_parser("list-sources", help="show sources and their ingestion patterns")
    list_p.set_defaults(func=cmd_list_sources)

    fetch_p = sub.add_parser("fetch", help="run one ingestion source")
    fetch_p.add_argument("source", choices=sorted(SOURCES))
    fetch_p.add_argument("--start", type=date.fromisoformat, help="range start (openmeteo)")
    fetch_p.add_argument("--end", type=date.fromisoformat, help="range end (openmeteo)")
    fetch_p.add_argument("--latest", type=int, default=2, help="gridindia: newest N files (default 2)")
    fetch_p.add_argument("--report", choices=sorted(REPORT_TYPES), default="psp", help="gridindia report type")
    fetch_p.add_argument("--mcc", type=int, nargs="+", default=list(INDIA_MCCS), help="opencellid MCC filter")
    fetch_p.add_argument("--indicators", nargs="*", default=None, help="worldbank indicator codes")
    fetch_p.add_argument("--cities", nargs="*", default=None, help="openmeteo city subset")
    add_shared(fetch_p)
    fetch_p.set_defaults(func=cmd_fetch)

    backfill_p = sub.add_parser("backfill", help="explicit historical range for date-based sources")
    backfill_p.add_argument("source", choices=("openmeteo", "worldbank"))
    backfill_p.add_argument("--start", type=date.fromisoformat, help="range start")
    backfill_p.add_argument("--end", type=date.fromisoformat, help="range end")
    backfill_p.add_argument("--indicators", nargs="*", default=None, help="worldbank indicator codes")
    add_shared(backfill_p)
    backfill_p.set_defaults(func=cmd_backfill)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except IngestionError as exc:
        LOGGER.error("INGESTION FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
