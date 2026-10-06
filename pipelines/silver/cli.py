"""Silver build CLI.

    python -m pipelines.silver.cli build [--source X]
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from typing import Any

from ingestion.config import load_settings
from pipelines.silver import base, grid, npci, openmeteo, trai, worldbank

LOGGER = logging.getLogger("silver")

BUILDERS: dict[str, Callable[[Any, Any], dict[str, int]]] = {
    "worldbank": worldbank.build_worldbank,
    "openmeteo": openmeteo.build_openmeteo,
    "npci": npci.build_npci,
    "gridindia": grid.build_grid,
    "trai": trai.build_trai,
}


def cmd_build(args: argparse.Namespace) -> int:
    settings = load_settings()
    con = base.connect(settings)
    sources = [args.source] if args.source else sorted(BUILDERS)
    failed = False
    for source in sources:
        builder = BUILDERS[source]
        LOGGER.info("silver: building %s", source)
        try:
            stats = builder(con, settings)
            print(f"silver/{source}: rows={stats['rows']} rejects={stats['rejects']}")
        except Exception as exc:
            failed = True
            LOGGER.error("silver: FAILED building %s: %s", source, exc)
            raise
    _ = failed
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(prog="python -m pipelines.silver.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build_p = sub.add_parser("build", help="build silver tables from bronze")
    build_p.add_argument("--source", choices=sorted(BUILDERS), default=None)
    build_p.set_defaults(func=cmd_build)
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:
        LOGGER.error("SILVER BUILD FAILED: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
