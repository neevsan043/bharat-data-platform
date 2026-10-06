"""Run Great Expectations gates over the silver tables.

    python -m quality.run

Writes every check to data/silver/_quality/quality_results.parquet and exits 1
if any expectation fails - downstream work (dbt) must not run on failed gates.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime

os.environ.setdefault("TQDM_DISABLE", "1")  # silence GX progress bars

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.config import load_settings
from pipelines.silver.base import silver_table_path
from quality.suites import GATES, to_dataframe

LOGGER = logging.getLogger("quality")

RESULTS_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("checked_at", pa.string()),
        ("source", pa.string()),
        ("table", pa.string()),
        ("expectation_type", pa.string()),
        ("success", pa.bool_()),
        ("observed_value", pa.string()),
        ("details", pa.string()),
    ]
)


def run() -> int:
    logging.basicConfig(level=logging.WARNING)
    settings = load_settings()
    run_id = uuid.uuid4().hex[:12]
    checked_at = datetime.now(UTC).isoformat(timespec="seconds")

    import great_expectations as gx

    context = gx.get_context()
    datasource = context.sources.add_pandas(f"silver_{run_id}")
    rows: list[dict] = []
    failures = 0

    for source, table, gate in GATES:
        path = silver_table_path(settings, source, table)
        if not path.exists():
            LOGGER.error("quality: silver table missing for gate %s/%s", source, table)
            rows.append(_row(run_id, checked_at, source, table, "table_exists", False, "missing", "{}"))
            failures += 1
            continue
        df = to_dataframe(pq.read_table(path), (source, table))
        asset = datasource.add_dataframe_asset(f"{source}_{table}")
        batch_request = asset.build_batch_request(dataframe=df)
        suite_name = f"{source}_{table}_suite"
        try:
            context.delete_expectation_suite(suite_name)
        except Exception:  # noqa: BLE001 - first run has no such suite
            pass
        suite = context.add_expectation_suite(suite_name)
        validator = context.get_validator(batch_request=batch_request, expectation_suite=suite)
        gate(validator)
        result = validator.validate(result_format="SUMMARY")
        for check in result.results:
            success = bool(check.success)
            if not success:
                failures += 1
            observed = json.dumps(check.result.get("observed_value", ""), default=str)
            details = json.dumps(
                {k: v for k, v in check.result.items() if k != "observed_value"}, default=str
            )
            rows.append(
                _row(
                    run_id, checked_at, source, table,
                    check.expectation_config.expectation_type, success, observed, details,
                )
            )
        LOGGER.info("quality: %s/%s -> %s", source, table, "PASS" if result.success else "FAIL")

    output = settings.data_dir / "silver" / "_quality" / "quality_results.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=RESULTS_SCHEMA), output)
    print(f"quality: {len(rows)} checks -> {output}; failures={failures}")
    return 1 if failures else 0


def _row(
    run_id: str, checked_at: str, source: str, table: str,
    expectation: str, success: bool, observed: str, details: str,
) -> dict:
    return {
        "run_id": run_id,
        "checked_at": checked_at,
        "source": source,
        "table": table,
        "expectation_type": expectation,
        "success": success,
        "observed_value": observed[:500],
        "details": details[:2000],
    }


if __name__ == "__main__":
    sys.exit(run())
