# AGENTS.md — working rules for this repository

Anyone (human or AI agent) working in this repo must follow these rules. They exist so
the project stays runnable on the target machine and every claim stays verifiable.

## Project goal

"Bharat Data Platform" — a portfolio-grade data engineering platform plus analytics
product over public Indian datasets: UPI payments (NPCI), macro indicators (World Bank,
optionally RBI), electricity grid reports (Grid-India), weather (Open-Meteo), and
telecom connectivity (OpenCelliD, TRAI). Medallion architecture (bronze → silver →
gold) on local Parquet + DuckDB, orchestrated by Dagster, transformed with dbt,
validated with Great Expectations, served with Streamlit. Phase plan and acceptance
criteria: `docs/phases.md`. Verified sources: `docs/data-sources.md`.

## Hard constraints (the 8 GB rules)

Target machine: MacBook, Apple M3, **8 GB RAM**. Non-negotiable:

1. **Banned tools:** Docker Desktop, Spark, Kafka, Airflow, Kubernetes, Superset. Do not
   introduce them or any resident server/JVM beyond the allowed stack.
2. **Allowed stack:** Python 3.11+ in a venv, Dagster, dbt-duckdb, DuckDB, Parquet,
   Great Expectations (library only), pandas/polars, Streamlit, GeoPandas.
3. **DuckDB first:** aggregations/joins over >~50 MB run as SQL with a connection opened
   `memory_limit='2GB', threads=4`. Never pull whole datasets into pandas.
4. **Polars lazy** (`scan_parquet` → `sink_parquet`) for heavy file transformations;
   pandas only for small frames, with `chunksize`/`read_only=True` for big text/Excel.
5. **Partition-wise work:** one source-month (or one report file) per task; never
   materialise a full history in memory. PDF/Excel extraction is page-/sheet-wise.
6. **No placeholder or fake data.** Every number in docs traces to a live verification;
   every dataset in bronze traces to a real download or manual drop. If a source cannot
   be verified, mark it FAIL/MANUAL — never fabricate.

## Folder responsibilities

| Path | Responsibility |
|---|---|
| `ingestion/` | One module per source: API/file fetchers + manual-drop registrar. Writes only to `data/bronze/`. |
| `pipelines/` | Dagster asset definitions, jobs, schedules wiring ingestion → quality → transform. |
| `transform/` | The dbt-duckdb project (`dbt_project.yml`, `profiles.yml`, `models/staging`, `models/marts`). |
| `quality/` | Great Expectations suites + validation assets (library usage only). |
| `analytics/` | KPI/forecasting studies reading **only** from `data/gold/`. |
| `dashboards/` | Streamlit app reading **only** from `data/gold/`. |
| `data/bronze,silver,gold/` | Medallion zones. Entirely git-ignored; rebuilt from code. |
| `data/landing/<source>/` | Landing zone: human-downloaded source files (NPCI Excel, TRAI PDF) awaiting registration into bronze. |
| `data/samples/` | Tiny verified samples kept as local evidence from Phase 0. |
| `docs/` | Source feasibility, selection, architecture, phase plan. Update when reality changes. |
| `tests/` | pytest suite (structural + unit). Must stay green. |
| `.github/workflows/` | CI: ruff + pytest on push/PR. |

## Coding conventions

- `from __future__ import annotations` and type hints on every function signature.
- Small functions (aim ≤ 40 lines), one module per source, no god-files.
- **Idempotent ingestion:** deterministic output paths (source + period key), skip when
  the artifact exists, explicit `overwrite=True` to force, write atomically
  (tmp file → `os.replace`). Re-running any task is always safe.
- `logging` module with a per-module logger; never `print` in library/pipeline code.
- Configuration via environment variables / `.env` (python-dotenv). **No secrets in
  code** — the OpenCelliD token and any future keys live in `.env` (git-ignored) and are
  read through a small config helper.
- SQL lives in dbt models; Python orchestrates, it does not embed large SQL strings.
- ruff must pass (`ruff check .`); pytest must pass (`pytest -q`) before any commit.
- Docstrings are one-liners stating purpose, not narration of the code.

## How to run things

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"            # pins resolved at install time

pytest -q                          # test suite
ruff check .                       # lint

dagster dev -m pipelines.definitions   # orchestration UI (from Phase 1 on)
python -m pipelines.silver.cli build  # bronze -> silver cleaning (Phase 2)
python -m quality.run                 # GE gates; exit 1 blocks downstream (Phase 2)
dbt build --project-dir transform --profiles-dir transform   # silver staging + gold marts
python -m ingestion.verify_bronze     # DuckDB bronze summary (Phase 1)
streamlit run dashboards/app.py    # dashboard (from Phase 5 on)
```

Landing zone: put the downloaded NPCI monthly Excel in `data/landing/npci/` and TRAI
quarterly PDFs in `data/landing/trai/`, then run the matching ingestion module to
register them into bronze.

## Definition of done — per phase

A phase is done when every acceptance criterion in `docs/phases.md` for that phase
passes **by running the listed commands on this machine**, tests and lint are green, and
docs touched by the change (sources, architecture, phases) are updated in the same
change. "Works on my screen" without a passing command is not done.
