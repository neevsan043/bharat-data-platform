# Phase Plan (Phase 0, Task 6)

Every phase has a goal, the files it creates, and acceptance criteria that are verified
by **running a command** — not by inspection. All phases must respect the constraints in
`AGENTS.md` (8 GB rules, allowed stack, no fake data).

---

## Phase 0 — Feasibility, selection, scaffold *(complete — 2026-10-06)*

**Goal:** know exactly which sources are usable and why; stand up the project skeleton
and all governing docs.

**Files:** `docs/data-sources.md`, `docs/source-selection.md`, `docs/architecture.md`,
`docs/phases.md`, `AGENTS.md`, `README.md`, `pyproject.toml`, `.gitignore`,
`.github/workflows/ci.yml`, folder tree, `tests/test_scaffold.py`, `data/samples/`.

**Acceptance criteria:**
- [x] Every source family has a live-verified entry (URL + evidence) in data-sources.md.
- [x] Scaffold exists with the required structure.
- [x] `pytest tests/test_scaffold.py` passes (verified with the system interpreter;
      re-run inside the venv in Phase 1).

---

## Phase 1 — Ingestion foundation

**Goal:** automated bronze ingestion for the three PASS sources + registrar for manual
drops; first Dagster asset graph.

**Files (as built):** `ingestion/{base,config,worldbank,openmeteo,gridindia,opencellid,drops,report_tables,cli,verify_bronze}.py`,
`pipelines/definitions.py`, `tests/test_{worldbank,openmeteo,gridindia,opencellid,drops}.py`,
`requirements-lock.txt`, `docs/bronze-report.md`.

**Acceptance criteria — all verified by running on 2026-10-06 (details in
docs/bronze-report.md):**
- [x] `python -m ingestion.cli fetch worldbank` writes 8 indicator partitions with 66
  annual rows each (1960–2025); re-running without `--overwrite` skips everything
  (`skipped-existing`). The API's intermittent HTML-5xx responses are absorbed by the
  retry/JSON guard.
- [x] `python -m ingestion.cli backfill openmeteo --start 2021-06-01 --end 2026-09-30`
  merges **daily** rows (not hourly — granularity chosen at implementation, one row per
  city-day) into month partitions for 10 cities; re-running `fetch openmeteo` is a no-op
  (19,550 rows unchanged, date-level dedup).
- [x] `python -m ingestion.cli fetch gridindia --report psp --latest 2` downloads the 2
  newest daily reports into `data/bronze/gridindia/` — 331 rows each from the
  machine-readable `.xls` variant (PDF fallback parses page-wise; raw files kept in
  `_raw/`). The original `DAILY_FREQUENCY_PROFILE` idea was swapped for the PSP report,
  which actually contains tables.
- [x] The six NPCI FY files in `data/landing/npci/` registered via
  `python -m ingestion.cli fetch npci`: 57 rows, sha256 ledger row per file, files moved
  to `data/landing/_processed/npci/`, re-run is a no-op.
- [x] `dagster definitions validate -m pipelines.definitions` passes, 6 bronze assets
  listed, and `dagster dev -m pipelines.definitions` serves the UI (HTTP 200 on
  `/server_info`).
- [x] `pytest -q` green (23 tests, offline via `data/samples/`); `ruff check .` clean;
  peak RSS across all ingesters ≤ ~139 MB (limit: 2 GB).
- [x] Honest failures documented: **opencellid cannot be ingested** — the upstream dump
  has 0 India rows and no 404/405/406 export (verified; the run fails loudly and writes
  nothing). **trai** has no dropped files yet (ingester ready, no-op verified).

---

## Phase 2 — TRAI landing, silver layer, quality gates, dbt gold marts
*(complete — 2026-10-06)*

**Goal:** land the TRAI telecom corpus (telecom flagship pivot after OpenCelliD's verified
absence of India data), clean bronze into typed silver with rejects, gate it with Great
Expectations, and build gold marts with dbt.

**Files (as built):** `data/landing/trai/` (32 PDFs + MANIFEST.md),
`pipelines/silver/{base,npci,worldbank,openmeteo,grid,trai,cli}.py`,
`quality/{suites,run}.py`, `transform/models/{staging,marts}/*.sql`,
`transform/models/*/schema.yml`, `transform/tests/*.sql` (5 custom),
`transform/macros/*.sql`, `pipelines/generate_data_dictionary.py`,
`docs/data-dictionary.md`, `tests/test_silver_parsers.py`.

**Acceptance criteria — all verified by running on 2026-10-06:**
- [x] TRAI landing: 8 quarterly QPIRs (2024-06 → 2026-03, yearly excluded) + 24 monthly
  Telecom Subscription reports downloaded with a MANIFEST.md (URL/size/pages); 31
  ingested to bronze, 1 rejected (`PR_No.19of2026_0.pdf`, scanned - no extractable text).
- [x] Silver built for all five sources with per-source rejects tables:
  worldbank 528 rows, openmeteo 21,428 (guwahati added for the North-Eastern region),
  npci 57 (Oct-2026 flagged `is_partial_month`, Apr/May gaps documented), gridindia 972
  (60 daily reports, PSP metrics pivoted), trai 2,892 (2,721 subscriber rows + 9 country
  + 162 circle teledensity; 593 rejects with reasons).
- [x] `python -m quality.run` — 51 GE checks (schema, ranges, freshness, uniqueness,
  row-count sanity) all pass; every check recorded in
  `data/silver/_quality/quality_results.parquet`; failure mode proven (in-memory probe
  fails as expected) and exit code 1 stops downstream work.
- [x] `dbt build --project-dir transform --profiles-dir transform` — **60/60 PASS**
  (7 staging views + 7 marts + schema tests + 5 custom business-rule tests: UPI ticket
  size band, UPI MoM growth bounds, grid shortage ≤ demand, GDP plausibility, India
  wireless total sanity; plus energy rows must carry weather).
- [x] `dbt docs generate` works; `docs/data-dictionary.md` generated from the dbt
  manifest + catalog (14 models).
- [x] Peak RSS across the whole chain: silver 305 MB, quality 320 MB, dbt 245 MB
  (limit: 2 GB). `pytest -q` green (30 tests); `ruff check .` clean.
- [x] Honest deviations recorded: bank-wise UPI concentration is absent (browser-gated
  upstream; no drop landed); circle teledensity covers the wireline table only (the
  wireless circle table's layout does not parse); Sep-2025 wireless subscriber month
  moved to rejects (upstream PDF font-encoding defect); grid PSP backfilled 60 days so
  the energy mart has a meaningful demand-vs-weather join.

---

## Phase 3 — Gold marts and KPIs

**Goal:** analytical marts in DuckDB for the three domains; KPI queries documented.

**Files:** `transform/models/marts/fct_upi_monthly.sql`, `dim_indicator.sql`,
`fct_grid_daily.sql`, `fct_weather_daily.sql`, `fct_generation_monthly.sql`,
`fct_telecom_kpi.sql`, `docs/kpis.md`, `tests/test_marts.py`.

**Acceptance criteria:**
- `dbt build --project-dir transform --profiles-dir transform` builds all marts with
  unique-key dbt tests passing.
- `python -c "import duckdb; print(duckdb.sql(\"ATTACH 'data/gold/bharat.duckdb'; SELECT count(*) FROM fct_upi_monthly\").fetchone())"`
  returns > 0 rows (same check pattern for the grid and weather marts).
- `docs/kpis.md` defines each KPI (name, formula, source marts, grain) — payments,
  energy-weather, telecom.
- `pytest -q` green; `ruff check .` clean.

---

## Phase 4 — Analytics and forecasting

**Goal:** the flagship analytics — energy demand/peak behaviour vs weather features —
plus the payments adoption KPI set, all reproducible scripts over gold.

**Files:** `analytics/energy_weather_study.py`, `analytics/payments_adoption.py`,
`analytics/forecast.py` (baseline + weather-featured model with backtesting),
`docs/analytics-methodology.md`.

**Acceptance criteria:**
- `python -m analytics.forecast --city delhi` writes `data/gold/analytics/forecast_metrics.json`
  containing backtested MAE/MAPE per model; the weather-featured model's documented
  comparison table is generated, not asserted.
- `python -m analytics.payments_adoption` writes the adoption index table into gold.
- Both scripts run end-to-end in < 2 GB peak RSS (verified with `/usr/bin/time -l`).
- `pytest -q` green; `ruff check .` clean.

---

## Phase 5 — Streamlit dashboard

**Goal:** the product surface — payments KPIs, energy-weather explorer with forecast
overlay, and the telecom geo flagship (GeoPandas map of tower density vs TRAI teledensity).

**Files:** `dashboards/app.py`, `dashboards/views/{payments,energy,telecom}.py`,
`dashboards/assets/attribution.md` (CC BY / CC BY-SA attributions).

**Acceptance criteria:**
- `streamlit run dashboards/app.py --server.headless true` serves the app;
  `curl -s -o /dev/null -w '%{http_code}' http://localhost:8501` returns 200 within 30 s.
- Every dataset shown cites its source and licence (attribution visible in the UI footer).
- Dashboard reads exclusively from `data/gold/` (grep-proof: no `data/bronze` or
  `data/silver` paths in dashboards/).
- `pytest -q` green; `ruff check .` clean.

---

## Phase 6 — Orchestration, freshness, polish

**Goal:** schedules for every ingestion cadence, freshness checks, CI fully green,
portfolio-ready README.

**Files:** `pipelines/schedules.py`, `pipelines/sensors.py` (landing-zone sensor),
`pipelines/freshness.py`, final `README.md`.

**Acceptance criteria:**
- `dagster dev -m pipelines.definitions` shows schedules for: openmeteo (daily),
  gridindia (daily), worldbank (monthly), and a landing-zone sensor; a materialization of
  every asset succeeds end-to-end from an empty `data/` directory (fresh-clone rehearsal).
- `dagster job list -m pipelines.definitions` exits 0 listing all jobs.
- `.github/workflows/ci.yml` passes on GitHub Actions (or local `act`-equivalent: ruff +
  pytest green).
- `pytest -q` and `ruff check .` green; every doc in `docs/` still matches reality.
