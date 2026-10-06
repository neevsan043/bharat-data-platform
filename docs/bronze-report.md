# Bronze Ingestion Report (Phase 1)

**Run date:** 2026-10-05/06 (night), on the target 8 GB M3 MacBook.
Every ingester was executed for real; numbers below come from the run logs and from
`python -m ingestion.verify_bronze` (DuckDB over `data/bronze/**`). Manifest of every
run: `data/bronze/_manifest/manifest.jsonl`.

## Summary

| Source | Bronze rows | Files | Date range | Peak RSS | Status |
|---|---|---|---|---|---|
| worldbank | 528 | 8 | years 1960–2025 | ~121 MB | ingested |
| openmeteo | 19,550 | 650 | 2021-06-01 .. 2026-10-12 (archive to 2026-09-30; rest = 7-day forecasts) | ~79 MB | ingested |
| gridindia | 662 | 2 | report dates 2026-10-04, 2026-10-05 | ~139 MB | ingested |
| npci | 57 | 6 | month starts 2021-06-01 .. 2026-10-01 | ~80 MB | ingested |
| trai | 0 | 0 | — | ~81 MB (no-op) | ready, no files dropped yet |
| opencellid | 0 | 0 | — | ~75 MB | **cannot ingest — upstream has no India data** (see below) |

**Peak memory across all runs: ~139 MB — far inside the 2 GB budget.**

## Per-source detail

### worldbank — ingested
- **Rows:** 528 (8 indicators × 66 annual observations each, 1960–2025).
- **Columns:** `indicator_id, indicator_name, country_iso3, year, value` (nulls kept for
  years without a published value, e.g. 2025 access-to-electricity).
- **Partitions:** `indicator=<code>.parquet`.
- **Indicators (all codes verified live by fetching them):** NY.GDP.MKTP.CD,
  NY.GDP.PCAP.CD, FP.CPI.TOTL.ZG, SP.POP.TOTL, IT.NET.USER.ZS, IT.CEL.SETS.P2,
  SL.UEM.TOTL.ZS, EG.ELC.ACCS.ZS.
- **Known problems:** the API intermittently serves HTML "5xx Error Page" bodies
  (observed over both HTTP/2 curl and urllib). The ingester's retry + JSON-content-type
  guard absorbed this; one full fetch needed six outer retry rounds. Spot values match
  earlier verification (GDP 2025 = US$3.956 tn).

### openmeteo — ingested
- **Rows:** 19,550 = 10 cities × 1,919 archive days (2021-06-01 .. 2026-09-30) + 10 × 7
  forecast rows.
- **Columns:** `city, latitude, longitude, date, temperature_2m_max, temperature_2m_min,
  precipitation_sum, wind_speed_10m_max, forecast_made_on`.
- **Partitions:** `<city>/archive/<yyyy-mm>.parquet` (merged, date-level dedup) and
  `<city>/forecast/<run-date>.parquet` (snapshot).
- **Known problems:** none. Re-run is a no-op (idempotency verified: row count unchanged).

### gridindia — ingested
- **Rows:** 662 = 2 daily PSP reports × 331 sheet rows each (5 sheets per workbook).
- **Columns:** `report_type, report_title, report_date, page, kind, table_idx, row_idx,
  cells_json, source_file`. Raw files kept under `bronze/gridindia/_raw/`.
- **What parsed reliably:** the machine-readable `.xls` version of the Daily Power Supply
  Position report (preferred over the PDF) — clean region-wise tables (Demand Met /
  Shortage at evening peak, Energy Met in MU by NR/WR/SR/ER/NER), state-wise ISTS bulk
  consumer rows, station-level tables.
- **What parsed less reliably (kept in bronze, flagged for silver):** the PDF variant —
  pdfplumber finds the ruled tables but page-1 Hindi text extracts with broken Devanagari
  spacing; column headers are multi-row and need stitching in silver. The listing API
  also occasionally returns truncated responses and repeats files under different MIME
  labels (deduped by FilePath; runs are idempotent so later runs pick up misses).

### npci — ingested
- **Rows:** 57 monthly rows across 6 FY partitions (`fy=2021-22` … `fy=2026-27`).
- **Columns:** `month_start, fy, volume_mn, avg_daily_volume_mn, value_cr,
  avg_daily_value_cr, source_file`. All six workbooks registered by sha256 and moved to
  `data/landing/_processed/npci/`.
- **Known problems (inherited from the source files, documented in data-sources.md):**
  April and May are missing for FY 2022-23 … 2025-26 (each per-FY file starts at June),
  and October-2026 is a partial month (4 days at download time). Schema validation guards
  against upstream layout changes (headers must match exactly).

### trai — ready, nothing ingested
- The ingester is implemented and unit-tested (page-wise PDF extraction, same schema as
  gridindia), but no PDF has been dropped into `data/landing/trai/` yet, so there is no
  bronze data and none was fabricated.

### opencellid — cannot be ingested (upstream problem, not ours)
- The ingestion path is fully implemented and verified end-to-end: token authorisation
  via POST to downloads.php, streaming download of the worldwide dump
  (119 MB gz), constant-memory stream-parse at 5,497,643 rows in ~5 s (peak 75 MB).
- **Verified finding:** the dump contains **zero** rows for MCC 404/405/406 and the
  downloads page offers no India country export — OpenCelliD currently holds no Indian
  cell towers (documented in data-sources.md). The run therefore fails loudly with a
  clear message and writes nothing, per the no-fake-data rule.
- **Consequence for the analytics story:** the telecom flagship's geo layer must lean on
  TRAI's circle-wise tables (service-area subscriber/teledensity data) instead of tower
  coordinates; revisit if OpenCelliD ever re-publishes India.

## Idempotency proof (re-runs)

- `fetch npci` → "no new files" (checksum ledger).
- `fetch openmeteo` → all partitions up to date / forecast snapshots skip; row count unchanged.
- `fetch gridindia --latest 2` → both partitions `skipped-existing`.
- `worldbank.run(plan)` without overwrite → all `skipped-existing`.
- DuckDB row counts after re-runs: npci 57, openmeteo 19,550 (unchanged).
