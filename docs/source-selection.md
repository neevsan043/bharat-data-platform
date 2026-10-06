# Final Source Selection (Phase 0, Task 2)

Selected from the feasibility results in `docs/data-sources.md`. An honest note first:
the strict PASS/NEEDS-KEY pool contains only **4** sources (World Bank, Grid-India,
Open-Meteo, OpenCelliD). Because the platform design explicitly includes the **file-drop**
ingestion pattern, the two strongest MANUAL sources (NPCI UPI, TRAI) are selected as
documented manual drops rather than pretending they are automated. data.gov.in (FAIL) and
PLFS (MANUAL with a registration wall + TLS problems) are excluded.

## Final list — 7 sources (6 core + 1 optional)

| # | Source | Feasibility | Ingestion pattern | Refresh | Primary domain |
|---|---|---|---|---|---|
| 1 | NPCI UPI product statistics | MANUAL (browser export) | **file drop** | monthly batch | payments |
| 2 | World Bank open data API | PASS | **REST API pull** | annual (full backfill once) | macro context |
| 3 | Grid-India reports | PASS | **JSON listing API + CDN pull** | daily batch (+monthly) | energy |
| 4 | Open-Meteo | PASS | **API micro-batch** | daily incremental refresh | weather |
| 5 | OpenCelliD | NEEDS-KEY (free token) | **tokened CSV snapshot pull** | quarterly snapshot | telecom (flagship, geo) |
| 6 | TRAI performance & subscription reports | MANUAL (PDF) | **file drop + table extraction** | quarterly batch | telecom KPIs |
| 7 | RBI DBIE xlsx *(optional)* | MANUAL (browser UI) | **file drop** | irregular | macro-finance |

## How each source supports the platform story

- **1. NPCI UPI (file drop, monthly batch).** The headline Indian payments dataset: monthly
  UPI volume (Mn), value (₹ Cr) and banks-live counts. Ingestion = human downloads the
  monthly Excel from `npci.org.in/product/upi/product-statistics` into
  `data/landing/npci/`; the ingestion layer registers and normalises it (idempotent by
  month key). The discovered JSON/Excel endpoints are documented in case headless-browser
  automation is added later.
- **2. World Bank (API batch).** Fully automated, keyless, CC BY 4.0. Pulls India series
  (GDP, CPI inflation, population, internet-users share, unemployment) as the macro
  backbone that every other domain joins against for normalisation (e.g. UPI value as %
  of GDP, payments per capita).
- **3. Grid-India (scheduled batch).** Keyless JSON listing API + direct CDN files.
  Daily reports (grid frequency profile, VRE share) and monthly reports (GDAM generation
  mix) give the energy domain its KPIs. Files are fetched by querying the listing API and
  downloading only newer-than-last-file entries — cheap and polite.
- **4. Open-Meteo (micro-batch).** Keyless JSON, CC BY 4.0, generous free-tier limits.
  A one-time historical backfill per city (archive API) plus a daily incremental refresh
  (forecast/past-days API). This is the platform's stand-in for streaming: small,
  frequent, append-only micro-batches orchestrated by Dagster — no Kafka possible or
  needed at this scale.
- **5. OpenCelliD (tokened batch).** Free-registered token stored in `.env` (never in
  code). Quarterly India (or per-circle) CSV snapshot downloaded into bronze; the
  CC BY-SA 4.0 attribution obligation is tracked in the dashboard footer and docs.
- **6. TRAI (file drop + extraction).** Quarterly PDF performance-indicator reports and
  monthly subscription PDFs, dropped manually into `data/landing/trai/`, then parsed
  page-wise (chunked PDF parsing to respect the 8 GB budget) into bronze Parquet.
- **7. RBI DBIE (optional).** Manual xlsx drops for series World Bank lacks (policy repo
  rate, banking aggregates). Kept optional so Phase 1 is never blocked on it.

## How the sources support the analytics story

**Domain A — Digital payments (NPCI + World Bank).**
KPIs: monthly UPI volume/value, YoY/MoM growth, banks live on UPI, UPI value as % of GDP,
transactions per capita. World Bank supplies the normalisation denominators. Analytics
output: payments adoption trends and a simple "digital intensity index".

**Domain B — Energy × weather (Grid-India + Open-Meteo).**
KPIs: daily grid-frequency stability, VRE (renewable) share of generation, monthly
generation mix by source; weather side: daily max/min temperature, cooling-degree days for
state capitals. Analytics output: **the platform's flagship forecasting use case** —
electricity demand / peak-load behaviour explained and forecast with weather features
(join on date + region). Both datasets are daily/monthly, so the join is clean.

**Domain C — Telecom connectivity, the geo flagship (OpenCelliD + TRAI).**
KPIs: cell-tower density per state/telecom circle (OpenCelliD points via GeoPandas
spatial join against state boundaries), teledensity and subscriber counts per service
area (TRAI PDFs). Analytics output: an interactive "Digital Connectivity" dashboard —
tower-density maps, coverage-vs-subscribers comparison, quarterly trends. This is the
demonstration piece that uses GeoPandas and shows spatial data engineering.

**Cross-cutting.** All three domains land in gold marts (payments, macro, energy-weather,
telecom) served to Streamlit from DuckDB. The macro domain doubles as the shared
dimension layer (dates, states/circles, indicators) so the analytics story stays
relational rather than three disconnected charts.

## Excluded, and how to revisit

- **data.gov.in — FAIL.** Its CKAN API is retired, the new front-end requires bearer
  tokens that cannot currently be obtained/verified, and the site self-describes as a
  sandbox. Re-check at Phase 3+; if dataset APIs return, MGNREGA-style employment data
  could join Domain A/B as an extra batch source.
- **PLFS — MANUAL (excluded).** Free-registration wall, broken TLS chain, heavy unit-level
  files. Revisit only for a dedicated labour deep-dive; World Bank unemployment series
  covers the KPI need at portfolio scale.
- **RBI — kept optional (row 7).** Only worth automating if a macro-finance KPI (e.g.
  policy-rate timeline overlay) earns its place.
