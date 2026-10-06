# Bharat Data Platform

A data engineering platform plus analytics product built entirely on **public Indian
datasets**: UPI payments (NPCI), macro indicators (World Bank), electricity grid reports
(Grid-India), weather (Open-Meteo), and telecom connectivity (OpenCelliD, TRAI).

The pipeline follows a **bronze → silver → gold** medallion layout on local files and
DuckDB, orchestrated by Dagster, transformed with dbt, validated with Great Expectations,
and served through Streamlit — all sized to run comfortably on an 8 GB Apple M3 MacBook
(no Docker, Spark, Kafka, Airflow, Kubernetes or Superset).

**Status:** Phase 1 complete — sources verified ([docs/data-sources.md](docs/data-sources.md)),
bronze ingestion live for World Bank, Open-Meteo, Grid-India and NPCI
([docs/bronze-report.md](docs/bronze-report.md)); OpenCelliD confirmed to carry no Indian
data upstream.

## Documentation

- [docs/data-sources.md](docs/data-sources.md) — verified sources: URLs, formats, licences, problems
- [docs/source-selection.md](docs/source-selection.md) — the final 7 sources and their platform/analytics roles
- [docs/architecture.md](docs/architecture.md) — bronze/silver/gold architecture and why each tool was chosen
- [docs/phases.md](docs/phases.md) — phase plan with verifiable acceptance criteria
- [AGENTS.md](AGENTS.md) — constraints and conventions for anyone (or any agent) working in this repo

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Analytics product

Three domains, one platform:

1. **Digital payments** — UPI adoption and growth KPIs, normalised against macro indicators.
2. **Energy × weather** — grid demand and renewable share explained and forecast with weather features.
3. **Telecom connectivity (flagship)** — cell-tower density maps by state/telecom circle with GeoPandas, combined with TRAI teledensity KPIs.
