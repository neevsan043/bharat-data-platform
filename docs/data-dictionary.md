# Data Dictionary

Generated from the dbt project (`transform/`) - staging views over the silver
Parquet zone and gold marts in `data/gold/bharat.duckdb`. Regenerate with
`dbt docs generate` + `python -m pipelines.generate_data_dictionary`.

## Staging (schema `silver` in the DuckDB warehouse)

### silver.stg_grid_supply_daily
Grid-India daily supply position pivoted to columns (silver).

| Column | Type | Description |
|---|---|---|
| report_date | DATE |  |
| region_code | VARCHAR |  |
| region_name | VARCHAR |  |
| demand_met_evening_peak_mw | DOUBLE |  |
| shortage_evening_peak_mw | DOUBLE |  |
| energy_met_mu | DOUBLE |  |

### silver.stg_npci_upi_monthly
Cleaned NPCI UPI monthly product statistics from silver.

| Column | Type | Description |
|---|---|---|
| month_start | DATE |  |
| fy | VARCHAR |  |
| volume_mn | DOUBLE |  |
| avg_daily_volume_mn | DOUBLE |  |
| value_cr | DOUBLE |  |
| avg_daily_value_cr | DOUBLE |  |
| is_partial_month | BOOLEAN |  |

### silver.stg_openmeteo_weather_daily
Cleaned daily weather observations for 10 cities from silver.

| Column | Type | Description |
|---|---|---|
| city | VARCHAR |  |
| latitude | DOUBLE |  |
| longitude | DOUBLE |  |
| date | DATE |  |
| temperature_2m_max | DOUBLE |  |
| temperature_2m_min | DOUBLE |  |
| precipitation_sum | DOUBLE |  |
| wind_speed_10m_max | DOUBLE |  |

### silver.stg_trai_subscribers
TRAI subscriber base by circle x operator x month (silver).

| Column | Type | Description |
|---|---|---|
| period_month | DATE |  |
| segment | VARCHAR |  |
| service_area | VARCHAR |  |
| operator | VARCHAR |  |
| subscribers | DOUBLE |  |

### silver.stg_trai_teledensity_circle

| Column | Type | Description |
|---|---|---|
| period_date | DATE |  |
| service_area | VARCHAR |  |
| rural_pct | DOUBLE |  |
| urban_pct | DOUBLE |  |
| total_pct | DOUBLE |  |

### silver.stg_trai_teledensity_country

| Column | Type | Description |
|---|---|---|
| period_date | DATE |  |
| wireless_teledensity_pct | DOUBLE |  |

### silver.stg_worldbank_indicators
Cleaned World Bank India indicator series from silver.

| Column | Type | Description |
|---|---|---|
| indicator_id | VARCHAR |  |
| indicator_name | VARCHAR |  |
| country_iso3 | VARCHAR |  |
| year | INTEGER |  |
| value | DOUBLE |  |

## Gold marts (schema `gold`)

### gold.dim_date
Date dimension, one row per day from 2021-06-01 (UPI history start) to today.

| Column | Type | Description |
|---|---|---|
| date_key | INTEGER |  |
| date_day | DATE |  |
| year | BIGINT |  |
| quarter | BIGINT |  |
| month | BIGINT |  |
| month_name | VARCHAR |  |
| year_month | VARCHAR |  |
| week_of_year | INTEGER |  |
| is_month_start | BOOLEAN |  |

### gold.dim_geography
Circles (TRAI LSAs), grid regions and weather cities with the project's circle-to-region mapping.

| Column | Type | Description |
|---|---|---|
| geography_key | VARCHAR |  |
| level | VARCHAR |  |
| geography_name | VARCHAR |  |
| grid_region | VARCHAR |  |

### gold.fct_energy_demand_weather_daily
Regional daily demand/shortage joined with city-aggregated weather.

| Column | Type | Description |
|---|---|---|
| report_date | DATE |  |
| region_name | VARCHAR |  |
| demand_met_evening_peak_mw | DOUBLE |  |
| shortage_evening_peak_mw | DOUBLE |  |
| energy_met_mu | DOUBLE |  |
| cities_reported | BIGINT |  |
| avg_temp_max_c | DOUBLE |  |
| max_temp_max_c | DOUBLE |  |
| avg_temp_min_c | DOUBLE |  |
| avg_precipitation_mm | DOUBLE |  |

### gold.fct_macro_indicators
World Bank indicators with YoY growth.

| Column | Type | Description |
|---|---|---|
| indicator_id | VARCHAR |  |
| indicator_name | VARCHAR |  |
| year | INTEGER |  |
| value | DOUBLE |  |
| yoy_growth_pct | DOUBLE |  |

### gold.fct_telecom_subscribers
Subscriber base by circle x operator x month with growth and circle share.

| Column | Type | Description |
|---|---|---|
| period_month | DATE |  |
| segment | VARCHAR |  |
| service_area | VARCHAR |  |
| operator | VARCHAR |  |
| subscribers | DOUBLE |  |
| subscribers_mom_growth_pct | DOUBLE |  |
| circle_share_pct | DOUBLE |  |
| circle_total | DOUBLE |  |

### gold.fct_telecom_teledensity_circle
Circle teledensity (wireline) by quarter.

| Column | Type | Description |
|---|---|---|
| period_date | DATE |  |
| service_area | VARCHAR |  |
| rural_pct | DOUBLE |  |
| urban_pct | DOUBLE |  |
| total_pct | DOUBLE |  |

### gold.fct_upi_monthly
UPI volume/value with MoM and YoY growth. Bank-wise concentration is not available (NPCI bank-wise stats are browser-gated; no drop landed).

| Column | Type | Description |
|---|---|---|
| month_start | DATE |  |
| volume_mn | DOUBLE |  |
| value_cr | DOUBLE |  |
| avg_daily_volume_mn | DOUBLE |  |
| avg_daily_value_cr | DOUBLE |  |
| avg_ticket_size_inr | DOUBLE |  |
| volume_mom_growth_pct | DOUBLE |  |
| value_mom_growth_pct | DOUBLE |  |
| volume_yoy_growth_pct | DOUBLE |  |
| value_yoy_growth_pct | DOUBLE |  |
| is_partial_month | BOOLEAN |  |

