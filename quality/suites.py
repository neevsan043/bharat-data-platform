"""Great Expectations suites for the silver tables (library usage only).

Each builder receives a Validator and adds its expectations. Gates cover schema,
ranges, freshness, uniqueness and row-count sanity per docs/phases.md Phase 2.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa

NPCCI_COLUMNS = [
    "month_start", "fy", "volume_mn", "avg_daily_volume_mn", "value_cr",
    "avg_daily_value_cr", "is_partial_month", "source_file",
]
WEATHER_COLUMNS = [
    "city", "latitude", "longitude", "date", "temperature_2m_max",
    "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max",
]
WORLDBANK_COLUMNS = ["indicator_id", "indicator_name", "country_iso3", "year", "value"]
GRID_COLUMNS = ["report_date", "metric", "region_code", "region_name", "value"]
SUBSCRIBERS_COLUMNS = [
    "period_month", "segment", "service_area", "operator", "subscribers", "source_file",
]
COUNTRY_TD_COLUMNS = ["period_date", "wireless_teledensity_pct", "source_file"]
CIRCLE_TD_COLUMNS = ["period_date", "service_area", "rural_pct", "urban_pct", "total_pct", "source_file"]


def gate_npci(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(NPCCI_COLUMNS)
    v.expect_column_values_to_not_be_null("month_start")
    v.expect_column_values_to_not_be_null("volume_mn")
    v.expect_column_values_to_not_be_null("value_cr")
    v.expect_column_values_to_be_unique("month_start")
    v.expect_column_values_to_be_between("volume_mn", 0, 200_000)
    v.expect_column_values_to_be_between("value_cr", 100_000, 100_000_000)
    v.expect_table_row_count_to_be_between(40, 120)
    v.expect_column_max_to_be_between("month_start", pd.Timestamp("today") - pd.Timedelta(days=45))


def gate_weather(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(WEATHER_COLUMNS)
    v.expect_column_values_to_not_be_null("city")
    v.expect_column_values_to_not_be_null("date")
    v.expect_compound_columns_to_be_unique(["city", "date"])
    v.expect_column_values_to_be_between("temperature_2m_max", -20, 60)
    v.expect_column_values_to_be_between("temperature_2m_min", -30, 55)
    v.expect_column_values_to_be_between("precipitation_sum", 0, 1_000)
    v.expect_column_values_to_be_between("wind_speed_10m_max", 0, 250)
    v.expect_table_row_count_to_be_between(18_000, 25_000)
    v.expect_column_max_to_be_between("date", pd.Timestamp("today") - pd.Timedelta(days=10))
    v.expect_column_values_to_be_in_set(
        "city", ["delhi", "mumbai", "kolkata", "chennai", "bengaluru",
                 "hyderabad", "ahmedabad", "pune", "jaipur", "lucknow", "guwahati"]
    )


def gate_worldbank(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(WORLDBANK_COLUMNS)
    v.expect_compound_columns_to_be_unique(["indicator_id", "year"])
    v.expect_column_values_to_be_between("year", 1950, 2026)
    v.expect_table_row_count_to_be_between(500, 560)
    v.expect_column_max_to_be_between("year", 2024)
    v.expect_column_values_to_be_in_set(
        "indicator_id",
        ["NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "FP.CPI.TOTL.ZG", "SP.POP.TOTL",
         "IT.NET.USER.ZS", "IT.CEL.SETS.P2", "SL.UEM.TOTL.ZS", "EG.ELC.ACCS.ZS"],
    )


def gate_grid(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(GRID_COLUMNS)
    v.expect_column_values_to_not_be_null("report_date")
    v.expect_column_values_to_not_be_null("value")
    v.expect_column_values_to_be_in_set(
        "region_code", ["NR", "WR", "SR", "ER", "NER", "TOTAL"]
    )
    v.expect_column_values_to_be_between("value", 0, 10_000_000)
    v.expect_table_row_count_to_be_between(900, 1_200)
    v.expect_column_max_to_be_between("report_date", pd.Timestamp("today") - pd.Timedelta(days=3))


def gate_subscribers(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(SUBSCRIBERS_COLUMNS)
    v.expect_column_values_to_not_be_null("period_month")
    v.expect_column_values_to_not_be_null("subscribers")
    v.expect_compound_columns_to_be_unique(["period_month", "segment", "service_area", "operator"])
    v.expect_column_values_to_be_between("subscribers", 0, 500_000_000)
    v.expect_column_values_to_be_in_set("segment", ["wireless", "wireline"])
    v.expect_table_row_count_to_be_between(2_000, 50_000)
    v.expect_column_max_to_be_between("period_month", pd.Timestamp("today") - pd.Timedelta(days=75))


def gate_country_teledensity(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(COUNTRY_TD_COLUMNS)
    v.expect_column_values_to_be_unique("period_date")
    v.expect_column_values_to_be_between("wireless_teledensity_pct", 0, 200)
    v.expect_table_row_count_to_be_between(8, 40)
    v.expect_column_max_to_be_between("period_date", pd.Timestamp("2026-01-01"))


def gate_circle_teledensity(v: Any) -> None:
    v.expect_table_columns_to_match_ordered_list(CIRCLE_TD_COLUMNS)
    v.expect_compound_columns_to_be_unique(["period_date", "service_area"])
    v.expect_column_values_to_be_between("total_pct", 0, 500)
    v.expect_table_row_count_to_be_between(140, 400)
    v.expect_column_max_to_be_between("period_date", pd.Timestamp("2026-01-01"))


GATES: list[tuple[str, str, Any]] = [
    ("npci", "upi_monthly", gate_npci),
    ("openmeteo", "weather_daily", gate_weather),
    ("worldbank", "indicators", gate_worldbank),
    ("gridindia", "supply_daily", gate_grid),
    ("trai", "subscribers", gate_subscribers),
    ("trai", "teledensity_country", gate_country_teledensity),
    ("trai", "teledensity_circle", gate_circle_teledensity),
]

DATE_COLUMNS = {
    ("npci", "upi_monthly"): ["month_start"],
    ("openmeteo", "weather_daily"): ["date"],
    ("gridindia", "supply_daily"): ["report_date"],
    ("trai", "subscribers"): ["period_month"],
    ("trai", "teledensity_country"): ["period_date"],
    ("trai", "teledensity_circle"): ["period_date"],
}


def to_dataframe(table: pa.Table, gate_key: tuple[str, str]) -> pd.DataFrame:
    df = table.to_pandas()
    for column in DATE_COLUMNS.get(gate_key, []):
        df[column] = pd.to_datetime(df[column])
    return df
