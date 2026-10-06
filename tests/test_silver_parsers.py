"""Unit tests for silver cleaning helpers - small in-test fixtures, no network."""

from datetime import date

import pytest

from pipelines.silver.grid import parse_number, parse_psp_rows
from pipelines.silver.trai import normalise_lsa, normalise_operator, parse_month_label


def test_parse_number_handles_source_noise() -> None:
    assert parse_number("71173.0") == 71173.0
    assert parse_number("1 ,091,098") == 1091098.0  # pdfplumber splits inside numbers
    assert parse_number("33,741,507]") == 33741507.0  # bracket noise from extraction
    assert parse_number("( 1,583)") == -1583.0  # TRAI negative convention
    assert parse_number("-") is None
    assert parse_number("") is None


def test_parse_number_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="unparsable number"):
        parse_number('!"#!$!#$"%')  # Sep-2025 PDF font-encoding garbage (real sample)


def test_parse_psp_rows_extracts_regional_metrics() -> None:
    rows = [
        ["", "", "NR", "WR", "SR", "ER", "NER", "TOTAL", ""],
        ["Demand Met (MW) during Evening Peak hr (19:00hrs)¹", "", "71173.0", "", "", "", "", "233422.0", ""],
        ["J. Power Supply Position of ISTS Bulk Consumer Entities", "", "", "", "", "", "", "", ""],
        ["Energy Met (MU)", "", "1588.0", "", "", "", "", "5426.82", ""],
    ]
    records, rejects = parse_psp_rows(rows)
    by_key = {(r["metric"], r["region_code"]): r["value"] for r in records}
    assert by_key[("demand_met_evening_peak_mw", "NR")] == 71173.0
    assert by_key[("demand_met_evening_peak_mw", "TOTAL")] == 233422.0
    assert by_key[("energy_met_mu", "NR")] == 1588.0
    assert rejects == []


def test_parse_psp_rows_handles_shoratge_typo() -> None:
    rows = [
        ["", "", "NR", "TOTAL"],
        ["Shoratge (MW) during Evening Peak hr (19:00hrs)¹", "", "1276.0", "5812.0"],
    ]
    records, _ = parse_psp_rows(rows)
    assert any(r["metric"] == "shortage_evening_peak_mw" and r["value"] == 1276.0 for r in records)


def test_normalise_lsa_variants() -> None:
    assert normalise_lsa("U.P.(W )") == "UP (West)"
    assert normalise_lsa("J&K") == "Jammu & Kashmir"
    assert normalise_lsa("Orissa") == "Odisha"
    assert normalise_lsa("M umbai") == "Mumbai"
    assert normalise_lsa("W est Bengal") == "West Bengal"
    assert normalise_lsa("Tamil Nadu") == "Tamil Nadu"


def test_normalise_operator_collapsed_match() -> None:
    assert normalise_operator("R e l i a nce Com.") == "Reliance Communications"
    assert normalise_operator("M TNL") == "MTNL"
    assert normalise_operator("Reliance Jio") == "Reliance Jio"


def test_parse_month_label() -> None:
    assert parse_month_label("Aug-26") == date(2026, 8, 1)
    assert parse_month_label("July-26") == date(2026, 7, 1)
    assert parse_month_label("Sept-25") == date(2025, 9, 1)
    assert parse_month_label("not-a-month") is None
