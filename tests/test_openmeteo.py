"""Unit tests for the Open-Meteo parser - no network (uses saved real samples)."""

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.base import SchemaChangedError
from ingestion.openmeteo import parse_daily_payload

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"


@pytest.mark.skipif(
    not (SAMPLES / "openmeteo_archive_delhi.json").exists(),
    reason="sample not present (git-ignored data/)",
)
def test_parse_real_archive_sample() -> None:
    payload = json.loads((SAMPLES / "openmeteo_archive_delhi.json").read_text())
    rows = parse_daily_payload("delhi", 28.6139, 77.2090, payload, made_on=None)
    assert len(rows) == 2
    assert rows[0]["date"] == date(2024, 6, 1)
    assert rows[0]["temperature_2m_max"] == 44.0
    assert rows[0]["temperature_2m_min"] == 31.3
    assert rows[0]["forecast_made_on"] is None


@pytest.mark.skipif(
    not (SAMPLES / "openmeteo_forecast_daily_delhi.json").exists(),
    reason="sample not present (git-ignored data/)",
)
def test_parse_real_forecast_sample() -> None:
    payload = json.loads((SAMPLES / "openmeteo_forecast_daily_delhi.json").read_text())
    rows = parse_daily_payload("delhi", 28.6139, 77.2090, payload, made_on=date(2026, 10, 6))
    assert len(rows) == 3
    assert all(row["forecast_made_on"] == date(2026, 10, 6) for row in rows)
    assert rows[0]["temperature_2m_max"] is not None


def test_missing_daily_var_fails_loudly() -> None:
    payload = {"daily": {"time": ["2024-06-01"], "temperature_2m_max": [40.0]}}
    with pytest.raises(SchemaChangedError, match="missing"):
        parse_daily_payload("delhi", 28.6139, 77.2090, payload, made_on=None)
