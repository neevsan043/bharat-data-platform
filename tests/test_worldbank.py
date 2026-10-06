"""Unit tests for the World Bank parser - no network (uses saved real samples)."""

import json
from pathlib import Path

import pytest

from ingestion.base import SchemaChangedError
from ingestion.worldbank import parse_indicator_rows

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
SAMPLE = SAMPLES / "worldbank_ind_gdp.json"


def load_sample() -> tuple[dict, list[dict]]:
    payload = json.loads(SAMPLE.read_text())
    meta, rows = payload
    return meta, rows


@pytest.mark.skipif(not SAMPLE.exists(), reason="sample not present (git-ignored data/)")
def test_parse_real_gdp_sample() -> None:
    meta, rows = load_sample()
    table = parse_indicator_rows("NY.GDP.MKTP.CD", rows)
    assert table.num_rows == meta["per_page"] == 3
    assert table.column("indicator_id").to_pylist() == ["NY.GDP.MKTP.CD"] * 3
    assert table.column("country_iso3").to_pylist() == ["IND"] * 3
    years = table.column("year").to_pylist()
    assert years == [2025, 2024, 2023]
    assert table.column("value").to_pylist()[0] == 3956067115771.63


@pytest.mark.skipif(not SAMPLE.exists(), reason="sample not present (git-ignored data/)")
def test_unexpected_date_fails_loudly() -> None:
    _, rows = load_sample()
    broken = [dict(rows[0], date="2025M09")] + rows[1:]
    with pytest.raises(SchemaChangedError, match="unexpected date value '2025M09'"):
        parse_indicator_rows("NY.GDP.MKTP.CD", broken)
