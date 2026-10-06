"""Unit tests for the OpenCelliD row mapper, India filter and token guard.

The sample CSV holds the first 200 real rows of the 2026-10-05 worldwide dump
(non-India rows - OpenCelliD contains no Indian towers, see docs/data-sources.md).
"""

from pathlib import Path

import pytest

from ingestion.base import IngestionError, SchemaChangedError
from ingestion.config import Settings
from ingestion.opencellid import OpenCelliDIngester, iter_india_batches, map_row

SAMPLES = Path(__file__).resolve().parents[1] / "data" / "samples"
SAMPLE = SAMPLES / "opencellid_worldsample_first200.csv"

REPO_ROOT = Path(__file__).resolve().parents[1]


def _settings(token: str | None) -> Settings:
    return Settings(
        repo_root=REPO_ROOT,
        data_dir=REPO_ROOT / "data",
        user_agent="test-agent",
        request_timeout_s=30,
        opencellid_token=token,
    )


def test_map_row_types() -> None:
    radio, mcc, net, area, cell, unit, lon, lat, rng, samples, changeable, created, updated, signal = (
        "LTE", "310", "410", "120", "48261", "", "-73.776962", "40.62437", "890", "3", "1",
        "1562838679", "1562838679", "60",
    )
    mapped = map_row([radio, mcc, net, area, cell, unit, lon, lat, rng, samples, changeable, created, updated, signal])
    assert mapped["mcc"] == 310 and mapped["net"] == 410
    assert mapped["lon"] == pytest.approx(-73.776962)
    assert mapped["average_signal"] == 60
    assert "unit" not in mapped  # the 'unit' column is not carried into bronze


@pytest.mark.skipif(not SAMPLE.exists(), reason="sample not present (git-ignored data/)")
def test_india_filter_excludes_non_india_rows(tmp_path: Path) -> None:
    import gzip

    gz_sample = tmp_path / "sample.csv.gz"
    with gzip.open(gz_sample, "wt", encoding="utf-8", newline="") as handle:
        handle.write(SAMPLE.read_text())
    batches = list(iter_india_batches(gz_sample, mccs=(404, 405), batch_rows=10))
    # The real worldwide dump has zero Indian rows; the filter must yield nothing.
    assert batches == []


def test_wrong_header_fails_loudly(tmp_path: Path) -> None:
    import gzip

    broken = tmp_path / "broken.csv.gz"
    with gzip.open(broken, "wt", encoding="utf-8") as handle:
        handle.write(",".join(["radio", "mcc"]) + "\n")
        handle.write("LTE,404\n")
    with pytest.raises(SchemaChangedError, match="header changed"):
        list(iter_india_batches(broken))


def test_missing_token_fails_with_clear_message() -> None:
    with pytest.raises(IngestionError, match="OPENCELLID_TOKEN"):
        OpenCelliDIngester(_settings(token=None))


def test_token_present_constructs() -> None:
    assert OpenCelliDIngester(_settings(token="pk.test")).settings.opencellid_token == "pk.test"
