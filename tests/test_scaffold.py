"""Structural checks for the Phase 0 scaffold."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_DIRS = [
    "ingestion",
    "pipelines",
    "transform/models/staging",
    "transform/models/marts",
    "quality",
    "analytics",
    "dashboards",
    "docs",
    "tests",
    ".github/workflows",
    "data/bronze",
    "data/silver",
    "data/gold",
]

REQUIRED_FILES = [
    "pyproject.toml",
    ".gitignore",
    "README.md",
    "AGENTS.md",
    "docs/data-sources.md",
    "docs/source-selection.md",
    "docs/architecture.md",
    "docs/phases.md",
    ".github/workflows/ci.yml",
]


def test_required_directories_exist() -> None:
    missing = [d for d in REQUIRED_DIRS if not (ROOT / d).is_dir()]
    assert missing == [], f"missing directories: {missing}"


def test_required_files_exist() -> None:
    missing = [f for f in REQUIRED_FILES if not (ROOT / f).is_file()]
    assert missing == [], f"missing files: {missing}"


def test_python_requires_at_least_311() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert pyproject["project"]["requires-python"] == ">=3.11"
