"""Configuration loaded from .env / environment. No secrets live in code."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)


@dataclass(frozen=True)
class Settings:
    """Runtime settings shared by every ingester."""

    repo_root: Path
    data_dir: Path
    user_agent: str
    request_timeout_s: float
    opencellid_token: str | None

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def manifest_dir(self) -> Path:
        return self.bronze_dir / "_manifest"

    @property
    def manifest_path(self) -> Path:
        return self.manifest_dir / "manifest.jsonl"

    def bronze_source_dir(self, source: str) -> Path:
        return self.bronze_dir / source

    def landing_source_dir(self, source: str) -> Path:
        return self.landing_dir / source

    def landing_processed_dir(self, source: str) -> Path:
        return self.landing_dir / "_processed" / source


def load_settings(env_file: Path | None = None) -> Settings:
    """Build Settings, reading .env once (real values only, no defaults pretending)."""
    load_dotenv(env_file or REPO_ROOT / ".env")
    token = (os.getenv("OPENCELLID_TOKEN") or "").strip() or None
    return Settings(
        repo_root=REPO_ROOT,
        data_dir=Path(os.getenv("BDP_DATA_DIR") or REPO_ROOT / "data"),
        user_agent=os.getenv("BDP_USER_AGENT", DEFAULT_USER_AGENT),
        request_timeout_s=float(os.getenv("BDP_REQUEST_TIMEOUT_S", "90")),
        opencellid_token=token,
    )
