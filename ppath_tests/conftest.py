from __future__ import annotations

import os
from pathlib import Path

import pytest

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    load_dotenv = None


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "live_r2: runs against a live Cloudflare R2 bucket using credentials from .env",
    )


def _load_local_dotenv() -> None:
    if load_dotenv is None:
        return

    repo_root = Path(__file__).resolve().parent.parent
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)

@pytest.fixture
def live_r2_enabled() -> bool:
    _load_local_dotenv()
    return os.getenv("USE_LIVE_CLOUD") == "1"
