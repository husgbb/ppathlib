from __future__ import annotations

import importlib.util
import os
from uuid import uuid4

import pytest

from ppathlib import PPath

pytestmark = pytest.mark.live_r2


def test_ppath_live_r2_roundtrip(live_r2_enabled):
    if not live_r2_enabled:
        pytest.skip("Set USE_LIVE_CLOUD=1 in .env to run live R2 tests.")

    if importlib.util.find_spec("boto3") is None:
        pytest.skip("boto3 is required for live R2 tests.")

    required_env = [
        "PPATH_DEV_R2_STORAGE_TYPE",
        "PPATH_DEV_R2_ENDPOINT_URL",
        "PPATH_DEV_R2_ACCESS_KEY_ID",
        "PPATH_DEV_R2_SECRET_ACCESS_KEY",
        "PPATH_DEV_R2_ROOT",
    ]
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        pytest.skip(f"Missing live R2 env vars: {', '.join(missing)}")

    path = PPath(f"ppath-tests/{uuid4().hex}.txt", profile="PPATH_DEV_R2")
    payload = "ppathlib-r2-smoke"

    try:
        path.write_text(payload)
        assert path.exists()
        assert path.read_text() == payload
    finally:
        if path.exists():
            path.unlink()
            pass
