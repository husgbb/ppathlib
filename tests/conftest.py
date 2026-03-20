import os
import re
from pathlib import Path

import pytest

from ppathlib import clear_client_cache


_LEGACY_PROFILE_FIELD_ENV_RE = re.compile(
    r"^PPATH_[A-Z0-9_]+_("
    r"STORAGE_TYPE|ROOT|ACCESS_KEY_ID|SECRET_ACCESS_KEY|ENDPOINT_URL|SESSION_TOKEN|REGION|"
    r"NO_SIGN_REQUEST|CREDENTIALS_JSON|PROJECT|STORAGE_CLIENT_JSON|ACCOUNT_URL|ACCOUNT_KEY|"
    r"CONNECTION_STRING|TENANT_ID|CLIENT_ID|CLIENT_SECRET"
    r")$"
)


@pytest.fixture(autouse=True)
def reset_process_state(monkeypatch, tmp_path):
    clear_client_cache()
    for key in list(os.environ):
        if _LEGACY_PROFILE_FIELD_ENV_RE.match(key):
            monkeypatch.delenv(key, raising=False)

    home_dir = tmp_path / "home"
    home_dir.mkdir()
    monkeypatch.setenv("HOME", str(home_dir))

    yield

    clear_client_cache()


@pytest.fixture
def project_dir(tmp_path, monkeypatch) -> Path:
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.chdir(project)
    return project


@pytest.fixture
def home_dir() -> Path:
    return Path.home()
