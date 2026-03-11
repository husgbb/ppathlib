from pathlib import Path, PureWindowsPath

import pytest

import ppathlib.ppath as ppath_module
from ppathlib import PPath, clear_client_cache, get_client
from ppathlib.cloudpath import implementation_registry
from ppathlib.enums import FileCacheMode
from ppathlib.exceptions import InvalidConfigurationException, MissingDependenciesError
from ppathlib.s3.s3client import S3Client


class DummyS3Client(S3Client):
    def __init__(self, label: str):
        self.label = label


@pytest.fixture(autouse=True)
def reset_client_cache():
    previous = implementation_registry["s3"].dependencies_loaded
    implementation_registry["s3"].dependencies_loaded = True
    clear_client_cache()
    yield
    clear_client_cache()
    implementation_registry["s3"].dependencies_loaded = previous


@pytest.fixture
def fake_s3_builder(monkeypatch):
    calls = []

    def _fake_build(client_kwargs):
        calls.append(dict(client_kwargs))
        return DummyS3Client(f"client-{len(calls)}")

    monkeypatch.setattr(ppath_module, "_build_s3_client", _fake_build)
    return calls


def test_ppath_local_mode_returns_pathlib_path():
    path = PPath("data/report.parquet")

    assert isinstance(path, Path)
    assert str(path) == "data/report.parquet"


def test_ppath_requires_profile_for_remote_uri():
    with pytest.raises(InvalidConfigurationException, match="Remote paths require a profile"):
        PPath("s3://bucket/path/file.parquet")


def test_ppath_resolves_r2_relative_path_against_root(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_DEV_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DEV_R2_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_DEV_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_DEV_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DEV_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_DEV_R2_REGION", "auto")

    path = PPath("daily/report.parquet", profile="ppath-dev-r2")

    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"
    assert path.client is get_client("PPATH_DEV_R2")
    assert fake_s3_builder == [
        {
            "endpoint_url": "https://example.r2.cloudflarestorage.com",
            "aws_access_key_id": "access-key",
            "aws_secret_access_key": "secret-key",
            "aws_session_token": None,
            "region_name": "auto",
            "no_sign_request": False,
        }
    ]


def test_ppath_accepts_explicit_remote_uri_without_root(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_DIRECT_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DIRECT_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_DIRECT_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DIRECT_R2_SECRET_ACCESS_KEY", "secret-key")

    path = PPath("s3://demo-bucket/raw/file.bin", profile="PPATH_DIRECT_R2")

    assert str(path) == "s3://demo-bucket/raw/file.bin"
    assert path.client is get_client("ppath_direct_r2")
    assert len(fake_s3_builder) == 1


def test_ppath_applies_profile_cache_dir_and_mode(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_DEV_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DEV_R2_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_DEV_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_DEV_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DEV_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_DEV_R2_REGION", "auto")
    monkeypatch.setenv("PPATH_DEV_R2_CACHE_DIR", "/tmp/ppath-cache")
    monkeypatch.setenv("PPATH_DEV_R2_CACHE_MODE", "persistent")

    path = PPath("daily/report.parquet", profile="PPATH_DEV_R2")

    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"
    assert fake_s3_builder == [
        {
            "endpoint_url": "https://example.r2.cloudflarestorage.com",
            "aws_access_key_id": "access-key",
            "aws_secret_access_key": "secret-key",
            "aws_session_token": None,
            "region_name": "auto",
            "no_sign_request": False,
            "local_cache_dir": "/tmp/ppath-cache/PPATH_DEV_R2",
            "file_cache_mode": FileCacheMode.persistent,
        }
    ]


def test_ppath_normalizes_windows_pathlike_segments_for_remote_paths(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_DEV_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DEV_R2_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_DEV_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_DEV_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DEV_R2_SECRET_ACCESS_KEY", "secret-key")

    path = PPath(PureWindowsPath("daily") / "report.parquet", profile="PPATH_DEV_R2")

    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"
    assert len(fake_s3_builder) == 1


def test_get_client_reuses_client_when_only_root_differs(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_ALPHA_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_ALPHA_R2_ROOT", "s3://bucket-a/root")
    monkeypatch.setenv("PPATH_ALPHA_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_ALPHA_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_ALPHA_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_ALPHA_R2_REGION", "auto")

    monkeypatch.setenv("PPATH_BETA_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_BETA_R2_ROOT", "s3://bucket-b/other-root")
    monkeypatch.setenv("PPATH_BETA_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_BETA_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_BETA_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_BETA_R2_REGION", "auto")

    client_a = get_client("PPATH_ALPHA_R2")
    client_b = get_client("ppath_beta_r2")

    assert client_a is client_b
    assert len(fake_s3_builder) == 1


def test_get_client_separates_profiles_when_cache_dirs_are_enabled(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_ALPHA_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_ALPHA_R2_ROOT", "s3://bucket-a/root")
    monkeypatch.setenv("PPATH_ALPHA_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_ALPHA_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_ALPHA_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_ALPHA_R2_CACHE_DIR", "/tmp/shared-cache")

    monkeypatch.setenv("PPATH_BETA_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_BETA_R2_ROOT", "s3://bucket-b/other-root")
    monkeypatch.setenv("PPATH_BETA_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_BETA_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_BETA_R2_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_BETA_R2_CACHE_DIR", "/tmp/shared-cache")

    client_a = get_client("PPATH_ALPHA_R2")
    client_b = get_client("PPATH_BETA_R2")

    assert client_a is not client_b
    assert len(fake_s3_builder) == 2
    assert fake_s3_builder[0]["local_cache_dir"] == "/tmp/shared-cache/PPATH_ALPHA_R2"
    assert fake_s3_builder[1]["local_cache_dir"] == "/tmp/shared-cache/PPATH_BETA_R2"


def test_ppath_rejects_relative_remote_path_without_root(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_NO_ROOT_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_NO_ROOT_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_NO_ROOT_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_NO_ROOT_R2_SECRET_ACCESS_KEY", "secret-key")

    with pytest.raises(
        InvalidConfigurationException,
        match="Relative path requires PPATH_NO_ROOT_R2_ROOT",
    ):
        PPath("daily/report.parquet", profile="PPATH_NO_ROOT_R2")

    assert fake_s3_builder == []


def test_ppath_rejects_scheme_mismatch(monkeypatch, fake_s3_builder):
    monkeypatch.setenv("PPATH_DEV_R2_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DEV_R2_ENDPOINT_URL", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("PPATH_DEV_R2_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DEV_R2_SECRET_ACCESS_KEY", "secret-key")

    with pytest.raises(InvalidConfigurationException, match="expects s3:// paths"):
        PPath("gs://bucket/file.parquet", profile="PPATH_DEV_R2")

    assert fake_s3_builder == []


def test_get_client_reports_missing_gs_dependencies(monkeypatch):
    monkeypatch.setenv("PPATH_DEV_GCS_STORAGE_TYPE", "gs")
    monkeypatch.setenv("PPATH_DEV_GCS_PROJECT", "demo-project")
    monkeypatch.setenv("PPATH_DEV_GCS_CREDENTIALS_JSON", "/tmp/fake-credentials.json")

    previous = implementation_registry["gs"].dependencies_loaded
    implementation_registry["gs"].dependencies_loaded = False
    try:
        with pytest.raises(MissingDependenciesError, match=r"ppathlib\[gs\]"):
            get_client("PPATH_DEV_GCS")
    finally:
        implementation_registry["gs"].dependencies_loaded = previous
