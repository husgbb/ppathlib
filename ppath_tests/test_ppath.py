from pathlib import Path, PureWindowsPath

import pytest

from ppathlib import RemoteBindingRequest, RemotePath, PPath, clear_client_cache, get_client
from ppathlib.exceptions import InvalidConfigurationException


@pytest.fixture(autouse=True)
def reset_client_cache():
    clear_client_cache()
    yield
    clear_client_cache()


def test_ppath_local_mode_returns_pathlib_path():
    path = PPath("data/report.parquet")

    assert isinstance(path, Path)
    assert str(path) == "data/report.parquet"


def test_ppath_requires_profile_for_remote_uri():
    with pytest.raises(InvalidConfigurationException, match="Remote paths require a profile"):
        PPath("s3://bucket/path/file.parquet")


def test_ppath_resolves_relative_remote_path_against_root(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_REMOTE_REGION", "auto")

    path = PPath("daily/report.parquet", profile="ppath-remote")

    assert isinstance(path, RemotePath)
    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"
    assert path.client is get_client("PPATH_REMOTE")
    assert path.client.storage_type == "s3"


def test_ppath_accepts_explicit_remote_uri_without_root(monkeypatch):
    monkeypatch.setenv("PPATH_DIRECT_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_DIRECT_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_DIRECT_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_DIRECT_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    path = PPath("s3://demo-bucket/raw/file.bin", profile="PPATH_DIRECT_REMOTE")

    assert isinstance(path, RemotePath)
    assert str(path) == "s3://demo-bucket/raw/file.bin"
    assert path.client is get_client("ppath_direct_remote")


def test_ppath_normalizes_windows_pathlike_segments_for_remote_paths(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    path = PPath(PureWindowsPath("daily") / "report.parquet", profile="PPATH_REMOTE")

    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"


def test_get_client_reuses_client_for_the_same_profile(monkeypatch):
    monkeypatch.setenv("PPATH_ALPHA_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_ALPHA_ROOT", "s3://bucket-a/root")
    monkeypatch.setenv("PPATH_ALPHA_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_ALPHA_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_ALPHA_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_ALPHA_REGION", "auto")

    client_a = get_client("PPATH_ALPHA")
    client_b = get_client("ppath_alpha")

    assert client_a is client_b


def test_get_client_does_not_share_clients_across_profiles(monkeypatch):
    monkeypatch.setenv("PPATH_ALPHA_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_ALPHA_ROOT", "s3://bucket-a/root")
    monkeypatch.setenv("PPATH_ALPHA_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_ALPHA_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_ALPHA_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_ALPHA_REGION", "auto")

    monkeypatch.setenv("PPATH_BETA_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_BETA_ROOT", "s3://bucket-b/other-root")
    monkeypatch.setenv("PPATH_BETA_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_BETA_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_BETA_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_BETA_REGION", "auto")

    client_a = get_client("PPATH_ALPHA")
    client_b = get_client("PPATH_BETA")

    assert client_a is not client_b
    assert client_a.profile_name == "PPATH_ALPHA"
    assert client_b.profile_name == "PPATH_BETA"


def test_ppath_rejects_relative_remote_path_without_root(monkeypatch):
    monkeypatch.setenv("PPATH_NO_ROOT_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_NO_ROOT_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_NO_ROOT_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_NO_ROOT_SECRET_ACCESS_KEY", "secret-key")

    with pytest.raises(
        InvalidConfigurationException,
        match="Relative path requires PPATH_NO_ROOT_ROOT",
    ):
        PPath("daily/report.parquet", profile="PPATH_NO_ROOT")


def test_ppath_rejects_scheme_mismatch(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    with pytest.raises(InvalidConfigurationException, match="expects s3:// paths"):
        PPath("gs://bucket/file.parquet", profile="PPATH_REMOTE")


def test_remote_path_lexical_operations(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    path = PPath("daily/archive/report.parquet", profile="PPATH_REMOTE")

    assert path.anchor == "s3://"
    assert path.drive == "demo-bucket"
    assert path.name == "report.parquet"
    assert path.stem == "report"
    assert path.suffix == ".parquet"
    assert path.parent == PPath("daily/archive", profile="PPATH_REMOTE")
    assert path.with_suffix(".csv") == PPath("daily/archive/report.csv", profile="PPATH_REMOTE")
    assert path.relative_to(PPath("daily", profile="PPATH_REMOTE")).as_posix() == "archive/report.parquet"


def test_remote_profile_client_builds_binding_request_from_root(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")
    monkeypatch.setenv("PPATH_REMOTE_REGION", "auto")

    client = get_client("PPATH_REMOTE")
    binding = client.binding_request()

    assert isinstance(binding, RemoteBindingRequest)
    assert binding.profile_name == "PPATH_REMOTE"
    assert binding.storage_type == "s3"
    assert binding.scope.container == "demo-bucket"
    assert binding.scope.prefix == "warehouse"
    assert binding.connection_options["endpoint_url"] == "https://storage.example.com"


def test_remote_profile_client_create_store_is_an_explicit_placeholder(monkeypatch):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    client = get_client("PPATH_REMOTE")

    with pytest.raises(NotImplementedError) as exc_info:
        client.create_store()

    assert str(exc_info.value) == (
        "RemoteProfileClient.create_store() is not implemented. "
        "Profile=PPATH_REMOTE. Storage type=s3. "
        "Root=s3://demo-bucket/warehouse. Required implementation: bind the abstract "
        "RemoteBindingRequest to a concrete store constructor in the runtime layer."
    )


def test_remote_profile_client_rejects_unsupported_gs_project(monkeypatch):
    monkeypatch.setenv("PPATH_GS_STORAGE_TYPE", "gs")
    monkeypatch.setenv("PPATH_GS_PROJECT", "demo-project")
    monkeypatch.setenv("PPATH_GS_CREDENTIALS_JSON", "/tmp/demo.json")

    with pytest.raises(
        InvalidConfigurationException,
        match="PPATH_GS_PROJECT is not supported by this prototype",
    ):
        get_client("PPATH_GS")


def test_remote_profile_client_rejects_unsupported_azure_connection_string(monkeypatch):
    monkeypatch.setenv("PPATH_AZ_STORAGE_TYPE", "azure")
    monkeypatch.setenv("PPATH_AZ_CONNECTION_STRING", "UseDevelopmentStorage=true")

    with pytest.raises(
        InvalidConfigurationException,
        match="PPATH_AZ_CONNECTION_STRING is not supported by this prototype",
    ):
        get_client("PPATH_AZ")


@pytest.mark.parametrize(
    ("method_name", "expected_requirement"),
    [
        ("open", "open_reader/open_writer"),
        ("iterdir", "list_with_delimiter"),
        ("glob", "pattern matching"),
        ("copy", "remote-to-remote copy"),
        ("move", "remote-to-remote rename"),
    ],
)
def test_remote_path_placeholder_messages_are_explicit(
    monkeypatch, method_name, expected_requirement
):
    monkeypatch.setenv("PPATH_REMOTE_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_REMOTE_ROOT", "s3://demo-bucket/warehouse")
    monkeypatch.setenv("PPATH_REMOTE_ENDPOINT_URL", "https://storage.example.com")
    monkeypatch.setenv("PPATH_REMOTE_ACCESS_KEY_ID", "access-key")
    monkeypatch.setenv("PPATH_REMOTE_SECRET_ACCESS_KEY", "secret-key")

    path = PPath("daily/report.parquet", profile="PPATH_REMOTE")
    method = getattr(path, method_name)

    expected_message = (
        f"RemotePath.{method_name}() is not implemented. "
        f"Path=s3://demo-bucket/warehouse/daily/report.parquet. "
        f"Profile=PPATH_REMOTE. Storage type=s3. "
    )

    with pytest.raises(NotImplementedError) as exc_info:
        if method_name in {"glob"}:
            method("*.parquet")
        elif method_name in {"copy", "move"}:
            method(path.with_suffix(".bak"))
        else:
            method()

    message = str(exc_info.value)
    assert message.startswith(expected_message)
    assert expected_requirement in message
