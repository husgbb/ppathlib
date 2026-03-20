import pytest

from ppathlib import ExperimentalRemoteRuntimeWarning, PPath
from tests.support import install_in_memory_remote_store


def test_remote_runtime_methods_emit_experimental_warning(monkeypatch):
    install_in_memory_remote_store(monkeypatch)
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    with pytest.warns(ExperimentalRemoteRuntimeWarning, match="experimental"):
        assert path.exists() is False


def test_unsupported_public_copy_to_local_still_fails_explicitly():
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    with pytest.warns(ExperimentalRemoteRuntimeWarning, match="experimental"):
        with pytest.raises(NotImplementedError) as exc_info:
            path.copy("local-target.txt")

    message = str(exc_info.value)
    assert message.startswith("PPath.copy() is not implemented for remote mode.")
    assert "Path=s3://public-bucket/daily/archive/report.parquet" in message
    assert "Profile=PUBLIC" in message
    assert "Storage type=s3" in message
    assert "Required implementation:" in message
