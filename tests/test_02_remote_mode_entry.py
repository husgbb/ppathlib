import pytest

from ppathlib import PPath
from ppathlib.exceptions import InvalidConfigurationException
from tests.support import write_project_config


def test_remote_mode_accepts_relative_path_when_profile_defines_root(project_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://demo-bucket/warehouse"
        endpoint_url = "https://storage.example.com"
        access_key_id = "access-key"
        secret_access_key = "secret-key"
        """,
    )

    path = PPath("daily/report.parquet", profile="analytics")

    assert type(path).__name__ == "PPath"
    assert str(path) == "s3://demo-bucket/warehouse/daily/report.parquet"


def test_remote_mode_accepts_explicit_remote_uri_with_profile(project_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        endpoint_url = "https://storage.example.com"
        access_key_id = "access-key"
        secret_access_key = "secret-key"
        """,
    )

    path = PPath("s3://demo-bucket/raw/file.parquet", profile="analytics")

    assert type(path).__name__ == "PPath"
    assert str(path) == "s3://demo-bucket/raw/file.parquet"


def test_remote_mode_accepts_profileless_public_remote_uri():
    path = PPath("s3://public-bucket/catalog/scene.parquet")

    assert type(path).__name__ == "PPath"
    assert str(path) == "s3://public-bucket/catalog/scene.parquet"
    assert path.anchor == "s3://"
    assert path.drive == "public-bucket"


def test_remote_mode_rejects_relative_path_when_profile_has_no_root(project_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        endpoint_url = "https://storage.example.com"
        access_key_id = "access-key"
        secret_access_key = "secret-key"
        """,
    )

    with pytest.raises(InvalidConfigurationException, match="root"):
        PPath("daily/report.parquet", profile="analytics")


def test_remote_mode_rejects_uri_scheme_that_contradicts_the_profile(project_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        endpoint_url = "https://storage.example.com"
        access_key_id = "access-key"
        secret_access_key = "secret-key"
        """,
    )

    with pytest.raises(InvalidConfigurationException, match="s3://"):
        PPath("gs://demo-bucket/file.parquet", profile="analytics")
