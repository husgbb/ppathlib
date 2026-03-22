import tomllib

import pytest

from ppathlib import PPath
from ppathlib.exceptions import InvalidConfigurationException
from tests.support import write_global_config, write_project_config


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


def test_remote_mode_accepts_pathless_profile_creation_and_persists_to_global_config(project_dir, home_dir):
    path = PPath(
        profile="personal-storage",
        profile_connection_params={
            "storage_type": "s3",
            "root": "s3://ppath-dev",
            "endpoint_url": "https://storage.example.com",
            "access_key_id": "access-key",
        },
    )

    config_path = home_dir / ".config" / "ppathlib" / ".ppathlib.toml"
    document = tomllib.loads(config_path.read_text(encoding="utf-8"))

    assert str(path) == "s3://ppath-dev"
    assert document["version"] == 1
    assert document["profiles"]["personal-storage"] == {
        "storage_type": "s3",
        "root": "s3://ppath-dev",
        "endpoint_url": "https://storage.example.com",
        "access_key_id": "access-key",
    }


def test_remote_mode_saves_profile_to_nearest_existing_project_config(project_dir, home_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://demo-bucket/warehouse"
        """,
    )
    global_path = write_global_config(
        home_dir,
        """
        version = 1

        [profiles.archive]
        storage_type = "s3"
        root = "s3://global-bucket/archive"
        """,
    )

    PPath(
        "daily/report.parquet",
        profile="personal-storage",
        profile_connection_params={
            "storage_type": "s3",
            "root": "s3://ppath-dev",
        },
    )

    project_document = tomllib.loads((project_dir / ".ppathlib.toml").read_text(encoding="utf-8"))
    global_document = tomllib.loads(global_path.read_text(encoding="utf-8"))

    assert project_document["profiles"]["personal-storage"] == {
        "storage_type": "s3",
        "root": "s3://ppath-dev",
    }
    assert "personal-storage" not in global_document["profiles"]


def test_remote_mode_overwrites_existing_profile_with_connection_params(project_dir):
    config_path = write_project_config(
        project_dir,
        """
        version = 1

        [profiles.personal-storage]
        storage_type = "s3"
        root = "s3://old-bucket"
        endpoint_url = "https://old.example.com"
        """,
    )

    path = PPath(
        "daily/report.parquet",
        profile="personal-storage",
        profile_connection_params={
            "storage_type": "s3",
            "root": "s3://ppath-dev",
            "endpoint_url": "https://new.example.com",
        },
    )

    document = tomllib.loads(config_path.read_text(encoding="utf-8"))

    assert str(path) == "s3://ppath-dev/daily/report.parquet"
    assert document["profiles"]["personal-storage"] == {
        "storage_type": "s3",
        "root": "s3://ppath-dev",
        "endpoint_url": "https://new.example.com",
    }


def test_remote_mode_can_reuse_saved_profile_without_connection_params(home_dir):
    PPath(
        profile="personal-storage",
        profile_connection_params={
            "storage_type": "s3",
            "root": "s3://ppath-dev",
        },
    )

    path = PPath("daily/report.parquet", profile="personal-storage")

    assert str(path) == "s3://ppath-dev/daily/report.parquet"


def test_remote_mode_rejects_connection_params_without_profile():
    with pytest.raises(InvalidConfigurationException, match="requires `profile`"):
        PPath(
            "daily/report.parquet",
            profile_connection_params={
                "storage_type": "s3",
                "root": "s3://ppath-dev",
            },
        )
