import pytest

from ppathlib import PPath
from ppathlib.exceptions import InvalidConfigurationException
from tests.support import write_global_config, write_project_config


def test_nearest_project_file_wins_over_parent_and_global(project_dir, home_dir, monkeypatch):
    write_global_config(
        home_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://global-bucket/global-root"
        """,
    )
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://parent-bucket/parent-root"
        """,
    )

    child = project_dir / "child"
    child.mkdir()
    write_project_config(
        child,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://child-bucket/child-root"
        """,
    )

    grandchild = child / "grandchild"
    grandchild.mkdir()
    monkeypatch.chdir(grandchild)

    path = PPath("daily/report.parquet", profile="analytics")

    assert str(path) == "s3://child-bucket/child-root/daily/report.parquet"


def test_global_file_is_used_when_no_project_file_exists(project_dir, home_dir):
    write_global_config(
        home_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://global-bucket/global-root"
        """,
    )

    path = PPath("daily/report.parquet", profile="analytics")

    assert str(path) == "s3://global-bucket/global-root/daily/report.parquet"


def test_project_and_global_files_do_not_merge(project_dir, home_dir):
    write_global_config(
        home_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://global-bucket/global-root"
        """,
    )
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        """,
    )

    with pytest.raises(InvalidConfigurationException, match="root"):
        PPath("daily/report.parquet", profile="analytics")


def test_global_profile_is_not_used_when_a_project_file_exists_without_that_profile(project_dir, home_dir):
    write_global_config(
        home_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://global-bucket/global-root"
        """,
    )
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.operations]
        storage_type = "s3"
        root = "s3://project-bucket/project-root"
        """,
    )

    with pytest.raises(InvalidConfigurationException, match="analytics"):
        PPath("daily/report.parquet", profile="analytics")


def test_profile_lookup_is_case_insensitive(project_dir):
    write_project_config(
        project_dir,
        """
        version = 1

        [profiles.analytics]
        storage_type = "s3"
        root = "s3://demo-bucket/warehouse"
        """,
    )

    lower = PPath("daily/report.parquet", profile="analytics")
    upper = PPath("daily/report.parquet", profile="ANALYTICS")

    assert str(lower) == str(upper)
