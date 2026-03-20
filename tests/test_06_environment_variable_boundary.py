import pytest

from ppathlib import PPath
from ppathlib.exceptions import InvalidConfigurationException


def test_dotenv_file_is_not_part_of_configuration_loading(project_dir):
    (project_dir / ".env").write_text(
        "PPATH_ANALYTICS_STORAGE_TYPE=s3\nPPATH_ANALYTICS_ROOT=s3://env-bucket/root\n",
        encoding="utf-8",
    )

    with pytest.raises(InvalidConfigurationException):
        PPath("daily/report.parquet", profile="analytics")


def test_environment_variables_do_not_define_profile_fields(monkeypatch, project_dir):
    monkeypatch.setenv("PPATH_ANALYTICS_STORAGE_TYPE", "s3")
    monkeypatch.setenv("PPATH_ANALYTICS_ROOT", "s3://env-bucket/root")

    with pytest.raises(InvalidConfigurationException):
        PPath("daily/report.parquet", profile="analytics")
