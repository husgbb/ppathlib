import pytest

from ppathlib import PPath


def test_remote_lexical_properties_follow_the_uri_string():
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    assert str(path) == "s3://public-bucket/daily/archive/report.parquet"
    assert path.anchor == "s3://"
    assert path.drive == "public-bucket"
    assert path.name == "report.parquet"
    assert path.stem == "report"
    assert path.suffix == ".parquet"
    assert path.suffixes == [".parquet"]


def test_remote_lexical_parent_and_joinpath_are_deterministic():
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    assert type(path.parent).__name__ == "PPath"
    assert str(path.parent) == "s3://public-bucket/daily/archive"
    assert str(path.joinpath("derived", "report.csv")) == (
        "s3://public-bucket/daily/archive/report.parquet/derived/report.csv"
    )


def test_remote_mode_supports_pathlib_style_base_directory_composition():
    data_dir = PPath("s3://public-bucket/data/")

    input_path = data_dir / "source/input.csv"
    output_path = data_dir / "output/pred.parquet"

    assert type(input_path).__name__ == "PPath"
    assert type(output_path).__name__ == "PPath"
    assert str(input_path) == "s3://public-bucket/data/source/input.csv"
    assert str(output_path) == "s3://public-bucket/data/output/pred.parquet"


def test_remote_lexical_transformations_do_not_require_backend_access():
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    assert type(path.with_suffix(".csv")).__name__ == "PPath"
    assert str(path.with_suffix(".csv")) == "s3://public-bucket/daily/archive/report.csv"
    assert path.relative_to(PPath("s3://public-bucket/daily")).as_posix() == (
        "archive/report.parquet"
    )
    assert path.is_relative_to(PPath("s3://public-bucket/daily")) is True


def test_remote_bucket_root_parent_is_stable():
    path = PPath("s3://public-bucket")

    assert str(path.parent) == "s3://public-bucket"
    assert path.parents == ()


def test_remote_joinpath_rejects_absolute_segments_and_remote_uris():
    path = PPath("s3://public-bucket/data")

    with pytest.raises(ValueError, match="absolute"):
        path.joinpath("/abs/file.txt")

    with pytest.raises(ValueError, match="remote URIs"):
        path.joinpath("s3://other-bucket/path.txt")
