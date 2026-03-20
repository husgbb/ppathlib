import pytest

from ppathlib import PPath


@pytest.mark.parametrize(
    ("method_name", "arguments"),
    [
        ("open", tuple()),
        ("read_text", tuple()),
        ("write_text", ("hello",)),
        ("read_bytes", tuple()),
        ("write_bytes", (b"hello",)),
        ("exists", tuple()),
        ("is_file", tuple()),
        ("is_dir", tuple()),
        ("iterdir", tuple()),
        ("glob", ("*.parquet",)),
        ("rglob", ("*.parquet",)),
        ("walk", tuple()),
        ("copy", ("s3://public-bucket/daily/archive/report-copy.parquet",)),
        ("move", ("s3://public-bucket/daily/archive/report-moved.parquet",)),
        ("rename", ("s3://public-bucket/daily/archive/report-renamed.parquet",)),
        ("replace", ("s3://public-bucket/daily/archive/report-replaced.parquet",)),
        ("unlink", tuple()),
    ],
)
def test_remote_placeholder_methods_fail_explicitly(method_name, arguments):
    path = PPath("s3://public-bucket/daily/archive/report.parquet")
    method = getattr(path, method_name)

    with pytest.raises(NotImplementedError) as exc_info:
        method(*arguments)

    message = str(exc_info.value)
    assert message.startswith(f"PPath.{method_name}() is not implemented for remote mode.")
    assert "Path=s3://public-bucket/daily/archive/report.parquet" in message
    assert "Profile=PUBLIC" in message
    assert "Storage type=s3" in message
    assert "Required implementation:" in message
