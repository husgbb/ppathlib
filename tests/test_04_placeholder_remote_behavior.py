import pytest

from ppathlib import PPath


def test_remote_placeholder_methods_fail_explicitly():
    path = PPath("s3://public-bucket/daily/archive/report.parquet")

    with pytest.raises(NotImplementedError) as exc_info:
        path.copy("local-target.txt")

    message = str(exc_info.value)
    assert message.startswith("PPath.copy() is not implemented for remote mode.")
    assert "Path=s3://public-bucket/daily/archive/report.parquet" in message
    assert "Profile=PUBLIC" in message
    assert "Storage type=s3" in message
    assert "Required implementation:" in message
