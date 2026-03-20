from pathlib import Path

import pandas as pd
import pytest

from ppathlib import PPath


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _live_config_is_ready() -> bool:
    config_path = _repo_root() / ".ppathlib.toml"
    if not config_path.is_file():
        return False
    content = config_path.read_text(encoding="utf-8")
    placeholders = (
        "<your-r2-bucket>",
        "<your-test-prefix>",
        "<your-account-id>",
        "<fill-me>",
    )
    return not any(placeholder in content for placeholder in placeholders)


@pytest.mark.live_remote
def test_live_r2_round_trip_uses_project_root_configuration(monkeypatch):
    if not _live_config_is_ready():
        pytest.skip("Fill the root .ppathlib.toml file with live R2 credentials to run this test.")

    monkeypatch.chdir(_repo_root())

    data_dir = PPath("live-tests", profile="r2_live")
    input_path = data_dir / "source/input.csv"
    output_path = data_dir / "output/pred.csv"
    copy_path = data_dir / "copies/input-copy.csv"
    moved_path = data_dir / "moved/input-moved.csv"
    renamed_path = data_dir / "renamed/input-renamed.csv"
    replaced_path = data_dir / "replaced/input.csv"
    paths_to_clean = [
        input_path,
        output_path,
        copy_path,
        moved_path,
        renamed_path,
        replaced_path,
    ]

    for path in paths_to_clean:
        if path.exists():
            path.unlink()

    source = pd.DataFrame({"id": [1], "value": [10]})
    with input_path.open("w", encoding="utf-8", newline="") as handle:
        source.to_csv(handle, index=False)

    with input_path.open("r", encoding="utf-8", newline="") as handle:
        loaded = pd.read_csv(handle)
    predicted = loaded.assign(prediction=loaded["value"] * 2)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        predicted.to_csv(handle, index=False)

    assert loaded.to_dict(orient="records") == [{"id": 1, "value": 10}]
    with output_path.open("r", encoding="utf-8", newline="") as handle:
        assert pd.read_csv(handle).to_dict(orient="records") == [
            {"id": 1, "value": 10, "prediction": 20}
        ]

    assert data_dir.is_dir() is True
    assert input_path.is_file() is True
    assert output_path.is_file() is True
    assert sorted(path.name for path in data_dir.iterdir()) == ["output", "source"]
    assert sorted(str(path) for path in data_dir.rglob("*.csv")) == [
        "s3://ppath-dev/live-tests/output/pred.csv",
        "s3://ppath-dev/live-tests/source/input.csv",
    ]

    copied = input_path.copy(copy_path)
    assert copied == copy_path
    assert copy_path.exists() is True

    moved = copy_path.move(moved_path)
    assert moved == moved_path
    assert copy_path.exists() is False
    assert moved_path.exists() is True

    renamed = moved_path.rename(renamed_path)
    assert renamed == renamed_path
    assert moved_path.exists() is False
    assert renamed_path.exists() is True

    replaced_path.write_text("old")
    replaced = renamed_path.replace(replaced_path)
    assert replaced == replaced_path
    assert renamed_path.exists() is False
    assert replaced_path.read_text() == "id,value\n1,10\n"

    input_path.unlink()
    output_path.unlink()
    replaced_path.unlink()

    assert input_path.exists() is False
    assert output_path.exists() is False
    assert replaced_path.exists() is False
