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

    output_path.unlink()

    assert output_path.exists() is False
