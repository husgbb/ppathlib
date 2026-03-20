from pathlib import Path

import pandas as pd

from ppathlib import PPath


def test_local_mode_returns_the_public_ppath_object():
    path = PPath("data/report.parquet")

    assert type(path).__name__ == "PPath"


def test_local_mode_matches_python_pathlib_for_basic_lexical_behavior():
    path = PPath("data/archive/report.parquet")
    reference = Path("data/archive/report.parquet")

    assert str(path) == str(reference)
    assert path.name == reference.name
    assert path.stem == reference.stem
    assert path.suffix == reference.suffix
    assert path.parent == reference.parent


def test_local_mode_keeps_the_public_ppath_type_for_derived_paths():
    path = PPath("data/archive/report.parquet")

    assert type(path.parent).__name__ == "PPath"
    assert type(path.joinpath("derived.csv")).__name__ == "PPath"
    assert type(path.with_suffix(".csv")).__name__ == "PPath"


def test_local_mode_supports_pathlib_style_base_directory_composition():
    data_dir = PPath("data")

    input_path = data_dir / "source/input.csv"
    output_path = data_dir / "output/pred.parquet"

    assert type(input_path).__name__ == "PPath"
    assert type(output_path).__name__ == "PPath"
    assert str(input_path) == "data/source/input.csv"
    assert str(output_path) == "data/output/pred.parquet"


def test_local_mode_supports_pathlib_style_file_round_trip(tmp_path):
    data_dir = PPath(tmp_path / "data")

    input_path = data_dir / "source/input.csv"
    output_path = data_dir / "output/pred.csv"

    input_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    source = pd.DataFrame({"id": [1], "value": [10]})
    source.to_csv(input_path, index=False)

    loaded = pd.read_csv(input_path)
    predicted = loaded.assign(prediction=loaded["value"] * 2)
    predicted.to_csv(output_path, index=False)

    assert loaded.to_dict(orient="records") == [{"id": 1, "value": 10}]
    assert pd.read_csv(output_path).to_dict(orient="records") == [
        {"id": 1, "value": 10, "prediction": 20}
    ]

    output_path.unlink()

    assert output_path.exists() is False


def test_local_mode_does_not_require_configuration_files(project_dir):
    path = PPath("local/report.txt")

    assert str(path) == "local/report.txt"
