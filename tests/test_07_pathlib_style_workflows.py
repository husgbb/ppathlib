import pandas as pd

from ppathlib import PPath
from tests.support import install_in_memory_remote_store, write_project_config


def test_remote_mode_supports_pathlib_style_file_round_trip(project_dir, monkeypatch):
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
    install_in_memory_remote_store(monkeypatch)

    data_dir = PPath("datasets", profile="analytics")

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


def test_remote_mode_supports_basic_text_and_bytes_io(project_dir, monkeypatch):
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
    install_in_memory_remote_store(monkeypatch)

    text_path = PPath("datasets/notes.txt", profile="analytics")
    bytes_path = PPath("datasets/payload.bin", profile="analytics")

    assert text_path.write_text("hello") == 5
    assert text_path.read_text() == "hello"
    assert text_path.exists() is True

    assert bytes_path.write_bytes(b"\x00\x01") == 2
    assert bytes_path.read_bytes() == b"\x00\x01"
    assert bytes_path.exists() is True


def test_remote_mode_distinguishes_files_directories_and_children(project_dir, monkeypatch):
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
    install_in_memory_remote_store(monkeypatch)

    data_dir = PPath("datasets", profile="analytics")
    source_dir = data_dir / "source"
    output_dir = data_dir / "output"
    input_path = source_dir / "input.csv"
    output_path = output_dir / "pred.csv"
    notes_path = data_dir / "notes.txt"

    input_path.write_text("id,value\n1,10\n")
    output_path.write_text("id,value,prediction\n1,10,20\n")
    notes_path.write_text("hello")

    assert input_path.exists() is True
    assert input_path.is_file() is True
    assert input_path.is_dir() is False

    assert data_dir.exists() is True
    assert data_dir.is_dir() is True
    assert data_dir.is_file() is False
    assert (data_dir / "missing.csv").exists() is False

    children = sorted(str(path) for path in data_dir.iterdir())

    assert children == [
        "s3://demo-bucket/warehouse/datasets/notes.txt",
        "s3://demo-bucket/warehouse/datasets/output",
        "s3://demo-bucket/warehouse/datasets/source",
    ]


def test_remote_mode_supports_glob_rglob_and_walk(project_dir, monkeypatch):
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
    install_in_memory_remote_store(monkeypatch)

    data_dir = PPath("datasets", profile="analytics")
    (data_dir / "source/input.csv").write_text("id,value\n1,10\n")
    (data_dir / "source/nested/extra.csv").write_text("id,value\n2,20\n")
    (data_dir / "output/pred.csv").write_text("id,value,prediction\n1,10,20\n")
    (data_dir / "notes.txt").write_text("hello")

    assert sorted(str(path) for path in data_dir.glob("*.txt")) == [
        "s3://demo-bucket/warehouse/datasets/notes.txt"
    ]
    assert sorted(str(path) for path in data_dir.glob("source/*.csv")) == [
        "s3://demo-bucket/warehouse/datasets/source/input.csv"
    ]
    assert sorted(str(path) for path in data_dir.rglob("*.csv")) == [
        "s3://demo-bucket/warehouse/datasets/output/pred.csv",
        "s3://demo-bucket/warehouse/datasets/source/input.csv",
        "s3://demo-bucket/warehouse/datasets/source/nested/extra.csv",
    ]

    walked = list(data_dir.walk())

    assert walked[0][0] == data_dir
    assert sorted(walked[0][1]) == ["output", "source"]
    assert walked[0][2] == ["notes.txt"]

    assert str(walked[1][0]) == "s3://demo-bucket/warehouse/datasets/output"
    assert walked[1][1] == []
    assert walked[1][2] == ["pred.csv"]

    assert str(walked[2][0]) == "s3://demo-bucket/warehouse/datasets/source"
    assert walked[2][1] == ["nested"]
    assert walked[2][2] == ["input.csv"]

    assert str(walked[3][0]) == "s3://demo-bucket/warehouse/datasets/source/nested"
    assert walked[3][1] == []
    assert walked[3][2] == ["extra.csv"]


def test_remote_mode_supports_copy_move_rename_and_replace(project_dir, monkeypatch):
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
    install_in_memory_remote_store(monkeypatch)

    data_dir = PPath("datasets", profile="analytics")
    source_path = data_dir / "source/input.csv"
    copy_path = data_dir / "copies/input-copy.csv"
    moved_path = data_dir / "moved/input-moved.csv"
    renamed_path = data_dir / "renamed/input-renamed.csv"
    replaced_path = data_dir / "replaced/input.csv"

    source_path.write_text("id,value\n1,10\n")

    copied = source_path.copy(copy_path)
    assert copied == copy_path
    assert source_path.exists() is True
    assert copy_path.exists() is True
    assert copy_path.read_text() == "id,value\n1,10\n"

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
