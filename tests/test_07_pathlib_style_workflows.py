import pandas as pd

from ppathlib import PPath
from tests.support import write_project_config


def test_remote_mode_supports_pathlib_style_file_round_trip(project_dir):
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

    data_dir = PPath("datasets", profile="analytics")

    input_path = data_dir / "source/input.csv"
    output_path = data_dir / "output/pred.csv"

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
