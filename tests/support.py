import textwrap
from pathlib import Path


def write_project_config(directory: Path, body: str) -> Path:
    path = directory / ".ppathlib.toml"
    path.write_text(_normalized_toml(body), encoding="utf-8")
    return path


def write_global_config(home_dir: Path, body: str) -> Path:
    config_dir = home_dir / ".config" / "ppathlib"
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / ".ppathlib.toml"
    path.write_text(_normalized_toml(body), encoding="utf-8")
    return path


def _normalized_toml(body: str) -> str:
    return textwrap.dedent(body).strip() + "\n"
