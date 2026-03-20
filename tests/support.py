import textwrap
from pathlib import Path
from typing import Any, Sequence


class _FakeGetResult:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def bytes(self) -> bytes:
        return self._payload


class _InMemoryRemoteStore:
    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, path: str, file: Any, **kwargs: Any) -> dict[str, str]:
        if isinstance(file, bytes):
            payload = file
        elif hasattr(file, "read"):
            payload = file.read()
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
        else:
            payload = bytes(file)
        self._objects[path] = payload
        return {}

    def get(self, path: str, **kwargs: Any) -> _FakeGetResult:
        if path not in self._objects:
            raise FileNotFoundError(path)
        return _FakeGetResult(self._objects[path])

    def head(self, path: str) -> dict[str, Any]:
        if path not in self._objects:
            raise FileNotFoundError(path)
        return {"path": path, "size": len(self._objects[path])}

    def list(self, prefix: str | None = None, **kwargs: Any) -> list[list[dict[str, Any]]]:
        normalized = "" if prefix in (None, "") else prefix
        objects = [
            {"path": key, "size": len(value)}
            for key, value in sorted(self._objects.items())
            if key.startswith(normalized)
        ]
        return [objects]

    def list_with_delimiter(self, prefix: str | None = None, **kwargs: Any) -> dict[str, Any]:
        normalized = "" if prefix in (None, "") else prefix
        objects: list[dict[str, Any]] = []
        common_prefixes: set[str] = set()

        for key, value in sorted(self._objects.items()):
            if not key.startswith(normalized):
                continue
            remainder = key[len(normalized) :]
            if not remainder:
                objects.append({"path": key, "size": len(value)})
                continue
            if "/" not in remainder:
                objects.append({"path": key, "size": len(value)})
                continue
            child_name = remainder.split("/", 1)[0]
            common_prefixes.add(f"{normalized}{child_name}")

        return {
            "common_prefixes": sorted(common_prefixes),
            "objects": objects,
        }

    def delete(self, path: str | Sequence[str]) -> None:
        if isinstance(path, str):
            if path not in self._objects:
                raise FileNotFoundError(path)
            self._objects.pop(path)
            return
        if isinstance(path, (list, tuple)):
            for item in path:
                if item not in self._objects:
                    raise FileNotFoundError(item)
                self._objects.pop(item)
            return
        for item in path:
            if item not in self._objects:
                raise FileNotFoundError(item)
            self._objects.pop(item)

    def copy(self, from_: str, to: str, *, overwrite: bool = True) -> None:
        if from_ not in self._objects:
            raise FileNotFoundError(from_)
        if not overwrite and to in self._objects:
            raise FileExistsError(to)
        self._objects[to] = self._objects[from_]

    def rename(self, from_: str, to: str, *, overwrite: bool = True) -> None:
        if from_ not in self._objects:
            raise FileNotFoundError(from_)
        if not overwrite and to in self._objects:
            raise FileExistsError(to)
        self._objects[to] = self._objects.pop(from_)


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


def install_in_memory_remote_store(monkeypatch) -> _InMemoryRemoteStore:
    from ppathlib.remote_path import RemoteProfileClient

    store = _InMemoryRemoteStore()
    monkeypatch.setattr(RemoteProfileClient, "create_store", lambda self, *args, **kwargs: store)
    return store
