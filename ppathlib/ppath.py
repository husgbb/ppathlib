from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Union
from urllib.parse import urlparse

import tomli as tomllib

from .exceptions import InvalidConfigurationException
from .remote_path import RemotePath, RemoteProfileClient


PathLike = Union[str, os.PathLike[str]]
_CONFIG_FILENAME = ".ppathlib.toml"
_REMOTE_URI_SCHEMES = {
    "s3": "s3",
    "gs": "gs",
    "az": "azure",
}
_STORAGE_TYPE_TO_URI_SCHEME = {
    "s3": "s3",
    "gs": "gs",
    "azure": "az",
}


@dataclass(frozen=True)
class _ResolvedProfile:
    cache_name: str
    display_name: str
    storage_type: str
    root: Optional[str]
    client_kwargs: dict[str, Any]


_CLIENT_CACHE: dict[tuple[Any, ...], Any] = {}


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _is_remote_uri(path: str) -> bool:
    return "://" in path and bool(urlparse(path).scheme)


def _canonical_storage_type(storage_type: str) -> str:
    lowered = storage_type.strip().lower()
    if lowered == "gcs":
        return "gs"
    if lowered in _STORAGE_TYPE_TO_URI_SCHEME:
        return lowered
    raise InvalidConfigurationException(f"Unsupported storage type for ppathlib: {storage_type}")


def _scheme_for_storage_type(storage_type: str) -> str:
    return _STORAGE_TYPE_TO_URI_SCHEME[storage_type]


def _storage_type_for_remote_uri(uri: str) -> str:
    scheme = urlparse(uri).scheme.lower()
    storage_type = _REMOTE_URI_SCHEMES.get(scheme)
    if storage_type is None:
        raise InvalidConfigurationException(f"Unsupported remote URI scheme for ppathlib: {uri}")
    return storage_type


def _join_remote_uri(base_uri: str, *parts: str) -> str:
    cleaned_parts = [
        part.replace("\\", "/").strip("/") for part in parts if part and part.strip("/\\")
    ]
    if not cleaned_parts:
        return base_uri.rstrip("/") if base_uri.endswith("://") is False else base_uri
    return f"{base_uri.rstrip('/')}/{'/'.join(cleaned_parts)}"


def _project_config_path(start_dir: Optional[Path] = None) -> Optional[Path]:
    current = (start_dir or Path.cwd()).resolve()
    for directory in (current, *current.parents):
        candidate = directory / _CONFIG_FILENAME
        if candidate.is_file():
            return candidate
    return None


def _global_config_path() -> Path:
    return Path.home() / ".config" / "ppathlib" / _CONFIG_FILENAME


def _discover_config_path() -> Optional[Path]:
    project_path = _project_config_path()
    if project_path is not None:
        return project_path

    global_path = _global_config_path()
    if global_path.is_file():
        return global_path

    return None


def _load_config_document(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        document = tomllib.load(handle)
    if not isinstance(document, dict):
        raise InvalidConfigurationException(f"Configuration document must be a TOML table: {path}")
    return document


def _profiles_table(document: Mapping[str, Any], path: Path) -> Mapping[str, Any]:
    profiles = document.get("profiles", {})
    if not isinstance(profiles, Mapping):
        raise InvalidConfigurationException(f"`profiles` must be a TOML table in configuration file: {path}")
    return profiles


def _lookup_profile_table(profiles: Mapping[str, Any], profile: str) -> Optional[Mapping[str, Any]]:
    requested = profile.casefold()
    for key, value in profiles.items():
        if key.casefold() == requested:
            if not isinstance(value, Mapping):
                raise InvalidConfigurationException(f"Profile {profile} must be a TOML table.")
            return value
    return None


def _resolve_profile(profile: str) -> _ResolvedProfile:
    display_name = profile.strip()
    if not display_name:
        raise InvalidConfigurationException("`profile` must not be empty.")

    config_path = _discover_config_path()
    if config_path is None:
        raise InvalidConfigurationException(
            f"Profile {display_name} could not be resolved because no {_CONFIG_FILENAME} file was found."
        )

    document = _load_config_document(config_path)
    profiles = _profiles_table(document, config_path)
    raw_profile = _lookup_profile_table(profiles, display_name)
    if raw_profile is None:
        raise InvalidConfigurationException(
            f"Profile {display_name} was not found in configuration file: {config_path}"
        )

    storage_type_value = raw_profile.get("storage_type")
    if not isinstance(storage_type_value, str) or not storage_type_value.strip():
        raise InvalidConfigurationException(
            f"Profile {display_name} must define a non-empty `storage_type`."
        )

    root_value = raw_profile.get("root")
    if root_value is not None and not isinstance(root_value, str):
        raise InvalidConfigurationException(f"Profile {display_name} must define `root` as a string.")

    client_kwargs = {
        key: value for key, value in raw_profile.items() if key not in {"storage_type", "root"}
    }
    return _ResolvedProfile(
        cache_name=display_name.casefold(),
        display_name=display_name,
        storage_type=_canonical_storage_type(storage_type_value),
        root=root_value,
        client_kwargs=client_kwargs,
    )


def _public_profile_for_uri(uri: str) -> _ResolvedProfile:
    return _ResolvedProfile(
        cache_name="public",
        display_name="PUBLIC",
        storage_type=_storage_type_for_remote_uri(uri),
        root=None,
        client_kwargs={},
    )


def _get_client_for_resolved_profile(resolved: _ResolvedProfile) -> RemoteProfileClient:
    cache_key = (
        resolved.cache_name,
        resolved.storage_type,
        resolved.root,
        _freeze(resolved.client_kwargs),
    )
    cached = _CLIENT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = RemoteProfileClient(
        profile_name=resolved.display_name,
        storage_type=resolved.storage_type,
        root=resolved.root,
        client_kwargs=dict(resolved.client_kwargs),
    )
    _CLIENT_CACHE[cache_key] = client
    return client


def _resolve_remote_path(path: PathLike, parts: tuple[PathLike, ...], resolved: _ResolvedProfile) -> str:
    raw_path = os.fspath(path)
    raw_parts = tuple(os.fspath(part) for part in parts)

    if _is_remote_uri(raw_path):
        expected_scheme = _scheme_for_storage_type(resolved.storage_type)
        actual_scheme = urlparse(raw_path).scheme.lower()
        if actual_scheme != expected_scheme:
            raise InvalidConfigurationException(
                f"Profile {resolved.display_name} expects {expected_scheme}:// paths: {raw_path}"
            )
        return _join_remote_uri(raw_path, *raw_parts)

    if Path(raw_path).is_absolute():
        raise InvalidConfigurationException(
            f"Profiled paths must be relative or explicit remote URIs: {raw_path}"
        )

    if resolved.root is None:
        raise InvalidConfigurationException(
            f"Relative remote path requires `root` in profile {resolved.display_name}."
        )

    return _join_remote_uri(resolved.root, raw_path, *raw_parts)


def _build_implementation(
    path: PathLike,
    *parts: PathLike,
    profile: Optional[str] = None,
) -> tuple[Any, str]:
    raw_path = os.fspath(path)
    raw_parts = tuple(os.fspath(part) for part in parts)

    if profile is None:
        if _is_remote_uri(raw_path):
            resolved = _public_profile_for_uri(raw_path)
            client = _get_client_for_resolved_profile(resolved)
            return RemotePath(_join_remote_uri(raw_path, *raw_parts), client=client), "remote"
        return Path(raw_path, *raw_parts), "local"

    resolved = _resolve_profile(profile)
    remote_path = _resolve_remote_path(raw_path, raw_parts, resolved)
    return RemotePath(remote_path, client=_get_client_for_resolved_profile(resolved)), "remote"


def _unwrap_value(value: Any) -> Any:
    if isinstance(value, PPath):
        return value._impl
    if isinstance(value, list):
        return [_unwrap_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_unwrap_value(item) for item in value)
    if isinstance(value, dict):
        return {key: _unwrap_value(item) for key, item in value.items()}
    return value


class PPath(os.PathLike[str]):
    def __init__(
        self,
        path: PathLike,
        *parts: PathLike,
        profile: Optional[str] = None,
    ) -> None:
        impl, mode = _build_implementation(path, *parts, profile=profile)
        self._impl = impl
        self._mode = mode

    @classmethod
    def _from_impl(cls, impl: Any, mode: str) -> "PPath":
        instance = cls.__new__(cls)
        instance._impl = impl
        instance._mode = mode
        return instance

    @property
    def mode(self) -> str:
        return self._mode

    def __fspath__(self) -> str:
        return os.fspath(self._impl)

    def __str__(self) -> str:
        return str(self._impl)

    def __repr__(self) -> str:
        return f"PPath({str(self)!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PPath):
            return self._impl == other._impl
        return self._impl == other

    def __hash__(self) -> int:
        return hash((type(self), self._mode, self._impl))

    def __truediv__(self, other: PathLike) -> "PPath":
        return self._wrap_result(self._impl / _unwrap_value(other))

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._impl, name)
        if callable(attribute):
            def wrapped(*args: Any, **kwargs: Any) -> Any:
                result = attribute(*_unwrap_value(args), **_unwrap_value(kwargs))
                return self._wrap_result(result)

            return wrapped
        return self._wrap_result(attribute)

    def _wrap_result(self, result: Any) -> Any:
        if isinstance(result, (Path, RemotePath)):
            mode = "remote" if isinstance(result, RemotePath) else "local"
            return type(self)._from_impl(result, mode=mode)
        if isinstance(result, tuple):
            return tuple(self._wrap_result(item) for item in result)
        if isinstance(result, list):
            return [self._wrap_result(item) for item in result]
        return result


def get_client(profile: str) -> RemoteProfileClient:
    return _get_client_for_resolved_profile(_resolve_profile(profile))


def clear_client_cache() -> None:
    _CLIENT_CACHE.clear()
