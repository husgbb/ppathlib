from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Union

from .exceptions import InvalidConfigurationException
from .remote_path import RemotePath, RemoteProfileClient


_PROFILE_TOKEN_RE = re.compile(r"[^0-9A-Za-z]+")
_REMOTE_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


@dataclass(frozen=True)
class _ResolvedProfile:
    normalized_name: str
    storage_type: str
    root: Optional[str]
    client_kwargs: dict[str, Any]


@dataclass(frozen=True)
class _ProfileSpec:
    canonical_storage_type: str
    uri_scheme: str
    required_env_keys: tuple[str, ...] = ()
    optional_env_keys: tuple[str, ...] = ()
    unsupported_env_keys: tuple[str, ...] = ()
    env_key_renames: tuple[tuple[str, str], ...] = ()


_CLIENT_CACHE: dict[tuple[Any, ...], Any] = {}
_STORAGE_TYPE_ALIASES = {
    "s3": "s3",
    "gs": "gs",
    "gcs": "gs",
    "azure": "azure",
    "az": "azure",
    "azblob": "azure",
}
_PROFILE_SPECS = {
    "s3": _ProfileSpec(
        canonical_storage_type="s3",
        uri_scheme="s3",
        optional_env_keys=(
            "ACCESS_KEY_ID",
            "SECRET_ACCESS_KEY",
            "ENDPOINT_URL",
            "SESSION_TOKEN",
            "REGION",
            "NO_SIGN_REQUEST",
        ),
        env_key_renames=(
            ("ACCESS_KEY_ID", "access_key_id"),
            ("SECRET_ACCESS_KEY", "secret_access_key"),
            ("SESSION_TOKEN", "session_token"),
            ("ENDPOINT_URL", "endpoint_url"),
            ("REGION", "region"),
            ("NO_SIGN_REQUEST", "no_sign_request"),
        ),
    ),
    "gs": _ProfileSpec(
        canonical_storage_type="gs",
        uri_scheme="gs",
        required_env_keys=("CREDENTIALS_JSON",),
        unsupported_env_keys=("PROJECT", "STORAGE_CLIENT_JSON"),
        env_key_renames=(
            ("CREDENTIALS_JSON", "credentials_json"),
        ),
    ),
    "azure": _ProfileSpec(
        canonical_storage_type="azure",
        uri_scheme="az",
        optional_env_keys=("ACCOUNT_URL", "ACCOUNT_KEY"),
        unsupported_env_keys=("CONNECTION_STRING", "TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"),
        env_key_renames=(
            ("ACCOUNT_URL", "account_url"),
            ("ACCOUNT_KEY", "account_key"),
        ),
    ),
}


def _normalize_profile_name(profile: str) -> str:
    normalized = _PROFILE_TOKEN_RE.sub("_", profile.strip()).strip("_").upper()
    if not normalized:
        raise InvalidConfigurationException("`profile` must not be empty.")
    return normalized


def _profile_env_name(profile: str, key: str) -> str:
    return f"{profile}_{key}"


def _get_env(
    profile: str,
    key: str,
    env: Mapping[str, str],
    *,
    required: bool = False,
) -> Optional[str]:
    value = env.get(_profile_env_name(profile, key))
    if value is None or value == "":
        if required:
            raise InvalidConfigurationException(
                f"Missing required ppathlib environment variable: {_profile_env_name(profile, key)}"
            )
        return None
    return value


def _parse_bool(profile: str, key: str, env: Mapping[str, str]) -> Optional[bool]:
    value = _get_env(profile, key, env)
    if value is None:
        return None

    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False

    raise InvalidConfigurationException(
        f"Invalid boolean value for {_profile_env_name(profile, key)}: {value}"
    )


def _canonical_storage_type(storage_type: str) -> str:
    lowered = storage_type.strip().lower()
    canonical = _STORAGE_TYPE_ALIASES.get(lowered)
    if canonical is None:
        raise InvalidConfigurationException(f"Unsupported storage type for PPath prototype: {storage_type}")
    return canonical


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _is_remote_uri(path: str) -> bool:
    return bool(_REMOTE_URI_RE.match(path))


def _join_remote_uri(base_uri: str, *parts: str) -> str:
    cleaned_parts = [
        part.replace("\\", "/").strip("/") for part in parts if part and part.strip("/\\")
    ]
    if not cleaned_parts:
        return base_uri
    return f"{base_uri.rstrip('/')}/{'/'.join(cleaned_parts)}"


def _resolve_backend_settings(profile: str, env: Mapping[str, str], spec: _ProfileSpec) -> dict[str, Any]:
    for env_key in spec.unsupported_env_keys:
        if _get_env(profile, env_key, env) is not None:
            raise InvalidConfigurationException(
                f"{_profile_env_name(profile, env_key)} is not supported by this prototype."
            )

    raw_values: dict[str, Any] = {}

    for env_key in spec.required_env_keys:
        raw_values[env_key] = _get_env(profile, env_key, env, required=True)

    for env_key in spec.optional_env_keys:
        raw_values[env_key] = _get_env(profile, env_key, env)

    if "NO_SIGN_REQUEST" in raw_values and raw_values["NO_SIGN_REQUEST"] is not None:
        raw_values["NO_SIGN_REQUEST"] = _parse_bool(profile, "NO_SIGN_REQUEST", env)

    if spec.canonical_storage_type == "s3":
        no_sign_request = bool(raw_values.get("NO_SIGN_REQUEST") or False)
        raw_values["NO_SIGN_REQUEST"] = no_sign_request
        if not no_sign_request:
            raw_values["ACCESS_KEY_ID"] = _get_env(profile, "ACCESS_KEY_ID", env, required=True)
            raw_values["SECRET_ACCESS_KEY"] = _get_env(
                profile,
                "SECRET_ACCESS_KEY",
                env,
                required=True,
            )

    if spec.canonical_storage_type == "gs":
        raw_values["CREDENTIALS_JSON"] = _get_env(profile, "CREDENTIALS_JSON", env, required=True)

    if spec.canonical_storage_type == "azure":
        raw_values["ACCOUNT_URL"] = _get_env(profile, "ACCOUNT_URL", env, required=True)
        raw_values["ACCOUNT_KEY"] = _get_env(profile, "ACCOUNT_KEY", env, required=True)

    resolved: dict[str, Any] = {}
    rename_map = dict(spec.env_key_renames)
    for env_key, value in raw_values.items():
        if value is None:
            continue
        resolved[rename_map.get(env_key, env_key.lower())] = value

    return resolved


def _resolve_profile(profile: str, env: Optional[Mapping[str, str]] = None) -> _ResolvedProfile:
    effective_env = env if env is not None else os.environ
    normalized_name = _normalize_profile_name(profile)
    storage_type = _canonical_storage_type(
        _get_env(normalized_name, "STORAGE_TYPE", effective_env, required=True) or ""
    )
    spec = _PROFILE_SPECS[storage_type]

    return _ResolvedProfile(
        normalized_name=normalized_name,
        storage_type=spec.canonical_storage_type,
        root=_get_env(normalized_name, "ROOT", effective_env),
        client_kwargs=_resolve_backend_settings(normalized_name, effective_env, spec),
    )


def _get_client_for_resolved_profile(resolved: _ResolvedProfile) -> Any:
    cache_key = (
        resolved.normalized_name,
        resolved.storage_type,
        resolved.root,
        _freeze(resolved.client_kwargs),
    )
    cached = _CLIENT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = RemoteProfileClient(
        profile_name=resolved.normalized_name,
        storage_type=resolved.storage_type,
        root=resolved.root,
        client_kwargs=dict(resolved.client_kwargs),
    )

    _CLIENT_CACHE[cache_key] = client
    return client


def _scheme_for_storage_type(storage_type: str) -> str:
    return _PROFILE_SPECS[storage_type].uri_scheme


def _resolve_remote_path(
    path: Union[str, os.PathLike[str]],
    parts: tuple[Union[str, os.PathLike[str]], ...],
    resolved: _ResolvedProfile,
) -> str:
    raw_path = os.fspath(path)
    raw_parts = tuple(os.fspath(part) for part in parts)

    if _is_remote_uri(raw_path):
        expected_scheme = _scheme_for_storage_type(resolved.storage_type)
        actual_scheme = raw_path.split("://", 1)[0].lower()
        if actual_scheme != expected_scheme:
            raise InvalidConfigurationException(
                f"Profile {resolved.normalized_name} expects {expected_scheme}:// paths: {raw_path}"
            )

        return _join_remote_uri(raw_path, *raw_parts)

    if Path(raw_path).is_absolute():
        raise InvalidConfigurationException(
            f"Profiled paths must be relative or explicit remote URIs: {raw_path}"
        )

    if resolved.root is None:
        raise InvalidConfigurationException(
            f"Relative path requires {_profile_env_name(resolved.normalized_name, 'ROOT')} to be set"
        )

    return _join_remote_uri(resolved.root, raw_path, *raw_parts)


def get_client(profile: str) -> Any:
    resolved = _resolve_profile(profile)
    return _get_client_for_resolved_profile(resolved)


def clear_client_cache() -> None:
    _CLIENT_CACHE.clear()


def PPath(
    path: Union[str, os.PathLike[str]],
    *parts: Union[str, os.PathLike[str]],
    profile: Optional[str] = None,
):
    if profile is None:
        raw_path = os.fspath(path)
        if _is_remote_uri(raw_path):
            raise InvalidConfigurationException(f"Remote paths require a profile: {raw_path}")
        return Path(path, *parts)

    resolved = _resolve_profile(profile)
    remote_path = _resolve_remote_path(path, parts, resolved)
    return RemotePath(remote_path, client=_get_client_for_resolved_profile(resolved))
