from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Union

from .azure.azblobclient import AzureBlobClient
from .azure.azblobpath import AzureBlobPath
from .cloudpath import implementation_registry
from .enums import FileCacheMode
from .exceptions import InvalidConfigurationException, MissingDependenciesError
from .gs.gsclient import GSClient
from .gs.gspath import GSPath
from .s3.s3client import S3Client
from .s3.s3path import S3Path


_PROFILE_TOKEN_RE = re.compile(r"[^0-9A-Za-z]+")
_REMOTE_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


@dataclass(frozen=True)
class _ResolvedProfile:
    normalized_name: str
    storage_type: str
    root: Optional[str]
    path_class: type
    client_kwargs: dict[str, Any]


_CLIENT_CACHE: dict[tuple[Any, ...], Any] = {}


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
    aliases = {
        "s3": "s3",
        "gs": "gs",
        "gcs": "gs",
        "azure": "azure",
        "az": "azure",
        "azblob": "azure",
    }
    canonical = aliases.get(lowered)
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


def _build_s3_client(client_kwargs: Mapping[str, Any]) -> S3Client:
    implementation_registry["s3"].validate_completeness()

    from .s3 import s3client as s3client_module

    session_factory = getattr(s3client_module, "Session", None)
    if session_factory is None:
        raise MissingDependenciesError(
            "Missing dependencies for S3Client. Install with `pip install ppathlib[s3]`."
        )

    session_kwargs = {
        "aws_access_key_id": client_kwargs.get("aws_access_key_id"),
        "aws_secret_access_key": client_kwargs.get("aws_secret_access_key"),
        "aws_session_token": client_kwargs.get("aws_session_token"),
        "region_name": client_kwargs.get("region_name"),
    }
    session_kwargs = {key: value for key, value in session_kwargs.items() if value is not None}

    return S3Client(
        boto3_session=session_factory(**session_kwargs),
        endpoint_url=client_kwargs.get("endpoint_url"),
        no_sign_request=bool(client_kwargs.get("no_sign_request", False)),
        file_cache_mode=client_kwargs.get("file_cache_mode"),
        local_cache_dir=client_kwargs.get("local_cache_dir"),
    )


def _build_gs_client(client_kwargs: Mapping[str, Any]) -> GSClient:
    implementation_registry["gs"].validate_completeness()
    return GSClient(**client_kwargs)


def _build_azure_client(client_kwargs: Mapping[str, Any]) -> AzureBlobClient:
    implementation_registry["azure"].validate_completeness()
    return AzureBlobClient(**client_kwargs)


def _resolve_cache_settings(profile: str, env: Mapping[str, str]) -> dict[str, Any]:
    settings: dict[str, Any] = {}

    cache_dir = _get_env(profile, "CACHE_DIR", env)
    if cache_dir is not None:
        settings["local_cache_dir"] = str(Path(cache_dir) / profile)

    cache_mode = _get_env(profile, "CACHE_MODE", env)
    if cache_mode is not None:
        try:
            settings["file_cache_mode"] = FileCacheMode(cache_mode.strip().lower())
        except ValueError as exc:
            raise InvalidConfigurationException(
                f"Invalid cache mode for {_profile_env_name(profile, 'CACHE_MODE')}: {cache_mode}"
            ) from exc

    return settings


def _resolve_s3_profile(profile: str, env: Mapping[str, str]) -> _ResolvedProfile:
    no_sign_request = _parse_bool(profile, "NO_SIGN_REQUEST", env) or False

    client_kwargs = {
        "endpoint_url": _get_env(profile, "ENDPOINT_URL", env),
        "aws_access_key_id": _get_env(profile, "ACCESS_KEY_ID", env, required=not no_sign_request),
        "aws_secret_access_key": _get_env(
            profile,
            "SECRET_ACCESS_KEY",
            env,
            required=not no_sign_request,
        ),
        "aws_session_token": _get_env(profile, "SESSION_TOKEN", env),
        "region_name": _get_env(profile, "REGION", env),
        "no_sign_request": no_sign_request,
    }
    client_kwargs.update(_resolve_cache_settings(profile, env))

    return _ResolvedProfile(
        normalized_name=profile,
        storage_type="s3",
        root=_get_env(profile, "ROOT", env),
        path_class=S3Path,
        client_kwargs=client_kwargs,
    )


def _resolve_gs_profile(profile: str, env: Mapping[str, str]) -> _ResolvedProfile:
    client_kwargs = {
        "project": _get_env(profile, "PROJECT", env, required=True),
        "application_credentials": _get_env(profile, "CREDENTIALS_JSON", env, required=True),
    }
    client_kwargs.update(_resolve_cache_settings(profile, env))

    if _get_env(profile, "STORAGE_CLIENT_JSON", env) is not None:
        raise InvalidConfigurationException(
            f"{_profile_env_name(profile, 'STORAGE_CLIENT_JSON')} is not supported by this prototype."
        )

    return _ResolvedProfile(
        normalized_name=profile,
        storage_type="gs",
        root=_get_env(profile, "ROOT", env),
        path_class=GSPath,
        client_kwargs=client_kwargs,
    )


def _resolve_azure_profile(profile: str, env: Mapping[str, str]) -> _ResolvedProfile:
    client_secret = _get_env(profile, "CLIENT_SECRET", env)
    tenant_id = _get_env(profile, "TENANT_ID", env)
    client_id = _get_env(profile, "CLIENT_ID", env)

    if any(value is not None for value in (client_secret, tenant_id, client_id)):
        raise InvalidConfigurationException(
            f"Service principal auth is not supported by this PPath prototype for profile {profile}."
        )

    connection_string = _get_env(profile, "CONNECTION_STRING", env)
    if connection_string is not None:
        client_kwargs = {"connection_string": connection_string}
    else:
        account_url = _get_env(profile, "ACCOUNT_URL", env, required=True)
        account_key = _get_env(profile, "ACCOUNT_KEY", env, required=True)
        client_kwargs = {"account_url": account_url, "credential": account_key}
    client_kwargs.update(_resolve_cache_settings(profile, env))

    return _ResolvedProfile(
        normalized_name=profile,
        storage_type="azure",
        root=_get_env(profile, "ROOT", env),
        path_class=AzureBlobPath,
        client_kwargs=client_kwargs,
    )


def _resolve_profile(profile: str, env: Optional[Mapping[str, str]] = None) -> _ResolvedProfile:
    effective_env = env if env is not None else os.environ
    normalized_name = _normalize_profile_name(profile)
    storage_type = _canonical_storage_type(
        _get_env(normalized_name, "STORAGE_TYPE", effective_env, required=True) or ""
    )

    if storage_type == "s3":
        return _resolve_s3_profile(normalized_name, effective_env)
    if storage_type == "gs":
        return _resolve_gs_profile(normalized_name, effective_env)
    if storage_type == "azure":
        return _resolve_azure_profile(normalized_name, effective_env)

    raise InvalidConfigurationException(f"Unsupported storage type for PPath prototype: {storage_type}")


def _get_client_for_resolved_profile(resolved: _ResolvedProfile) -> Any:
    cache_key = (resolved.storage_type, _freeze(resolved.client_kwargs))
    cached = _CLIENT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if resolved.storage_type == "s3":
        client = _build_s3_client(resolved.client_kwargs)
    elif resolved.storage_type == "gs":
        client = _build_gs_client(resolved.client_kwargs)
    elif resolved.storage_type == "azure":
        client = _build_azure_client(resolved.client_kwargs)
    else:
        raise InvalidConfigurationException(
            f"Unsupported storage type for PPath prototype: {resolved.storage_type}"
        )

    _CLIENT_CACHE[cache_key] = client
    return client


def _resolve_remote_path(
    path: Union[str, os.PathLike[str]],
    parts: tuple[Union[str, os.PathLike[str]], ...],
    resolved: _ResolvedProfile,
) -> str:
    raw_path = os.fspath(path)
    raw_parts = tuple(os.fspath(part) for part in parts)

    if _is_remote_uri(raw_path):
        if not resolved.path_class.is_valid_cloudpath(raw_path, raise_on_error=False):
            raise InvalidConfigurationException(
                f"Profile {resolved.normalized_name} expects {resolved.path_class.cloud_prefix} paths: {raw_path}"
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
    return resolved.path_class(remote_path, client=_get_client_for_resolved_profile(resolved))
