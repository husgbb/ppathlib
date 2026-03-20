from __future__ import annotations

from dataclasses import dataclass
import os
import posixpath
from pathlib import PurePosixPath
from typing import Any, NoReturn, Optional, Union
from urllib.parse import urlparse

from .exceptions import InvalidConfigurationException


PathLike = Union[str, os.PathLike[str]]


@dataclass(frozen=True)
class RemoteScope:
    raw_uri: str
    scheme: str
    container: str
    prefix: str


@dataclass(frozen=True)
class RemoteBindingRequest:
    profile_name: str
    storage_type: str
    scope: RemoteScope
    connection_options: dict[str, Any]


@dataclass(frozen=True)
class RemoteProfileClient:
    profile_name: str
    storage_type: str
    root: Optional[str]
    client_kwargs: dict[str, Any]

    def _expected_scheme(self) -> str:
        if self.storage_type == "s3":
            return "s3"
        if self.storage_type == "gs":
            return "gs"
        if self.storage_type == "azure":
            return "az"
        raise InvalidConfigurationException(
            f"Unsupported storage type for remote profile client: {self.storage_type}"
        )

    def resolve_scope(self, uri: Optional[str] = None) -> RemoteScope:
        raw_uri = uri or self.root
        if raw_uri is None:
            raise InvalidConfigurationException(
                f"Profile {self.profile_name} has no root URI. Pass an explicit remote URI to resolve scope."
            )

        parsed = urlparse(raw_uri)
        if not parsed.scheme:
            raise InvalidConfigurationException(
                f"Remote store scope requires an explicit URI with a scheme: {raw_uri}"
            )

        expected_scheme = self._expected_scheme()
        if parsed.scheme.lower() != expected_scheme:
            raise InvalidConfigurationException(
                f"Profile {self.profile_name} expects {expected_scheme}:// URIs: {raw_uri}"
            )

        container = parsed.netloc
        if not container:
            raise InvalidConfigurationException(
                f"Remote store scope requires a bucket or container name: {raw_uri}"
            )

        return RemoteScope(
            raw_uri=raw_uri,
            scheme=parsed.scheme.lower(),
            container=container,
            prefix=parsed.path.lstrip("/"),
        )

    def binding_request(self, uri: Optional[str] = None) -> RemoteBindingRequest:
        return RemoteBindingRequest(
            profile_name=self.profile_name,
            storage_type=self.storage_type,
            scope=self.resolve_scope(uri),
            connection_options=dict(self.client_kwargs),
        )

    def create_store(self, uri: Optional[str] = None) -> Any:
        binding = self.binding_request(uri)
        raise NotImplementedError(
            f"RemoteProfileClient.create_store() is not implemented. "
            f"Profile={binding.profile_name}. Storage type={binding.storage_type}. "
            f"Root={binding.scope.raw_uri}. Required implementation: bind the abstract "
            f"RemoteBindingRequest to a concrete store constructor in the runtime layer."
        )


class RemotePath(os.PathLike[str]):
    """Prototype remote path for the current harness rewrite."""

    def __init__(self, uri: str, *, client: RemoteProfileClient) -> None:
        parsed = urlparse(uri)
        if not parsed.scheme:
            raise InvalidConfigurationException(
                f"Remote paths must be explicit URIs with a scheme: {uri}"
            )

        self._raw_uri = uri
        self._client = client
        self._scheme = parsed.scheme
        self._drive = parsed.netloc
        self._key = parsed.path.lstrip("/")
        lexical = "/".join(part for part in [self._drive, self._key] if part)
        self._pure = PurePosixPath("/" + lexical) if lexical else PurePosixPath("/")

    @property
    def client(self) -> RemoteProfileClient:
        return self._client

    def __repr__(self) -> str:
        return f"RemotePath({self._raw_uri!r})"

    def __str__(self) -> str:
        return self._raw_uri

    def __fspath__(self) -> str:
        return self._raw_uri

    def __hash__(self) -> int:
        return hash((type(self), self._raw_uri))

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RemotePath) and self._raw_uri == other._raw_uri

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, RemotePath):
            return NotImplemented
        return self.parts < other.parts

    def __truediv__(self, other: PathLike) -> "RemotePath":
        return self.joinpath(other)

    @property
    def anchor(self) -> str:
        return f"{self._scheme}://"

    @property
    def drive(self) -> str:
        return self._drive

    @property
    def name(self) -> str:
        return self._pure.name

    @property
    def stem(self) -> str:
        return self._pure.stem

    @property
    def suffix(self) -> str:
        return self._pure.suffix

    @property
    def suffixes(self) -> list[str]:
        return list(self._pure.suffixes)

    @property
    def parent(self) -> "RemotePath":
        return self._from_pure(self._pure.parent)

    @property
    def parents(self) -> tuple["RemotePath", ...]:
        return tuple(self._from_pure(parent) for parent in self._pure.parents)

    @property
    def parts(self) -> tuple[str, ...]:
        pure_parts = tuple(part for part in self._pure.parts if part != "/")
        if pure_parts:
            return (self.anchor, *pure_parts)
        return (self.anchor,)

    def as_uri(self) -> str:
        return self._raw_uri

    def absolute(self) -> "RemotePath":
        return self

    def is_absolute(self) -> bool:
        return True

    def resolve(self, strict: bool = False) -> "RemotePath":
        return self

    def joinpath(self, *pathsegments: PathLike) -> "RemotePath":
        pure = self._pure
        for segment in pathsegments:
            pure = pure.joinpath(os.fspath(segment).replace("\\", "/"))
        return self._from_pure(pure)

    def with_name(self, name: str) -> "RemotePath":
        return self._from_pure(self._pure.with_name(name))

    def with_stem(self, stem: str) -> "RemotePath":
        return self._from_pure(self._pure.with_stem(stem))

    def with_suffix(self, suffix: str) -> "RemotePath":
        return self._from_pure(self._pure.with_suffix(suffix))

    def relative_to(self, other: "RemotePath", walk_up: bool = False) -> PurePosixPath:
        if not isinstance(other, RemotePath):
            raise ValueError(f"{other!r} is not a RemotePath")
        if self.anchor != other.anchor:
            raise ValueError(f"{self} and {other} use different URI schemes")
        if not walk_up:
            return self._pure.relative_to(other._pure)
        relative = posixpath.relpath(self._pure.as_posix(), other._pure.as_posix())
        return PurePosixPath(relative)

    def is_relative_to(self, other: "RemotePath") -> bool:
        try:
            self.relative_to(other)
            return True
        except ValueError:
            return False

    def match(self, path_pattern: str, *, case_sensitive: Optional[bool] = None) -> bool:
        if case_sensitive is False:
            return PurePosixPath(self._pure.as_posix().lower()).match(path_pattern.lower())
        return self._pure.match(path_pattern)

    def full_match(self, pattern: str, *, case_sensitive: Optional[bool] = None) -> bool:
        candidate = self._pure.as_posix()
        if case_sensitive is False:
            candidate = candidate.lower()
            pattern = pattern.lower()
        return PurePosixPath(candidate).match(pattern)

    def open(self, *args: Any, **kwargs: Any) -> Any:
        self._not_implemented(
            "open",
            "bind this method to open_reader/open_writer, or to a local cache wrapper that "
            "downloads before read and uploads on close for write modes",
        )

    def read_text(self, *args: Any, **kwargs: Any) -> str:
        self._not_implemented(
            "read_text",
            "load text bytes through the runtime reader interface and decode with the requested "
            "encoding, errors, and newline semantics",
        )

    def write_text(self, *args: Any, **kwargs: Any) -> int:
        self._not_implemented(
            "write_text",
            "encode text exactly once and persist it through the runtime writer interface",
        )

    def read_bytes(self) -> bytes:
        self._not_implemented(
            "read_bytes",
            "load object bytes through the runtime reader interface without materializing "
            "local cache semantics yet",
        )

    def write_bytes(self, data: bytes) -> int:
        self._not_implemented(
            "write_bytes",
            "persist bytes through the runtime writer interface and return the written byte count",
        )

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        self._not_implemented(
            "exists",
            "map portable existence checks to store head or list semantics for files and "
            "directory-like prefixes",
        )

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        self._not_implemented(
            "is_file",
            "differentiate object keys from directory-like prefixes using head plus prefix "
            "listing rules",
        )

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        self._not_implemented(
            "is_dir",
            "treat directory-like prefixes consistently across supported stores using list semantics",
        )

    def iterdir(self):
        self._not_implemented(
            "iterdir",
            "emit immediate children only, without recursion, using list_with_delimiter or "
            "equivalent prefix filtering",
        )

    def glob(self, pattern: PathLike, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False):
        self._not_implemented(
            "glob",
            "combine lexical pathlib-style pattern matching with store-backed child listing "
            "for the minimum required subtree",
        )

    def rglob(self, pattern: PathLike, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False):
        self._not_implemented(
            "rglob",
            "combine recursive store listing with pathlib-compatible pattern filtering",
        )

    def walk(self, top_down: bool = True, on_error: Any = None, follow_symlinks: bool = False):
        self._not_implemented(
            "walk",
            "materialize a deterministic directory tree view from object prefixes and return "
            "tuples compatible with pathlib.Path.walk()",
        )

    def copy(self, target: PathLike) -> Any:
        self._not_implemented(
            "copy",
            "support remote-to-remote copy through the runtime copy interface and define explicit "
            "fallback behavior for remote-to-local and local-to-remote transfers",
        )

    def move(self, target: PathLike) -> Any:
        self._not_implemented(
            "move",
            "support remote-to-remote rename through the runtime rename interface and define "
            "explicit fallback behavior when a direct rename is unavailable",
        )

    def rename(self, target: PathLike) -> Any:
        self._not_implemented(
            "rename",
            "alias or specialize move semantics with pathlib-compatible return behavior",
        )

    def replace(self, target: PathLike) -> Any:
        self._not_implemented(
            "replace",
            "define overwrite semantics explicitly and map them to rename or copy-plus-delete",
        )

    def unlink(self, missing_ok: bool = False) -> None:
        self._not_implemented(
            "unlink",
            "delete a single object key through the runtime delete interface and implement "
            "pathlib-compatible missing_ok behavior",
        )

    def _from_pure(self, pure: PurePosixPath) -> "RemotePath":
        lexical = pure.as_posix().lstrip("/")
        uri = f"{self.anchor}{lexical}" if lexical else self.anchor
        return type(self)(uri, client=self.client)

    def _not_implemented(self, method: str, required_implementation: str) -> NoReturn:
        raise NotImplementedError(
            f"PPath.{method}() is not implemented for remote mode. Path={self}. "
            f"Profile={self.client.profile_name}. Storage type={self.client.storage_type}. "
            f"Required implementation: {required_implementation}."
        )
