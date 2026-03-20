from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
import io
import os
import posixpath
from pathlib import Path, PurePosixPath
from typing import Any, NoReturn, Optional, Union
from urllib.parse import urlparse

from obstore.store import AzureStore, GCSStore, S3Store

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
        kwargs = dict(binding.connection_options)
        prefix = binding.scope.prefix or None

        if binding.storage_type == "s3":
            return S3Store(binding.scope.container, prefix=prefix, **kwargs)
        if binding.storage_type == "gs":
            return GCSStore(binding.scope.container, prefix=prefix, **kwargs)
        if binding.storage_type == "azure":
            return AzureStore(binding.scope.container, prefix=prefix, **kwargs)

        raise InvalidConfigurationException(
            f"Unsupported storage type for remote store creation: {binding.storage_type}"
        )


class _RemoteBytesWriter(io.BytesIO):
    def __init__(self, store: Any, key: str) -> None:
        super().__init__()
        self._store = store
        self._key = key

    def close(self) -> None:
        if not self.closed:
            self._store.put(self._key, self.getvalue())
        super().close()


class _RemoteTextWriter(io.StringIO):
    def __init__(self, store: Any, key: str, *, encoding: str, newline: Optional[str]) -> None:
        super().__init__(newline=newline)
        self._store = store
        self._key = key
        self._encoding = encoding

    def close(self) -> None:
        if not self.closed:
            self._store.put(self._key, self.getvalue().encode(self._encoding))
        super().close()


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
        mode = args[0] if args else kwargs.pop("mode", "r")
        if args:
            args = args[1:]

        buffering = kwargs.pop("buffering", -1)
        encoding = kwargs.pop("encoding", None) or "utf-8"
        errors = kwargs.pop("errors", None)
        newline = kwargs.pop("newline", None)

        if args or kwargs:
            raise NotImplementedError(
                "PPath.open() supports only mode, buffering, encoding, errors, and newline "
                "for remote mode in the current runtime slice."
            )
        if buffering not in (-1, 1):
            raise NotImplementedError(
                "PPath.open() does not implement custom buffering behavior for remote mode yet."
            )
        if errors not in (None, "strict"):
            raise NotImplementedError(
                "PPath.open() does not implement non-default error handling for remote mode yet."
            )
        if any(flag in mode for flag in ("a", "x", "+")):
            raise NotImplementedError(
                "PPath.open() does not implement append, exclusive creation, or read/write "
                "modes for remote paths yet."
            )

        store = self._store()
        key = self._store_key()

        if "r" in mode:
            payload = bytes(store.get(key).bytes())
            if "b" in mode:
                return io.BytesIO(payload)
            return io.TextIOWrapper(io.BytesIO(payload), encoding=encoding, newline=newline)

        if "w" in mode:
            if "b" in mode:
                return _RemoteBytesWriter(store, key)
            return _RemoteTextWriter(store, key, encoding=encoding, newline=newline)

        raise NotImplementedError(f"PPath.open() does not support mode={mode!r} for remote paths yet.")

    def read_text(self, *args: Any, **kwargs: Any) -> str:
        encoding = kwargs.pop("encoding", None) or "utf-8"
        errors = kwargs.pop("errors", None) or "strict"
        if args or kwargs:
            raise NotImplementedError(
                "PPath.read_text() supports only the `encoding` and `errors` arguments "
                "for remote mode in the current runtime slice."
            )
        return self.read_bytes().decode(encoding, errors)

    def write_text(self, *args: Any, **kwargs: Any) -> int:
        if not args:
            raise TypeError("PPath.write_text() missing required argument: 'data'")
        data = args[0]
        if not isinstance(data, str):
            raise TypeError("PPath.write_text() requires `data` to be str.")
        encoding = kwargs.pop("encoding", None) or "utf-8"
        errors = kwargs.pop("errors", None) or "strict"
        if len(args) > 1 or kwargs:
            raise NotImplementedError(
                "PPath.write_text() supports only `data`, `encoding`, and `errors` for "
                "remote mode in the current runtime slice."
            )
        payload = data.encode(encoding, errors)
        self.write_bytes(payload)
        return len(data)

    def read_bytes(self) -> bytes:
        return bytes(self._store().get(self._store_key()).bytes())

    def write_bytes(self, data: bytes) -> int:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("PPath.write_bytes() requires a bytes-like object.")
        payload = bytes(data)
        self._store().put(self._store_key(), payload)
        return len(payload)

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        return self.is_file() or self.is_dir()

    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        key = self._store_key()
        if key == "" or self._raw_uri.endswith("/"):
            return False
        try:
            self._store().head(key)
            return True
        except FileNotFoundError:
            return False

    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        key = self._store_key()
        if key == "":
            return True
        result = self._list_result(self._directory_prefix())
        return bool(result["common_prefixes"] or result["objects"])

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError(str(self))

        result = self._list_result(self._directory_prefix())
        children = [
            self._path_from_store_path(prefix)
            for prefix in result["common_prefixes"]
        ]
        children.extend(
            self._path_from_store_path(obj["path"])
            for obj in result["objects"]
            if obj["path"] != self._store_key().rstrip("/")
        )
        children.sort()
        return iter(children)

    def glob(self, pattern: PathLike, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        pattern_text = os.fspath(pattern).replace("\\", "/")
        if "/" not in pattern_text and "**" not in pattern_text:
            matches = [
                child
                for child in self.iterdir()
                if self._match_basename(child.name, pattern_text, case_sensitive=case_sensitive)
            ]
            return iter(matches)

        matches = []
        for candidate in self._recursive_candidates():
            relative = candidate.relative_to(self).as_posix()
            if self._match_relative_path(relative, pattern_text, case_sensitive=case_sensitive):
                matches.append(candidate)
        matches.sort()
        return iter(matches)

    def rglob(self, pattern: PathLike, *, case_sensitive: Optional[bool] = None, recurse_symlinks: bool = False):
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        pattern_text = os.fspath(pattern).replace("\\", "/")
        matches = []
        for candidate in self._recursive_candidates():
            relative = candidate.relative_to(self).as_posix()
            if "/" not in pattern_text and "**" not in pattern_text:
                if self._match_basename(candidate.name, pattern_text, case_sensitive=case_sensitive):
                    matches.append(candidate)
                continue
            if self._match_relative_path(relative, pattern_text, case_sensitive=case_sensitive):
                matches.append(candidate)
        matches.sort()
        return iter(matches)

    def walk(self, top_down: bool = True, on_error: Any = None, follow_symlinks: bool = False):
        if not self.is_dir():
            raise NotADirectoryError(str(self))

        tree = self._walk_tree()

        def emit(relative_dir: str):
            node = tree.get(relative_dir, {"dirs": set(), "files": set()})
            dirnames = sorted(node["dirs"])
            filenames = sorted(node["files"])
            current_path = self if relative_dir == "" else self.joinpath(*relative_dir.split("/"))
            if top_down:
                yield (current_path, dirnames, filenames)
            for dirname in dirnames:
                child_relative = f"{relative_dir}/{dirname}" if relative_dir else dirname
                yield from emit(child_relative)
            if not top_down:
                yield (current_path, dirnames, filenames)

        return emit("")

    def copy(self, target: PathLike) -> Any:
        target_path = self._coerce_target_path(target, method="copy")
        if isinstance(target_path, Path):
            return self._copy_to_local_path(target_path, method="copy")

        if self._can_use_native_remote_transfer(target_path):
            self._store().copy(self._store_key(), target_path._store_key(), overwrite=True)
            return target_path

        target_path.write_bytes(self.read_bytes())
        return target_path

    def move(self, target: PathLike) -> Any:
        target_path = self._coerce_target_path(target, method="move")
        if isinstance(target_path, Path):
            copied = self._copy_to_local_path(target_path, method="move")
            self.unlink()
            return copied

        if self._can_use_native_remote_transfer(target_path):
            self._store().rename(self._store_key(), target_path._store_key(), overwrite=True)
            return target_path

        target_path.write_bytes(self.read_bytes())
        self.unlink()
        return target_path

    def rename(self, target: PathLike) -> Any:
        return self.move(target)

    def replace(self, target: PathLike) -> Any:
        return self.move(target)

    def unlink(self, missing_ok: bool = False) -> None:
        key = self._store_key()
        if key == "":
            raise IsADirectoryError(f"Remote root prefixes cannot be unlinked: {self}")
        if not missing_ok and not self.exists():
            raise FileNotFoundError(str(self))
        self._store().delete(key)

    def _from_pure(self, pure: PurePosixPath) -> "RemotePath":
        lexical = pure.as_posix().lstrip("/")
        uri = f"{self.anchor}{lexical}" if lexical else self.anchor
        return type(self)(uri, client=self.client)

    def _bucket_uri(self) -> str:
        return f"{self.anchor}{self.drive}"

    def _root_scope(self) -> Optional[RemoteScope]:
        if self.client.root is None:
            return None
        return self.client.resolve_scope()

    def _store(self) -> Any:
        root_scope = self._root_scope()
        if root_scope is not None:
            return self.client.create_store()
        return self.client.create_store(uri=self._bucket_uri())

    def _store_key(self) -> str:
        root_scope = self._root_scope()
        if root_scope is None:
            return self._key

        root_prefix = root_scope.prefix.strip("/")
        if not root_prefix:
            return self._key
        if self._key == root_prefix:
            return ""
        prefix_with_sep = root_prefix.rstrip("/") + "/"
        if self._key.startswith(prefix_with_sep):
            return self._key[len(prefix_with_sep) :]
        raise InvalidConfigurationException(
            f"Remote path {self} is outside the configured root {self.client.root}."
        )

    def _directory_prefix(self) -> str:
        key = self._store_key().strip("/")
        if key == "":
            return ""
        return key.rstrip("/") + "/"

    def _list_result(self, prefix: str) -> dict[str, Any]:
        result = self._store().list_with_delimiter(prefix=prefix or None)
        return {
            "common_prefixes": list(result.get("common_prefixes", [])),
            "objects": list(result.get("objects", [])),
        }

    def _list_objects_recursive(self, prefix: str) -> list[dict[str, Any]]:
        stream = self._store().list(prefix or None)
        objects: list[dict[str, Any]] = []
        for chunk in stream:
            objects.extend(list(chunk))
        return objects

    def _path_from_store_path(self, store_path: str) -> "RemotePath":
        normalized_store_path = store_path.strip("/")
        root_scope = self._root_scope()
        full_key = normalized_store_path
        if root_scope is not None and root_scope.prefix.strip("/"):
            full_key = f"{root_scope.prefix.strip('/')}/{normalized_store_path}" if normalized_store_path else root_scope.prefix.strip("/")
        uri = f"{self.anchor}{self.drive}"
        if full_key:
            uri = f"{uri}/{full_key}"
        return type(self)(uri, client=self.client)

    def _recursive_candidates(self) -> list["RemotePath"]:
        prefix = self._directory_prefix()
        objects = self._list_objects_recursive(prefix)
        directory_paths: set[str] = set()
        file_paths: set[str] = set()

        for obj in objects:
            store_path = obj["path"].strip("/")
            file_paths.add(store_path)
            relative = store_path[len(prefix) :] if prefix and store_path.startswith(prefix) else store_path
            parent_parts = relative.split("/")[:-1]
            current_parts: list[str] = []
            for part in parent_parts:
                current_parts.append(part)
                directory_paths.add(f"{prefix}{'/'.join(current_parts)}".strip("/"))

        candidates = [self._path_from_store_path(path) for path in sorted(directory_paths | file_paths)]
        candidates.sort()
        return candidates

    def _match_basename(self, name: str, pattern: str, *, case_sensitive: Optional[bool]) -> bool:
        if case_sensitive is False:
            return fnmatch(name.lower(), pattern.lower())
        return fnmatch(name, pattern)

    def _match_relative_path(self, relative_path: str, pattern: str, *, case_sensitive: Optional[bool]) -> bool:
        candidate = PurePosixPath(relative_path)
        if case_sensitive is False:
            candidate = PurePosixPath(relative_path.lower())
            pattern = pattern.lower()
        return candidate.match(pattern)

    def _walk_tree(self) -> dict[str, dict[str, set[str]]]:
        prefix = self._directory_prefix()
        tree: dict[str, dict[str, set[str]]] = {
            "": {"dirs": set(), "files": set()},
        }
        for obj in self._list_objects_recursive(prefix):
            store_path = obj["path"].strip("/")
            relative = store_path[len(prefix) :] if prefix and store_path.startswith(prefix) else store_path
            parts = [part for part in relative.split("/") if part]
            if not parts:
                continue

            current = ""
            for dirname in parts[:-1]:
                tree.setdefault(current, {"dirs": set(), "files": set()})
                tree[current]["dirs"].add(dirname)
                current = f"{current}/{dirname}" if current else dirname
                tree.setdefault(current, {"dirs": set(), "files": set()})

            tree.setdefault(current, {"dirs": set(), "files": set()})
            tree[current]["files"].add(parts[-1])

        return tree

    def _coerce_target_path(self, target: PathLike, *, method: str) -> Path | "RemotePath":
        if isinstance(target, RemotePath):
            return target

        target_text = os.fspath(target)
        if not self._is_remote_target_text(target_text):
            return Path(target_text)

        target_storage_type = self._storage_type_for_scheme(urlparse(target_text).scheme.lower())
        if target_storage_type != self.client.storage_type:
            self._not_implemented(
                method,
                "support cross-scheme remote transfers explicitly before accepting targets "
                "outside the source storage type",
            )

        target_client = RemoteProfileClient(
            profile_name=self.client.profile_name,
            storage_type=self.client.storage_type,
            root=None,
            client_kwargs=dict(self.client.client_kwargs),
        )
        return type(self)(target_text, client=target_client)

    def _is_remote_target_text(self, target: str) -> bool:
        parsed = urlparse(target)
        return bool(parsed.scheme and parsed.netloc)

    def _copy_to_local_path(self, target: Path, *, method: str) -> Path:
        if self.client.profile_name == "PUBLIC":
            self._not_implemented(
                method,
                "support remote-to-local transfers for profileless public URIs explicitly "
                "before accepting local targets",
            )
        if self.is_dir():
            self._not_implemented(
                method,
                "support recursive remote directory transfers explicitly before accepting "
                "directory-like prefixes as copy sources",
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.read_bytes())
        return target

    def _can_use_native_remote_transfer(self, target: "RemotePath") -> bool:
        return (
            self.client.profile_name == target.client.profile_name
            and self.client.storage_type == target.client.storage_type
            and self.client.root == target.client.root
            and self.client.client_kwargs == target.client.client_kwargs
            and self.anchor == target.anchor
            and self.drive == target.drive
        )

    def _storage_type_for_scheme(self, scheme: str) -> str:
        if scheme == "s3":
            return "s3"
        if scheme == "gs":
            return "gs"
        if scheme == "az":
            return "azure"
        raise InvalidConfigurationException(f"Unsupported remote URI scheme for target: {scheme}://")

    def _not_implemented(self, method: str, required_implementation: str) -> NoReturn:
        raise NotImplementedError(
            f"PPath.{method}() is not implemented for remote mode. Path={self}. "
            f"Profile={self.client.profile_name}. Storage type={self.client.storage_type}. "
            f"Required implementation: {required_implementation}."
        )
