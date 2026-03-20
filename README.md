# ppathlib

`ppathlib` provides a `pathlib`-style public path object for local paths and named remote storage.

The current library exposes one public path type:

- `PPath(path)` uses local mode internally
- `PPath(path, profile=...)` uses remote mode internally
- `PPath("s3://...")` can also enter public remote mode without a profile for lightweight public S3 access

## Current Status

The library currently supports:

- local mode with `pathlib`-style composition
- remote lexical behavior such as `/`, `joinpath`, `name`, `stem`, `suffix`, `parent`, `parts`, `relative_to`, and `with_suffix`
- backend-backed remote runtime operations such as `open`, `read_*`, `write_*`, `exists`, listing, globbing, copy, move, and unlink
- project-scoped TOML configuration discovery
- profile-less public S3 access for lightweight public objects

Remote runtime behavior is still experimental.
Remote runtime methods emit `ExperimentalRemoteRuntimeWarning` so callers do not mistake the current behavior for a frozen contract.

The source of truth for project governance is the numbered documentation set in `docs/`.

## Installation

```bash
pip install ppathlib
```

Minimum supported Python version: `3.11`.

## Quick Start

### Local Mode

```python
from ppathlib import PPath

path = PPath("data/local-report.parquet")
print(path.mode)
print(path.parent)
```

### Profiled Remote Mode

```toml
version = 1

[profiles.analytics]
storage_type = "s3"
endpoint_url = "https://storage.example.com"
access_key_id = "xxx"
secret_access_key = "yyy"
root = "s3://analytics-bucket"
```

```python
from ppathlib import PPath

path = PPath("daily/report.parquet", profile="analytics")
print(path)
print(path.name)
print(path.with_suffix(".csv"))
```

### Public S3 Mode

```python
from ppathlib import PPath

path = PPath("s3://wikisum/README.txt")
print(path.read_text(encoding="utf-8")[:120])
```

## Current API Surface

### `PPath(path, profile=None)`

Returns the public `PPath` object in either local or remote mode.

### `get_client(profile)`

Returns the cached profile client for a named remote.

### `clear_client_cache()`

Clears the internal profile-client registry.

## Documentation

- [Philosophy](docs/0_PHILOSOPHY.md)
- [Standard](docs/1_STANDARD.md)
- [Decisions](docs/2_DECISIONS.md)
- [Current Notes](docs/3_CURRENT.md)

## License

MIT
