# ppathlib

`ppathlib` is currently in a remote-path prototype rewrite.

The current prototype has two modes:

- `PPath(path)` returns `pathlib.Path`
- `PPath(path, profile=...)` returns a `RemotePath` prototype object

## Prototype Status

The current remote-path implementation is not feature-complete.

Working now:

- profile resolution
- profile-bound binding request construction
- local mode
- remote lexical path behavior such as `/`, `joinpath`, `name`, `stem`, `suffix`, `parent`, `parts`, `relative_to`, `with_suffix`
- explicit placeholder errors for deferred remote I/O
- documentation governance through `docs/PHILOSOPHY.md`, `docs/STANDARD.md`, and `docs/DECISIONS.md`

Deferred:

- actual backend-backed remote reads and writes
- remote listing and globbing
- remote copy and move
- bucket or container root listing
- stdlib monkey-patch compatibility helpers

The source of truth for the harness stage is the three-layer documentation set in `docs/`.

## Installation

```bash
pip install ppathlib
```

When remote behavior starts being implemented, the first backend runtime is expected to use `obstore` as an optional dependency.

## Quick Start

### Local Mode

```python
from ppathlib import PPath

path = PPath("data/local-report.parquet")
assert path.read_text if hasattr(path, "read_text") else True
```

### Remote Mode

```toml
version = 1

[profiles.my_remote]
storage_type = "s3"
endpoint_url = "https://storage.example.com"
access_key_id = "xxx"
secret_access_key = "yyy"
root = "s3://analytics-bucket"
```

```python
from ppathlib import PPath

path = PPath("daily/report.parquet", profile="MY_REMOTE")
print(path)
print(path.name)
print(path.with_suffix(".csv"))
```

At the current prototype stage, remote I/O methods such as `open()` are placeholders and will raise explicit implementation errors.
The profile client can resolve remote scope and emit an abstract binding request, but it does not construct concrete runtime stores yet.
URIs ending with `/`, such as `s3://bucket/a/`, are treated as directory-like prefixes by contract.
Configuration is intended to come from `.ppathlib.toml` discovery in the project tree, with `~/.config/ppathlib/.ppathlib.toml` as the user-level fallback.

## Current API Surface

### `PPath(path, profile=None)`

- if `profile` is omitted, returns `pathlib.Path`
- if `profile` is provided, returns `RemotePath`

### `get_client(profile)`

Returns the cached prototype profile client for a named remote.

### `clear_client_cache()`

Clears the internal profile-client registry.

## Documentation

- [Philosophy](docs/PHILOSOPHY.md)
- [Standard](docs/STANDARD.md)
- [Decisions](docs/DECISIONS.md)

## License

MIT
