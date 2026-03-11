# ppathlib

`ppathlib` is a path interface for local files and named remote storage.

It behaves like `pathlib.Path` when no `profile` is provided, and it switches to remote mode when a `profile` is given. A profile maps to environment variables that define a backend such as S3, GCS, or Azure Blob Storage, plus an optional default `ROOT`. SFTP and WebDAV support are planned `(coming soon)`.

The main examples in this README use S3 because that is the most familiar starting point for most users. The same model also works for other supported remotes.

## Why ppathlib

- Use one path API for both local and remote storage
- Keep remote access explicit with named profiles
- Avoid process-global provider credential variables
- Support relative paths against a configured remote `ROOT`
- Reuse the same profile across pandas and pyarrow workflows

## Installation

```bash
pip install ppathlib
```

For parquet workflows you will typically also want:

```bash
pip install pandas pyarrow
```

## Quick Start

### Local Mode

If `profile` is omitted, `PPath(...)` behaves like `pathlib.Path(...)`.

```python
from ppathlib import PPath

path = PPath("data/local-report.parquet")
```

### Remote Mode

Define a named remote with a stable profile name:

```bash
export MY_RESEARCH_BUCKET_STORAGE_TYPE=s3
export MY_RESEARCH_BUCKET_ENDPOINT_URL=https://s3.ap-northeast-2.amazonaws.com
export MY_RESEARCH_BUCKET_ACCESS_KEY_ID=xxx
export MY_RESEARCH_BUCKET_SECRET_ACCESS_KEY=yyy
export MY_RESEARCH_BUCKET_REGION=ap-northeast-2
export MY_RESEARCH_BUCKET_ROOT=s3://analytics-bucket
```

Use a relative path against that remote root:

```python
from ppathlib import PPath

path = PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")

with path.open("rb") as f:
    payload = f.read()
```

You can also pass a full remote URI:

```python
from ppathlib import PPath

path = PPath(
    "s3://analytics-bucket/daily/report.parquet",
    profile="MY_RESEARCH_BUCKET",
)
```

## Core Behavior

```python
from ppathlib import PPath
```

- `PPath("data/file.parquet")`
  - local mode
- `PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")`
  - remote mode with `<PROFILE>_ROOT`
- `PPath("s3://bucket/file.parquet", profile="MY_RESEARCH_BUCKET")`
  - remote mode with an explicit URI
- `PPath("s3://bucket/file.parquet")`
  - error, because remote URIs require a profile

## Example Usage

```python
import pandas as pd
from ppathlib import PPath

src = PPath("in.parquet", profile="MY_RESEARCH_BUCKET")
dst = PPath("out.parquet", profile="MY_RESEARCH_BUCKET")

df = pd.read_parquet(src)
df.to_parquet(dst)
```

## API

### `PPath(path, profile=None)`

Creates either:

- a local path when `profile` is omitted
- a remote path when `profile` is provided

### `get_client(profile)`

Returns the cached client for a named remote.

### `clear_client_cache()`

Clears the internal client registry.

## Documentation

- [Configuration](docs/configuration.md)
- [Backends and Examples](docs/backends.md)

## License

MIT
