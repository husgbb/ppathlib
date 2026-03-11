# Backends and Examples

## S3

### Amazon S3

```bash
export MY_RESEARCH_BUCKET_STORAGE_TYPE=s3
export MY_RESEARCH_BUCKET_ENDPOINT_URL=https://s3.ap-northeast-2.amazonaws.com
export MY_RESEARCH_BUCKET_ACCESS_KEY_ID=xxx
export MY_RESEARCH_BUCKET_SECRET_ACCESS_KEY=yyy
export MY_RESEARCH_BUCKET_REGION=ap-northeast-2
export MY_RESEARCH_BUCKET_ROOT=s3://analytics-bucket
```

```python
from ppathlib import PPath

path = PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")
```

### Cloudflare R2

```bash
export R2_ARCHIVE_STORAGE_TYPE=s3
export R2_ARCHIVE_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
export R2_ARCHIVE_ACCESS_KEY_ID=xxx
export R2_ARCHIVE_SECRET_ACCESS_KEY=yyy
export R2_ARCHIVE_REGION=auto
export R2_ARCHIVE_ROOT=s3://archive-bucket
```

### Local MinIO

```bash
export MINIO_DEV_STORAGE_TYPE=s3
export MINIO_DEV_ENDPOINT_URL=http://127.0.0.1:9000
export MINIO_DEV_ACCESS_KEY_ID=minioadmin
export MINIO_DEV_SECRET_ACCESS_KEY=minioadmin
export MINIO_DEV_REGION=us-east-1
export MINIO_DEV_ADDRESSING_STYLE=path
export MINIO_DEV_SSL=false
export MINIO_DEV_ROOT=s3://dev-bucket
```

## GCS

```bash
export GCS_RAW_STORAGE_TYPE=gcs
export GCS_RAW_PROJECT=my-gcp-project
export GCS_RAW_CREDENTIALS_JSON=/secure/path/service-account.json
export GCS_RAW_ROOT=gs://raw-bucket
```

```python
from ppathlib import PPath

path = PPath("data/file.parquet", profile="GCS_RAW")
```

## Azure Blob Storage

```bash
export AZURE_ARCHIVE_STORAGE_TYPE=azure
export AZURE_ARCHIVE_ACCOUNT_URL=https://<account>.blob.core.windows.net
export AZURE_ARCHIVE_ACCOUNT_KEY=xxx
export AZURE_ARCHIVE_ROOT=az://archive-container
```

```python
from ppathlib import PPath

path = PPath("data/file.parquet", profile="AZURE_ARCHIVE")
```

## SFTP

```bash
export SFTP_EXPORT_STORAGE_TYPE=sftp
export SFTP_EXPORT_HOST=sftp.example.com
export SFTP_EXPORT_PORT=22
export SFTP_EXPORT_USERNAME=data-user
export SFTP_EXPORT_PRIVATE_KEY=/secure/path/id_ed25519
export SFTP_EXPORT_ROOT=sftp://sftp.example.com/export
```

```python
from ppathlib import PPath

path = PPath("data/file.parquet", profile="SFTP_EXPORT")
```

## Local and Remote I/O

### Local Mode

```python
from ppathlib import PPath

path = PPath("data/file.txt")

with path.open("w", encoding="utf-8") as f:
    f.write("hello")

with path.open("r", encoding="utf-8") as f:
    print(f.read())
```

### Remote Binary I/O

```python
from ppathlib import PPath

path = PPath("data/file.bin", profile="MINIO_DEV")

with path.open("wb") as f:
    f.write(b"hello")

with path.open("rb") as f:
    print(f.read())
```

### pandas

```python
import pandas as pd
from ppathlib import PPath

src = PPath("in.parquet", profile="MY_RESEARCH_BUCKET")
dst = PPath("out.parquet", profile="MY_RESEARCH_BUCKET")

df = pd.read_parquet(src)
df.to_parquet(dst)
```

### pyarrow

```python
import pyarrow.parquet as pq
from ppathlib import PPath

src = PPath("in.parquet", profile="MINIO_DEV")
dst = PPath("out.parquet", profile="MINIO_DEV")

table = pq.read_table(src)
pq.write_table(table, dst)
```

## Typical Workflow

```python
import pandas as pd
from ppathlib import PPath

src = PPath("daily/input.parquet", profile="GCS_RAW")
dst = PPath("daily/output.parquet", profile="AZURE_ARCHIVE")

df = pd.read_parquet(src)
df["processed_at"] = pd.Timestamp.utcnow()
df.to_parquet(dst)
```

The source and destination can use different named remotes in the same process.
