# Configuration

## Overview

`ppathlib` uses the `profile` argument as the environment-variable prefix.

If the caller uses:

```python
PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")
```

then `ppathlib` reads variables such as:

```bash
MY_RESEARCH_BUCKET_STORAGE_TYPE=...
MY_RESEARCH_BUCKET_ROOT=...
```

## Local and Remote Modes

### Local Mode

If `profile` is omitted, `PPath(...)` behaves like `pathlib.Path(...)`.

```python
from ppathlib import PPath

path = PPath("data/report.parquet")
tmp = PPath("/tmp/report.parquet")
```

### Remote Mode

If `profile` is provided:

- the profile names a configured remote alias
- relative paths are resolved against `<PROFILE>_ROOT`
- absolute remote URIs are accepted directly
- remote URIs without `profile` are rejected

Examples:

```python
from ppathlib import PPath

rel = PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")
full = PPath("s3://analytics-bucket/daily/report.parquet", profile="MY_RESEARCH_BUCKET")
```

## Common Variables

Required:

```bash
<PROFILE>_STORAGE_TYPE=s3
```

Supported values:

- `s3`
- `gcs`
- `azure`
- `sftp`
- `webdav`
- other supported remote types

Optional:

```bash
<PROFILE>_ROOT=s3://bucket-name
```

`ROOT` is used only when the caller passes a relative path with a profile.

## Backend-Specific Variables

After `STORAGE_TYPE` is resolved, `ppathlib` reads the backend-specific variables for that profile.

### S3

Required:

```bash
<PROFILE>_STORAGE_TYPE=s3
<PROFILE>_ENDPOINT_URL=https://storage.example.com
<PROFILE>_ACCESS_KEY_ID=your-access-key
<PROFILE>_SECRET_ACCESS_KEY=your-secret-key
```

Optional:

```bash
<PROFILE>_REGION=us-east-1
<PROFILE>_SESSION_TOKEN=...
<PROFILE>_ADDRESSING_STYLE=path
<PROFILE>_VERIFY=true
<PROFILE>_SSL=true
<PROFILE>_ROOT=s3://bucket-name
```

Resolved fields:

- `endpoint_url`
- `aws_access_key_id`
- `aws_secret_access_key`
- `aws_session_token`
- `region_name`
- `addressing_style`
- `verify`
- `use_ssl`

### GCS

Required:

```bash
<PROFILE>_STORAGE_TYPE=gcs
<PROFILE>_PROJECT=my-gcp-project
<PROFILE>_CREDENTIALS_JSON=/path/to/service-account.json
```

Optional:

```bash
<PROFILE>_STORAGE_CLIENT_JSON=...
<PROFILE>_ROOT=gs://bucket-name
```

Resolved fields:

- `project`
- `credentials_json`
- `storage_client_json`

### Azure

Required:

```bash
<PROFILE>_STORAGE_TYPE=azure
<PROFILE>_ACCOUNT_URL=https://<account>.blob.core.windows.net
```

One of:

```bash
<PROFILE>_ACCOUNT_KEY=...
```

or:

```bash
<PROFILE>_CONNECTION_STRING=...
```

Optional:

```bash
<PROFILE>_TENANT_ID=...
<PROFILE>_CLIENT_ID=...
<PROFILE>_CLIENT_SECRET=...
<PROFILE>_ROOT=az://container-name
```

Resolved fields:

- `account_url`
- `account_key`
- `connection_string`
- `tenant_id`
- `client_id`
- `client_secret`

### SFTP

Required:

```bash
<PROFILE>_STORAGE_TYPE=sftp
<PROFILE>_HOST=sftp.example.com
<PROFILE>_USERNAME=data-user
```

Optional:

```bash
<PROFILE>_PORT=22
<PROFILE>_PASSWORD=...
<PROFILE>_PRIVATE_KEY=/path/to/id_ed25519
<PROFILE>_KNOWN_HOSTS=/path/to/known_hosts
<PROFILE>_ROOT=sftp://sftp.example.com/export
```

Resolved fields:

- `host`
- `port`
- `username`
- `password`
- `private_key`
- `known_hosts`

## Profile Rules

- `profile` is optional
- omitting `profile` enables local mode
- profile lookup is case-insensitive from the caller perspective
- profile names are normalized before environment lookup
- uppercase-safe identifiers such as `MY_RESEARCH_BUCKET`, `GCS_RAW`, `AZURE_ARCHIVE`, and `SFTP_EXPORT` are recommended
- with a profile, relative paths require `<PROFILE>_ROOT`
- with a profile, absolute remote URIs can always be passed directly
- remote URIs without a profile raise an error

## Client Cache

`ppathlib` keeps an internal client registry.

Cache behavior:

- the same resolved connection configuration reuses the same client
- different profiles may share a client if they differ only by `ROOT`
- changing environment values creates a different configuration fingerprint
- `clear_client_cache()` resets the registry

Example:

```python
from ppathlib import PPath, get_client

client1 = get_client(profile="MY_RESEARCH_BUCKET")
client2 = get_client(profile="my_research_bucket")
path = PPath("daily/report.parquet", profile="MY_RESEARCH_BUCKET")

assert client1 is client2
assert path.client is client1
```

## Error Handling

Typical failure cases:

- missing required environment variables
- relative path without configured `ROOT`
- unsupported path scheme
- remote URI without `profile`
- invalid boolean values
- invalid endpoint configuration
- unsupported addressing style values

Example messages:

```text
Missing required ppathlib environment variable: MY_RESEARCH_BUCKET_SECRET_ACCESS_KEY
```

```text
Relative path requires MY_RESEARCH_BUCKET_ROOT to be set
```

```text
Remote paths require a profile: s3://analytics-bucket/daily/report.parquet
```

## Boolean Parsing

Accepted values for boolean variables such as `VERIFY` and `SSL`:

- `true`
- `false`
- `1`
- `0`
- `yes`
- `no`

Values are parsed case-insensitively. Any other value raises a configuration error.

## What ppathlib Avoids

`ppathlib` does not depend on process-global SDK variables such as:

```bash
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION
GOOGLE_APPLICATION_CREDENTIALS
AZURE_STORAGE_CONNECTION_STRING
```
