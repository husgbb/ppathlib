# STANDARD

This document defines success criteria that must hold across the development and validation lifecycle.

AI agents must not modify this file unless a human explicitly approves that change.

This document is intended to guide test design and contract validation.
If code and tests disagree, this document is the reference for what success means.

## Scope

The current contract defines four observable surfaces:

1. `PPath` behavior in local mode
2. `PPath` behavior in remote lexical mode
3. `PPath` behavior in remote runtime mode
4. TOML-based profile configuration behavior

For this document, `lexical` means path behavior that is derived only from the path string and profile metadata.
Lexical behavior must not require backend network access, backend object metadata, or backend existence checks.

For this document, `runtime` means backend-backed behavior that may perform I/O after lexical resolution is complete.

## Success Criteria

### 1. Local Mode

`PPath(path)` succeeds when:

- it returns a `PPath` object
- that object uses local mode internally
- that object behaves like Python `pathlib` local path handling for local paths
- it does not require remote configuration

Local transfer helpers succeed when:

- `copy()` and `move()` work on supported Python versions
- local operations do not depend on stdlib methods that are unavailable in the supported version range

### 2. Remote Mode Entry

`PPath(path, profile=...)` succeeds when:

- it returns a `PPath` object
- that object uses remote mode internally
- it accepts relative remote paths when the selected profile defines `root`
- it accepts explicit remote URIs with a profile
- it accepts explicit remote S3 URIs without a profile when the URI is sufficient to identify a public remote resource
- it does not require a profile for profile-less public S3 access
- it rejects relative remote paths when the selected profile does not define `root`
- it rejects URI schemes that contradict the selected profile's `storage_type`
- it rejects invalid profile roots early and explicitly

### 3. Lexical Remote Behavior

`PPath` in remote mode succeeds when these operations are deterministic and require no backend I/O:

- `__str__`
- `__repr__`
- `__truediv__`
- `joinpath`
- `anchor`
- `drive`
- `name`
- `stem`
- `suffix`
- `suffixes`
- `parent`
- `parents`
- `parts`
- `with_name`
- `with_stem`
- `with_suffix`
- `relative_to`
- `is_relative_to`
- `match`
- `full_match`
- `as_uri`
- `absolute`
- `is_absolute`
- `resolve`

These operations succeed only when they depend on:

- the original path input
- normalized URI components
- selected profile metadata

These operations fail this standard if they depend on:

- backend connectivity
- backend listing results
- backend object metadata
- backend existence checks

These operations also fail this standard if they silently cross bucket/container boundaries or generate invalid root URIs.

### 4. Remote Runtime Behavior

Remote runtime behavior succeeds when these methods exist on remote paths and perform backend I/O:

- `open`
- `read_text`
- `write_text`
- `read_bytes`
- `write_bytes`
- `exists`
- `is_file`
- `is_dir`
- `iterdir`
- `glob`
- `rglob`
- `walk`
- `copy`
- `move`
- `rename`
- `replace`
- `unlink`

Each runtime method succeeds when:

- it emits `ExperimentalRemoteRuntimeWarning`
- it performs backend work only after remote lexical resolution is complete
- it works with profiled remote paths
- it keeps unsupported behavior explicit instead of silently pretending success

Unsupported runtime slices succeed when:

- they raise `NotImplementedError`
- the message starts with:
  - `PPath.<method>() is not implemented for remote mode.`
- the message includes:
  - `Path=<uri>`
  - `Profile=<profile>`
  - `Storage type=<storage_type>`
  - `Required implementation: ...`

Examples of unsupported runtime slices in the current contract:

- `open()` append, exclusive-create, and read/write mixed modes
- recursive remote directory transfer
- profile-less public remote-to-local transfer
- cross-scheme remote transfer

### 5. Configuration Discovery

Configuration discovery succeeds when resolution follows this order:

1. `.ppathlib.toml` in the current working directory
2. `.ppathlib.toml` in the nearest parent directory that contains it
3. `~/.config/ppathlib/.ppathlib.toml` as the final fallback

This behavior succeeds only if:

- the nearest project file wins
- the global file is used only when no project file is found
- project and global files are not merged in the first implementation

### 6. Configuration Format

Configuration succeeds when:

- TOML is the only supported configuration format
- named profiles live under a profile table
- profile lookup is case-insensitive from the caller perspective
- `storage_type` is required
- `root` is optional but required for relative remote paths
- `root`, when present, is an explicit remote URI whose scheme matches `storage_type`

Supported storage types for this contract:

- `s3`
- `gs`
- `azure`

### 7. Environment Variable Boundary

This contract succeeds when:

- `.env` loading is not part of the configuration system
- environment variables are optional, not required
- environment variables may be recognized only when they help locate or select TOML-based configuration
- environment variables must not define profile fields such as `storage_type`, `root`, credentials, or backend options
- mixed field-by-field resolution across environment variables and TOML files is not allowed

### 8. Backend Boundary

This contract succeeds when:

- lexical tests remain network-free
- runtime tests may exercise real backend I/O when explicitly marked as live tests
- documentation may state that the first runtime layer uses `obstore`
- public runtime contracts do not depend on backend-specific type names
- public S3 runtime access may use anonymous requests without requiring a profile

## Testing Guidance

Tests should be written to verify:

1. exact mode selection
2. exact lexical behavior
3. exact runtime warning behavior
4. exact configuration resolution rules
5. exact unsupported-runtime failure messages
6. live runtime behavior only in isolated prefixes or public lightweight objects

Tests should not:

- assume hidden fallback behavior
- accept vague error text
- treat shared live prefixes as deterministic
- depend on live backend connectivity when validating lexical behavior
