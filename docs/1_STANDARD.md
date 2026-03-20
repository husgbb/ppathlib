# STANDARD

This document defines success criteria that must hold across the development and validation lifecycle.

AI agents must not modify this file unless a human explicitly approves that change.

This document is intended to guide test design and contract validation.
If code and tests disagree, this document is the reference for what success means.

## Scope

The current contract defines three observable surfaces:

1. `PPath` behavior in local mode
2. `PPath` behavior in remote lexical mode
3. TOML-based profile configuration behavior

For this document, `lexical` means path behavior that is derived only from the path string and profile metadata.
Lexical behavior must not require backend network access, backend object metadata, or backend existence checks.

## Success Criteria

### 1. Local Mode

`PPath(path)` succeeds when:

- it returns a `PPath` object
- that object uses local mode internally
- that object behaves like Python `pathlib` local path handling for local paths
- it does not require remote configuration

### 2. Remote Mode Entry

`PPath(path, profile=...)` succeeds when:

- it returns a `PPath` object
- that object uses remote mode internally
- it accepts relative remote paths when the selected profile defines `root`
- it accepts explicit remote URIs with a profile
- it accepts explicit remote URIs without a profile when the URI is sufficient to identify a public remote resource
- it does not require a profile for profile-less public remote access
- it rejects relative remote paths when the selected profile does not define `root`
- it rejects URI schemes that contradict the selected profile's `storage_type`

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

### 4. Placeholder Remote Behavior

Placeholder remote behavior succeeds when deferred runtime methods exist immediately and fail explicitly.

Required placeholder methods:

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

Each placeholder succeeds when:

- it raises `NotImplementedError`
- the message starts with:
  - `PPath.<method>() is not implemented for remote mode.`
- the message includes:
  - `Path=<uri>`
  - `Profile=<profile>`
  - `Storage type=<storage_type>`
  - `Required implementation: ...`

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

- backend runtime behavior is not yet required for lexical tests
- backend construction remains abstract
- documentation may state that the first runtime layer is expected to use `obstore`
- public runtime contracts do not depend on backend-specific type names

## Testing Guidance

Tests should be written to verify:

1. exact mode selection
2. exact lexical behavior
3. exact configuration resolution rules
4. exact placeholder failure messages

Tests should not:

- assume hidden fallback behavior
- accept vague error text
- depend on live backend connectivity when validating lexical behavior or placeholder contracts
