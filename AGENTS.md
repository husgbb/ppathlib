# AGENTS.md

## Project Context

- This repository is a downstream fork of `cloudpathlib`, currently bootstrapped from upstream `v0.23.0`.
- The long-term goal is to evolve the library from object-storage-first abstractions into a broader remote-path library with named profiles and local fallback.
- Product-facing behavior is documented in [README.md](/Users/hus/pkm/git/huspath/README.md), [configuration.md](/Users/hus/pkm/git/huspath/docs/configuration.md), and [backends.md](/Users/hus/pkm/git/huspath/docs/backends.md).

## Current Architecture Rules

- Keep the upstream package structure recognizable while the fork is still stabilizing.
- Prefer incremental refactors over large rewrites.
- Preserve working upstream behavior before introducing new profile-driven behavior.
- Treat `profile=None` as local mode.
- Treat `<PROFILE>_STORAGE_TYPE` and `<PROFILE>_ROOT` as the current configuration contract.

## Development Workflow

- Start from the smallest change that can be verified.
- Use `uv` for virtual environment and dependency management during development.
- Keep development dependencies in `pyproject.toml` under `[dependency-groups].dev`.
- When changing path resolution or client construction logic, add or update tests first when practical.
- Keep comments in code in English.
- Keep user-facing documentation aligned with behavior changes in the same change set.
- Do not introduce unrelated cleanup while touching forked upstream code.

## Testing Expectations

- Prefer targeted test runs while iterating.
- Use `uv sync` to prepare the development environment when needed.
- Use `uv run pytest tests/<target> -n 0` for focused debugging.
- Run broader suites only after scoped changes are stable.
- If optional cloud dependencies are missing, document what could not be executed.

## Fork Maintenance

- Record upstream source details in [UPSTREAM.md](/Users/hus/pkm/git/huspath/UPSTREAM.md).
- When modifying imported upstream code, preserve enough structure to make future diffs against upstream understandable.
- Avoid renaming modules or moving directories without a clear migration reason.
