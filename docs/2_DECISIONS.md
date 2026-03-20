# DECISIONS

This document records decisions explicitly made by the user for this project.

AI agents must not introduce conflicting decisions without explicit human approval.
If implementation needs a new architectural choice, that choice must be added here only after a human decides it.

## Active Decisions

### D-001: Configuration Format

- Decision: configuration uses TOML

### D-002: User-Level Fallback Path

- Decision: the default user-level configuration file path is `~/.config/ppathlib/.ppathlib.toml`

### D-003: Project Configuration Discovery

- Decision: configuration discovery starts from the current working directory and walks upward through parent directories looking for `.ppathlib.toml`

### D-004: Discovery Priority

- Decision: project-local configuration has priority over the user-level fallback file

### D-005: Merge Policy
- Decision: the first implementation uses fallback, not merge
- Consequence: project config does not merge with global config

### D-006: Environment Variables
- Decision: environment-variable configuration is removed entirely
- Consequence: there is no fallback layer, override layer, or mixed mode using environment variables

### D-007: Harness Engineering Methodology
- Decision: harness engineering is the project's context-engineering methodology, not a temporary development stage
- Consequence: abstract interfaces, placeholder errors, and contract-first tests remain valid tools throughout the development lifecycle
- Consequence: concrete runtime wiring may be added when explicitly requested, but it must remain subordinate to documented contracts and decisions

### D-008: Backend Mention Policy
- Decision: documentation may mention that the first backend runtime is expected to use `obstore`
- Consequence: backend usage may be documented
- Consequence: backend-specific naming should not define the public path model

### D-009: Directory-Like Prefix Semantics
- Decision: `s3://bucket/a/` is treated as a directory-like prefix
