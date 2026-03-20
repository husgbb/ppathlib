# AGENTS.md

Follow `docs/` numbered files in filename order:

1. [0_PHILOSOPHY.md](/Users/hus/pkm/git/ppathlib/docs/0_PHILOSOPHY.md)
2. [1_STANDARD.md](/Users/hus/pkm/git/ppathlib/docs/1_STANDARD.md)
3. [2_DECISIONS.md](/Users/hus/pkm/git/ppathlib/docs/2_DECISIONS.md)
4. [3_CURRENT.md](/Users/hus/pkm/git/ppathlib/docs/3_CURRENT.md)

Rules:

- `docs/` is organized into governance layers.
- `0_PHILOSOPHY.md` stores implementation philosophy only.
- `0_PHILOSOPHY.md` is human-only. Do not edit it unless a human explicitly asks.
- `1_STANDARD.md` stores success criteria for tests and validation.
- `1_STANDARD.md` requires explicit human approval before any AI edit.
- `2_DECISIONS.md` stores user-decided choices that AI must not override.
- Do not introduce behavior that conflicts with `2_DECISIONS.md`.
- If implementation requires a new architectural choice, ask the human instead of deciding in code or tests.
- `3_CURRENT.md` stores temporary current-stage instructions and status notes.
- Treat `3_CURRENT.md` as a temporary instruction layer with the lowest priority among the numbered docs.
