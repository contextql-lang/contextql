# ContextQL Python Package

## Module Overview

| Module | Purpose |
|--------|---------|
| `parser.py` | Lark-based parser wrapping `grammar/contextql.lark`. Earley parser with position tracking and friendly error messages. |
| `linter.py` | Semantic analyzer with 11 lint rules (E100-E118, W001-W004). Catalog-aware: understands context definitions, entity key types, dependencies. |
| `diagnostics.py` | Rich Rust/Elm-style diagnostic renderer with source annotations, underlines, and suggestions. |
| `errors.py` | Error code registry. E001-E099 syntax, E100-E199 semantic, W001-W499 warnings. |
| `types.py` | Type lattice and entity key type definitions for semantic checking. |
| `lsp/server.py` | Language Server Protocol server (pygls v2). Real-time diagnostics, completions, hover, document symbols. |

## API Stability

The parser and linter expose stable APIs:

- `ContextQLParser.parse(text) -> lark.Tree`
- `ContextQLLinter.lint(text) -> list[LintDiagnostic]`
- `Catalog` with `add_context()`, `add_table()`, `add_event_log()`

These are tooling-facing contracts. They must remain stable even as the execution engine evolves.

## Grammar

The canonical grammar is `grammar/contextql.lark` (Lark/Earley). It supports 27 statement types including SELECT, context DDL, event log DDL, process model DDL, provider registration, security, and administration.

## Lint Rules

| Code | Severity | Description |
|------|----------|-------------|
| E001 | error | Syntax error |
| E100 | error | Undefined context |
| E102 | error | Entity key type mismatch |
| E103 | error | Circular context dependency |
| E107 | error | ORDER BY CONTEXT without WHERE CONTEXT IN |
| E108 | error | CONTEXT_SCORE() outside context query |
| E109 | error | Temporal qualifier on non-temporal context |
| E110 | error | Negative weight |
| E118 | error | ORDER BY in context definition |
| W001 | warning | CONTEXT WINDOW without scored contexts |
| W002 | warning | Joined query missing CONTEXT ON |
| W004 | warning | Weight of zero |
