````markdown
# ContextQL Tooling Specification (v0.1)

Copyright (c) 2026 Anton du Plessis (github/adpatza)

Specification license: CC-BY-4.0
Implementation license: Apache 2.0

## Purpose

This document defines the minimum tooling surface required to make ContextQL usable as a real language rather than just a whitepaper artifact.

The tooling stack has five parts:

- grammar
- parser
- linter
- language server
- CLI / test harness

---

## 1. Tooling Layers

```text
Source Text
   ↓
Lexer / Grammar
   ↓
Parser
   ↓
Semantic Analyzer / Linter
   ↓
Language Server + CLI + Tests
```

---

## 2. Deliverables

### 2.1 Grammar

Canonical grammar file:

```text
grammar/contextql.lark
```

Responsibilities:
- tokenization
- parse tree generation
- source position tracking

### 2.2 Parser

Canonical parser module:

```text
contextql/parser.py
```

Responsibilities:
- parse text to CST / AST entry point
- normalize syntax errors
- expose stable API for tools

### 2.3 Linter

Canonical linter module:

```text
contextql/linter.py
```

Responsibilities:
- semantic validation
- diagnostics
- editor-friendly rule IDs

### 2.4 Language Server

Canonical future module:

```text
contextql/lsp_server.py
```

Responsibilities:
- completions
- hover
- diagnostics
- go-to-definition

### 2.5 CLI

Canonical future module:

```text
contextql/cli.py
```

Responsibilities:
- validate
- parse
- lint
- test

---

## 3. Lint Rules

Error codes follow the whitepaper Section 35 scheme:
- `E001–E099` — syntax errors (parser)
- `E100–E199` — semantic errors (linter)
- `W001–W499` — warnings

| Rule ID | Severity | Purpose |
|---------|----------|---------|
| E001    | error    | syntax error (parse failure) |
| E100    | error    | undefined context reference |
| E102    | error    | entity key type mismatch between context and table |
| E103    | error    | circular context dependency |
| E107    | error    | ORDER BY CONTEXT without WHERE CONTEXT IN |
| E108    | error    | CONTEXT_SCORE() / CONTEXT_COUNT() outside context query |
| E109    | error    | temporal qualifier on non-temporal context |
| E110    | error    | negative context weight |
| E118    | error    | ORDER BY in context definition body |
| W001    | warning  | CONTEXT WINDOW without scored contexts |
| W002    | warning  | joined query missing explicit CONTEXT ON binding |
| W004    | warning  | context weight is zero (no-op) |

---

## 4. Stability Contract

The parser and linter are tooling-facing APIs.

They must remain stable even if the execution engine evolves.

This means:
- stable diagnostic codes
- stable AST/CST entrypoint contract
- stable catalog lookup interface

---

## 5. v1 Principle

```text
Tooling should be stricter than execution where helpful,
but never invent semantics.
```

The linter may warn early.
Only the engine defines runtime truth.
````
