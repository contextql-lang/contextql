````markdown
# ContextQL Tooling Specification (v0.1)

Copyright (c) 2026 Anton du Plessis (github/adpatza)

Specification license: CC-BY-4.0
Implementation license: Apache 2.0

## Purpose

This document defines the minimum tooling surface required to make ContextQL usable as a real language rather than just a whitepaper artifact.

The tooling stack has seven parts:

- grammar
- parser
- linter
- execution engine + Python SDK
- language server
- CLI
- Jupyter magic

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

Canonical module (implemented):

```text
contextql/lsp/server.py
```

Entry point: `contextql-lsp` (installed via `pip install -e ".[lsp]"`)

Implemented features:
- diagnostics (real-time via parser + linter)
- completions (keywords + catalog contexts)
- hover (keyword documentation)
- document symbols (CREATE CONTEXT / EVENT LOG / PROCESS MODEL)

Planned:
- go-to-definition
- catalog-aware completions from live metadata

### 2.5 CLI

Implemented. Entry point: `cql` (installed via `pip install -e ".[executor]"`).

```bash
cql                        # interactive REPL (bare engine)
cql demo                   # interactive REPL with pre-loaded demo data
cql demo --file query.cql  # run a .cql file against demo data
cql explain "SELECT ..."   # print the query plan
```

The REPL accepts multi-line queries terminated with `;`. Use `\d` to list tables and contexts, `\q` to quit. Output formats: `table` (default), `json`, `csv` via `--format`.

### 2.6 Python SDK

Implemented. Install: `pip install -e ".[executor]"`.

```python
import contextql as cql

# Engine
e = cql.demo()                          # pre-loaded demo data
e = cql.Engine()                        # blank engine
e.register_table("t", df, primary_key="id")
e.register_context("ctx", "SELECT id FROM t WHERE ...", entity_key="id")

# Direct execution
result = e.execute("SELECT ... WHERE CONTEXT IN (ctx);")
result.show()                           # formatted print + row count
result.row_count                        # int
result.columns                          # List[str]
result.to_pandas()                      # pandas DataFrame
result.to_arrow()                       # pyarrow.Table (requires pyarrow extra)
result.to_polars()                      # polars DataFrame (requires polars extra)

# Fluent builder
result = (e.query("invoices")
           .select("invoice_id", "CONTEXT_SCORE() AS score")
           .where_context("overdue_invoice")
           .order_by_context()
           .limit(10)
           .execute())

# @context decorator
@e.context("high_value", entity_key="invoice_id")
def high_value():
    return "SELECT invoice_id FROM invoices WHERE amount > 10000"
```

### 2.7 Jupyter Magic

Implemented. Load with `%load_ext contextql`.

```python
%cql_setup demo          # create demo engine → _cql_engine
%cql_setup engine        # use existing 'engine' variable from namespace
%cql_contexts            # list registered tables and contexts

%%cql                    # execute cell; result stored in _cql_result
SELECT invoice_id FROM invoices WHERE CONTEXT IN (open_invoice) LIMIT 5;

%%cql my_result          # execute cell; result stored in my_result
SELECT ...
```

`%%cql` displays the result as a DataFrame inline and stores the `Result` object in the named variable.

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
