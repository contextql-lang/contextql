````markdown
# ContextQL Language Server Specification (v0.1)

Copyright (c) 2026 Anton du Plessis (github/adpatza)

Specification license: CC-BY-4.0
Implementation license: Apache 2.0

## Purpose

The ContextQL Language Server is the developer-intelligence layer for the language.

It provides:
- parse-time diagnostics
- semantic validation
- autocomplete
- hover docs
- go-to-definition

It is not the execution engine.

---

## 1. Architectural Position

```text
Editor
  ↓
LSP Server
  ↓
Parser + Linter + Catalog Adapter
```

The language server depends on:
- grammar/contextql.lark
- contextql/parser.py
- contextql/linter.py
- metadata/catalog access

---

## 2. Supported LSP Features

| Feature | v1 |
|--------|----|
| diagnostics | YES |
| completion | YES |
| hover | YES |
| go-to-definition | YES |
| document symbols | YES |
| rename | NO |
| code actions | Limited |

---

## 3. Metadata Sources

### Live mode
- connected database / adapter
- current contexts
- event logs
- process models

### Offline mode
- cached metadata snapshot
- file-based specs

---

## 4. Completion Sources

### Keyword completion
Examples:
- CREATE CONTEXT
- CREATE EVENT LOG
- CONTEXT IN
- ORDER BY CONTEXT

### Catalog completion
Examples:
- context names
- table names
- provider names
- event log names

### Function completion
Examples:
- THROUGHPUT_TIME
- PATH_CONTAINS
- CONTEXT_SCORE

---

## 5. Hover Examples

### Context hover
Shows:
- name
- entity key
- score presence
- description
- tags

### Function hover
Shows:
- signature
- return type
- short explanation

---

## 6. Go-To-Definition

Supported targets:
- CREATE CONTEXT
- CREATE EVENT LOG
- CREATE PROCESS MODEL

---

## 7. Diagnostics Contract

Diagnostics are emitted as:

```text
(rule_id, severity, message, range, suggestion?)
```

Severity values:
- error
- warning
- info

---

## 8. Performance Targets

| Operation | Target |
|----------|--------|
| parse | < 5 ms |
| lint | < 15 ms |
| completion | < 20 ms |

---

## 9. v1 Implementation Stack

Recommended stack:
- Python
- pygls
- Lark
- shared parser/linter modules

---

## 10. Principle

```text
Language Server = Analyzer as a Service
```

Not:
- scheduler
- query executor
- storage system
````
