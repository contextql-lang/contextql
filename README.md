# ContextQL

**ContextQL** is a SQL-first query language that introduces **contexts as first-class query primitives** for operational intelligence.

A context is a named, reusable operational situation — a risk condition, process anomaly, compliance violation, or any business-meaningful condition that needs to be queried, composed, ranked, and governed.

```sql
SELECT invoice_id, vendor_name, amount, CONTEXT_SCORE() AS risk_score
FROM invoices i
JOIN vendors v ON i.vendor_id = v.vendor_id
WHERE CONTEXT ON i IN (late_invoice, high_value WEIGHT 0.8)
ORDER BY CONTEXT DESC
LIMIT 20;
```

## Why not SQL views?

SQL can emulate parts of this. But ContextQL provides what views, CTEs, and materialized views cannot:

| Capability | SQL Views | ContextQL Contexts |
|---|---|---|
| Named, reusable logic | Yes | Yes |
| Composable (union, intersect, sequence) | Manual | First-class algebra |
| Rankable with scores | No | `ORDER BY CONTEXT`, `CONTEXT_SCORE()` |
| Weighted combination | No | `WEIGHT` clause |
| Governed lifecycle | No | Draft, validated, materialized, retired |
| Temporal versioning | No | `AT`, `BETWEEN` qualifiers |
| Cross-system federation | No | `MCP(...)`, `REMOTE(...)` providers |
| Process-aware | No | `THROUGHPUT_TIME_BETWEEN`, `CONFORMS_TO` |

The difference is not "can SQL express it?" but "can operational situations become first-class managed objects?"

## Implementation Status

| Component | Status | Location |
|---|---|---|
| Language grammar | Implemented | `grammar/contextql.lark` |
| Parser | Implemented | `contextql/parser.py` |
| Semantic linter (11 rules) | Implemented | `contextql/linter.py` |
| Rich diagnostics | Implemented | `contextql/diagnostics.py` |
| Error code registry | Implemented | `contextql/errors.py` |
| Type system definitions | Implemented | `contextql/types.py` |
| Language Server (LSP) | Implemented | `contextql/lsp/server.py` |
| VS Code extension | Implemented | `vscode-contextql/` |
| Execution engine | Specified | See WHITEPAPER.md Sections 15-18 |
| Context Ops lifecycle | Specified | See WHITEPAPER.md Sections 19-21 |
| Process intelligence functions | Specified | See WHITEPAPER.md Sections 13-14 |
| Federation (MCP/REMOTE) | Designed | See WHITEPAPER.md Sections 22-23 |
| Security and governance | Designed | See WHITEPAPER.md Sections 24-30 |

## Quick Start

```bash
# Install
pip install -e .

# Parse a query
python -c "
from contextql.parser import ContextQLParser
p = ContextQLParser()
tree = p.parse('SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);')
print(tree.pretty())
"

# Run the linter
python examples/lint_demo.py

# Install with LSP support
pip install -e ".[lsp]"
contextql-lsp  # starts the language server (stdio)
```

## Lint Rules

ContextQL includes a semantic linter that catches errors at authoring time:

| Code | Severity | Description |
|------|----------|-------------|
| E001 | error | Syntax error |
| E100 | error | Undefined context |
| E102 | error | Entity key type mismatch |
| E103 | error | Circular context dependency |
| E107 | error | `ORDER BY CONTEXT` without `WHERE CONTEXT IN` |
| E108 | error | `CONTEXT_SCORE()` outside context query |
| E109 | error | Temporal qualifier on non-temporal context |
| E110 | error | Negative weight |
| E118 | error | `ORDER BY` in context definition |
| W001 | warning | `CONTEXT WINDOW` without scored contexts |
| W002 | warning | Joined query missing `CONTEXT ON` |
| W004 | warning | Weight of zero |

## Resources

- **Language Specification**: [`SPEC.md`](SPEC.md)
- **Design Whitepaper**: [`WHITEPAPER.md`](WHITEPAPER.md)
- **Design Decisions**: [`DECISIONS.md`](DECISIONS.md)

## License

Specification: CC-BY-4.0
Implementation: Apache 2.0

Copyright (c) 2026 Anton du Plessis
