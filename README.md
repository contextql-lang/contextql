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
| Execution engine (DuckDB) | Implemented | `contextql/executor.py`, `contextql/__init__.py` |
| Python SDK (Engine, QueryBuilder) | Implemented | `contextql/__init__.py`, `contextql/_builder.py` |
| CLI (`cql`) | Implemented | `contextql/cli.py` |
| Jupyter magic (`%%cql`) | Implemented | `contextql/_magic.py` |
| Context Ops lifecycle | Specified | See WHITEPAPER.md Sections 19-21 |
| Process intelligence functions | Specified | See WHITEPAPER.md Sections 13-14 |
| Federation (MCP/REMOTE) | Designed | See WHITEPAPER.md Sections 22-23 |
| Security and governance | Designed | See WHITEPAPER.md Sections 24-30 |

## Repository Map

```
contextql/          Python package
  __init__.py       Public API: Engine, Result, CatalogProxy, demo()
  executor.py       Hybrid DuckDB executor with context algebra
  semantic.py       SemanticLowerer, SemanticAnalyzer, InMemoryCatalog
  _builder.py       QueryBuilder fluent API
  _magic.py         Jupyter magic (%%cql, %cql_setup, %cql_contexts)
  cli.py            cql CLI entry point
  adapters/         DuckDB adapter
  lsp/server.py     pygls-based LSP server
  parser.py         Lark-based parser
  linter.py         Semantic linter (11 rules)
grammar/            Canonical Lark grammar (contextql.lark)
tests/              pytest suite (283 tests)
examples/           Runnable demos (lint_demo.py, context_showcase.py)
vscode-contextql/   VS Code extension (LSP client)
docs/               Tooling and LSP specifications
agents/             Specialist agent specs and drafts
SPEC.md             Language specification (v0.2)
WHITEPAPER.md       Design whitepaper (43 sections)
DECISIONS.md        Architectural decisions register (59 decisions)
```

## Quick Start

The quickest way to understand the project is:
1. Install the package with the DuckDB execution engine.
2. Run the built-in demo dataset.
3. Try the parser or LSP separately if you are working on tooling.

### Run the Built-In Demo

```bash
# Install the package with the execution engine
pip install -e ".[executor]"

# Open the interactive demo REPL
cql demo
```

Inside the REPL:
- `\d` lists the registered tables and contexts.
- End each query with `;`.
- `\q` exits.

Example query:

```sql
SELECT invoice_id, amount, CONTEXT_SCORE() AS score
FROM invoices
WHERE CONTEXT IN (overdue_invoice)
ORDER BY CONTEXT DESC
LIMIT 5;
```

If you prefer a non-interactive Python snippet:

```bash
python - <<'PY'
import contextql as cql
engine = cql.demo()
result = engine.execute(
    """
    SELECT invoice_id, amount, CONTEXT_SCORE() AS score
    FROM invoices
    WHERE CONTEXT IN (overdue_invoice)
    ORDER BY CONTEXT DESC
    LIMIT 5;
    """
)
result.show()
PY
```

### Use the Fluent Builder API

```bash
python - <<'PY'
import contextql as cql
engine = cql.demo()
result = (
    engine.query("invoices")
    .select("invoice_id", "amount", "CONTEXT_SCORE() AS score")
    .where_context("overdue_invoice")
    .order_by_context()
    .limit(5)
    .execute()
)
result.show()
PY
```

### Parse Without Running Queries

```bash
pip install -e .

python - <<'PY'
from contextql.parser import ContextQLParser
parser = ContextQLParser()
tree = parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
print(tree.pretty())
PY
```

### Start the Language Server

```bash
pip install -e ".[lsp]"

contextql-lsp  # starts the language server (stdio)
```

## Demo Environment

The repository also includes a multi-source demo stack at [`demo/docker-compose.yml`](demo/docker-compose.yml).

This is useful when you want a more realistic walkthrough than the built-in in-memory Python demo:
- PostgreSQL holds operational tables such as invoices, payments, orders, events, and tickets.
- ClickHouse holds higher-volume telemetry and fulfillment-span data.
- MinIO holds supporting documents and audit evidence.
- Adminer provides a simple browser UI for the PostgreSQL side.

Important: this Docker demo is a source-system sandbox. It seeds realistic upstream data, but it is not yet wired into the current DuckDB-backed `contextql.demo()` engine automatically.

### Start the Docker Demo

```bash
docker compose -f demo/docker-compose.yml up -d

# Optional: watch the one-shot seed containers complete
docker compose -f demo/docker-compose.yml logs -f postgres-seed clickhouse-seed minio-seed

# Check the stack state
docker compose -f demo/docker-compose.yml ps
```

You should expect the `*-seed` containers to exit successfully after bootstrapping data. The long-running services are `postgres`, `clickhouse`, `minio`, and `adminer`.

### What Gets Seeded

PostgreSQL:
- `finance.vendors`
- `finance.invoices`
- `finance.payments`
- `ops.orders`
- `ops.order_events`
- `support.tickets`

ClickHouse:
- `telemetry.auth_events`
- `telemetry.fulfillment_spans`

MinIO buckets:
- `contracts`
- `policies`
- `audit-evidence`

### Connection Details

PostgreSQL:
- Host: `localhost`
- Port: `5433`
- Database: `contextql_demo`
- User: `contextql`
- Password: `contextql`

ClickHouse:
- HTTP: `http://localhost:8123`
- Native client port: `9000`

MinIO:
- Console: `http://localhost:9001`
- S3 API: `http://localhost:9002`
- User: `contextql`
- Password: `contextql-demo`

Adminer:
- URL: `http://localhost:8080`

### Inspect the Seeded Data

PostgreSQL examples:

```bash
psql postgresql://contextql:contextql@localhost:5433/contextql_demo -c \
  "SELECT status, COUNT(*) FROM finance.invoices GROUP BY 1 ORDER BY 1;"

psql postgresql://contextql:contextql@localhost:5433/contextql_demo -c \
  "SELECT risk_tier, COUNT(*) FROM finance.vendors GROUP BY 1 ORDER BY 1;"

psql postgresql://contextql:contextql@localhost:5433/contextql_demo -c \
  "SELECT i.invoice_id, i.amount, v.vendor_name, v.risk_tier
     FROM finance.invoices i
     JOIN finance.vendors v ON v.vendor_id = i.vendor_id
    WHERE i.status = 'open'
    ORDER BY i.amount DESC
    LIMIT 10;"
```

ClickHouse examples:

```bash
clickhouse-client --host localhost --query \
  "SELECT tenant, count() FROM telemetry.auth_events GROUP BY tenant ORDER BY tenant;"

clickhouse-client --host localhost --query \
  "SELECT stage, avg(duration_ms) AS avg_duration_ms
     FROM telemetry.fulfillment_spans
    GROUP BY stage
    ORDER BY avg_duration_ms DESC;"
```

### Stop or Reset the Demo

```bash
# Stop containers but keep data
docker compose -f demo/docker-compose.yml down

# Stop containers and delete seeded volumes
docker compose -f demo/docker-compose.yml down -v
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
