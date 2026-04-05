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
| Type system definitions | Defined (not yet integrated) | `contextql/types.py` |
| Language Server (LSP) | Implemented | `contextql/lsp/server.py` |
| VS Code extension | Implemented | `vscode-contextql/` |
| Execution engine (DuckDB) | Implemented | `contextql/executor.py`, `contextql/__init__.py` |
| Python Runtime SDK (`ContextQL`/`Engine`, `QueryBuilder`, `Result`, provider types) | Implemented | `contextql/__init__.py`, `contextql/_builder.py`, `contextql/providers/` |
| CLI (`cql`) | Implemented | `contextql/cli.py` |
| Jupyter magic (`%%cql`) | Implemented | `contextql/_magic.py` |
| Context Ops lifecycle | Specified | See WHITEPAPER.md Sections 19-21 |
| Process intelligence functions | Specified | See WHITEPAPER.md Sections 13-14 |
| Python SDK federation runtime | Implemented | `contextql/providers/`, `contextql/executor.py` |
| Federation DDL (REGISTER PROVIDER, wire protocol) | Designed | See WHITEPAPER.md Sections 22-23 |
| Security and governance | Designed | See WHITEPAPER.md Sections 24-30 |

## Repository Map

```
contextql/          Python package
  __init__.py       Public API: Engine, Result, CatalogProxy, demo()
  executor.py       Hybrid DuckDB executor with context algebra
  semantic.py       SemanticLowerer, SemanticAnalyzer, InMemoryCatalog
  providers/        MCP/REMOTE protocols and reference implementations
  _builder.py       QueryBuilder fluent API
  _magic.py         Jupyter magic (%%cql, %cql_setup, %cql_contexts)
  cli.py            cql CLI entry point
  adapters/         DuckDB adapter
  lsp/server.py     pygls-based LSP server
  parser.py         Lark-based parser
  linter.py         Semantic linter (11 rules)
grammar/            Canonical Lark grammar (contextql.lark)
tests/              pytest suite (361 tests)
examples/           Runnable demos (procurement_showcase, federation_demo, context_showcase, lint_demo)
vscode-contextql/   VS Code extension (LSP client)
docs/               Tooling and LSP specifications
agents/             Specialist agent specs and drafts
SPEC.md             Language specification (v0.2)
WHITEPAPER.md       Design whitepaper (43 sections)
DECISIONS.md        Architectural decisions register (79 decisions)
```

## Quick Start

Run these commands from the repository root:

```bash
cd /path/to/contextql
```

On Ubuntu/Debian, `pip install -e ...` will usually fail outside a virtual environment because of PEP 668. The recommended setup is:

```bash
cd /path/to/contextql
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

If `python3 -m venv .venv` fails, install the system venv package first:

```bash
sudo apt install python3-venv
```

After that, the quickest way to understand the project is:
1. Install the package with the DuckDB execution engine.
2. Run the built-in demo dataset.
3. Try the parser or LSP separately if you are working on tooling.

### Run the Built-In Demo

```bash
# From the repository root, inside the virtual environment:
cd /path/to/contextql
source .venv/bin/activate
python -m pip install -e ".[executor]"

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
cd /path/to/contextql
source .venv/bin/activate

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

### Run the Procurement Showcase

The procurement showcase walks through 7 escalating scenes of operational intelligence — from basic context queries to ML-augmented, cross-entity, federated analysis over the 240-invoice procurement dataset.

```bash
source .venv/bin/activate
python -m pip install -e ".[executor]"
python examples/procurement_showcase.py
```

Scenes covered:
1. **Situational Awareness** — context union with `WEIGHT` and `CONTEXT_SCORE()`
2. **Risk Amplification** — cross-entity `CONTEXT ON` with identity maps and JOINs
3. **ML Augmentation** — MCP fraud-scoring provider
4. **External Enrichment** — REMOTE Jira join with MCP context
5. **Custom Intelligence** — `@context` decorator + QueryBuilder fluent API
6. **Statistical Outliers** — `GLOBAL()` and `ZSCORE()` window macros
7. **Executive Dashboard** — full pipeline + Result API + EXPLAIN

For the federation-focused subset, run `python examples/federation_demo.py`.

### Use the Fluent Builder API

```bash
cd /path/to/contextql
source .venv/bin/activate

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

### Python Runtime API

`ContextQL` (aliased as `Engine`) is the primary runtime class.

```python
import contextql as cql

ctx = cql.ContextQL()

ctx.register_table("invoices", invoices_df, primary_key="invoice_id")

ctx.register_context(
    "open_invoice",
    "SELECT invoice_id FROM invoices WHERE status = 'open'",
    entity_key="invoice_id",
)

result = ctx.execute("""
    SELECT invoice_id, amount, CONTEXT_SCORE() AS score
    FROM invoices
    WHERE CONTEXT IN (open_invoice)
    ORDER BY CONTEXT DESC
    LIMIT 10;
""")

print(result.to_pandas())
```

The `@ctx.context()` decorator is an alternative to `register_context()`:

```python
@ctx.context("late_invoice", entity_key="invoice_id")
def late_invoice():
    return "SELECT invoice_id FROM invoices WHERE status = 'open' AND due_date < CURRENT_DATE"
```

To register MCP providers with explicit entity keys:

```python
from contextql.providers import FraudDetectionMCP

ctx.register_mcp_provider("fraud", FraudDetectionMCP(threshold=0.6), entity_key="invoice_id")
```

### MCP & REMOTE Federation

ContextQL can pull context from external ML models (MCP providers) and join federated data sources (REMOTE providers):

```python
import contextql as cql
from contextql.providers import FraudDetectionMCP, JiraRemoteProvider

engine = cql.demo()

# Register an MCP context provider (ML fraud model)
engine.register_mcp_provider("fraud", FraudDetectionMCP(threshold=0.6))

# Register a REMOTE data source (Jira issue tracker)
engine.register_remote_provider("jira", JiraRemoteProvider())

# Query combining SQL contexts + MCP + REMOTE
result = engine.execute("""
    SELECT i.invoice_id, i.amount, j.status AS jira_status,
           CONTEXT_SCORE() AS risk
    FROM invoices AS i
    JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id
    WHERE CONTEXT IN (MCP(fraud), overdue_invoice)
    ORDER BY CONTEXT DESC
    LIMIT 10;
""")
result.show()
```

For cross-entity resolution (e.g., MCP provider returns vendor IDs but the query is on invoices), register an identity map and specify the entity key:

```python
engine.register_mcp_provider("vendor_risk", VendorRiskMCP(), entity_key="vendor_id")
engine.register_identity_map("vendor", {"invoices.vendor_id": "vendors.vendor_id"})
```

Built-in reference providers: `FraudDetectionMCP`, `PriorityMCP`, `JiraRemoteProvider`. Implement your own by following the `MCPProvider` or `RemoteProvider` protocol.

### Parse Without Running Queries

```bash
cd /path/to/contextql
source .venv/bin/activate
python -m pip install -e .

python - <<'PY'
from contextql.parser import ContextQLParser
parser = ContextQLParser()
tree = parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
print(tree.pretty())
PY
```

### Start the Language Server

```bash
cd /path/to/contextql
source .venv/bin/activate
python -m pip install -e ".[lsp]"

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

By default the stack publishes services onto a high-numbered host port block to reduce collisions with software already running on your machine:
- PostgreSQL: `11010`
- ClickHouse HTTP: `11011`
- ClickHouse native client: `11012`
- MinIO console: `11013`
- MinIO S3 API: `11014`
- Adminer: `11015`

These ports are chosen to be unlikely to conflict, not guaranteed to be free. If you do need to override them, set one or more of these environment variables before starting the stack:
- `CONTEXTQL_DEMO_PG_PORT`
- `CONTEXTQL_DEMO_CLICKHOUSE_HTTP_PORT`
- `CONTEXTQL_DEMO_CLICKHOUSE_NATIVE_PORT`
- `CONTEXTQL_DEMO_MINIO_CONSOLE_PORT`
- `CONTEXTQL_DEMO_MINIO_API_PORT`
- `CONTEXTQL_DEMO_ADMINER_PORT`

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
- Port: `11010`
- Database: `contextql_demo`
- User: `contextql`
- Password: `contextql`

ClickHouse:
- HTTP: `http://localhost:11011`
- Native client port: `11012`

MinIO:
- Console: `http://localhost:11013`
- S3 API: `http://localhost:11014`
- User: `contextql`
- Password: `contextql-demo`

Adminer:
- URL: `http://localhost:11015`

### Inspect the Seeded Data

PostgreSQL examples:

```bash
psql postgresql://contextql:contextql@localhost:11010/contextql_demo -c \
  "SELECT status, COUNT(*) FROM finance.invoices GROUP BY 1 ORDER BY 1;"

psql postgresql://contextql:contextql@localhost:11010/contextql_demo -c \
  "SELECT risk_tier, COUNT(*) FROM finance.vendors GROUP BY 1 ORDER BY 1;"

psql postgresql://contextql:contextql@localhost:11010/contextql_demo -c \
  "SELECT i.invoice_id, i.amount, v.vendor_name, v.risk_tier
     FROM finance.invoices i
     JOIN finance.vendors v ON v.vendor_id = i.vendor_id
    WHERE i.status = 'open'
    ORDER BY i.amount DESC
    LIMIT 10;"
```

ClickHouse examples:

```bash
clickhouse-client --host localhost --port 11012 --user contextql --password contextql --query \
  "SELECT tenant, count() FROM telemetry.auth_events GROUP BY tenant ORDER BY tenant;"

clickhouse-client --host localhost --port 11012 --user contextql --password contextql --query \
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
| W003 | warning | Score expression outside [0.0, 1.0] range |
| W004 | warning | Weight of zero |

## Resources

- **Language Specification**: [`SPEC.md`](SPEC.md)
- **Design Whitepaper**: [`WHITEPAPER.md`](WHITEPAPER.md)
- **Design Decisions**: [`DECISIONS.md`](DECISIONS.md)

## License

Specification: CC-BY-4.0
Implementation: Apache 2.0

Copyright (c) 2026 Anton du Plessis
