# ContextQL Language Specification

Version: v0.2
Status: Draft
Copyright (c) 2026 Anton du Plessis (github/adpatza)

Specification license: CC-BY-4.0
Implementation license: Apache 2.0

> **Note**: This specification describes the ContextQL language. The grammar at `grammar/contextql.lark` is the normative source. The reference implementation (parser, linter, LSP, DuckDB execution engine, Python SDK, CLI) is fully operational. For the full design rationale and architecture, see `WHITEPAPER.md`.

---

## 1. Overview

ContextQL is a SQL-first query language that introduces **contexts** as first-class query primitives for operational intelligence. A context is a named, reusable definition of an operational situation — a risk condition, process anomaly, compliance violation, or any business-meaningful condition.

The canonical grammar is defined in `grammar/contextql.lark`. This specification describes the language constructs implemented by that grammar.

For the full design rationale, execution model, and architecture, see `WHITEPAPER.md`.

---

## 2. Statements

ContextQL supports the following statement types:

### Query
- `SELECT` — query with optional context filtering, ranking, and windowing

### Context DDL
- `CREATE CONTEXT` — define a new context
- `ALTER CONTEXT` — modify an existing context
- `DROP CONTEXT` — remove a context
- `SHOW CONTEXTS` — list available contexts
- `DESCRIBE CONTEXT` — show context metadata
- `REFRESH CONTEXT` — recompute context membership
- `VALIDATE CONTEXT` — check context definition for errors

### Event Log DDL
- `CREATE EVENT LOG` — define an event log over a table
- `ALTER EVENT LOG` — modify event log metadata
- `DROP EVENT LOG` — remove an event log
- `SHOW EVENT LOGS` — list available event logs
- `DESCRIBE EVENT LOG` — show event log metadata

### Process Model DDL
- `CREATE PROCESS MODEL` — define expected process paths
- `DROP PROCESS MODEL` — remove a process model
- `SHOW PROCESS MODELS` — list process models

### Provider Registration
- `REGISTER MCP PROVIDER` — register a context provider via Model Context Protocol
- `REGISTER REMOTE PROVIDER` — register a federated data source

### Security and Administration
- `GRANT` — assign privileges to roles, users, or service accounts
- `CREATE NAMESPACE` — create a context namespace with ownership
- `SET` — configure runtime settings

---

## 3. SELECT

```
[WITH CONTEXT WINDOW n]
[WITH cte AS (subquery), ...]
SELECT [DISTINCT] projection
FROM table_ref [JOIN ...]
[WHERE predicate]
[GROUP BY expr, ...]
[HAVING predicate]
[ORDER BY order_item, ...]
[LIMIT n]
[OFFSET n]
```

### Context Window

`WITH CONTEXT WINDOW n` limits results to the top *n* entities by context score. Requires at least one scored context in the `WHERE` clause.

### Order By Context

`ORDER BY CONTEXT [USING strategy] [ASC|DESC]` ranks results by context relevance score.

Scoring strategies: `MAX`, `MIN`, `AVG`, `SUM`, `COUNT`, `WEIGHTED_MAX`, `WEIGHTED_SUM`.

### Table References

Table references may be:
- A table name with optional alias
- A `REMOTE(provider_name)` source
- A subquery
- A table-valued function: `HISTOGRAM(expr, buckets)` or `CLUSTER(subquery, args...)`

### Joins

Standard SQL join types: `[INNER] JOIN`, `LEFT [OUTER] JOIN`, `RIGHT [OUTER] JOIN`, `FULL [OUTER] JOIN`, `CROSS JOIN`.

---

## 4. Context Predicates

### CONTEXT IN

```sql
WHERE CONTEXT [ON alias] IN [ALL] (context_ref, ...)
```

Filters rows to those matching one or more contexts. Without `ALL`, contexts are combined by union (OR). With `ALL`, contexts are combined by intersection (AND).

### CONTEXT ON

`CONTEXT ON alias` binds the context predicate to a specific table in a multi-table query. Required when entity key types differ across joined tables.

### CONTEXT NOT IN

```sql
WHERE CONTEXT [ON alias] NOT IN (context_ref, ...)
```

Excludes rows matching the specified contexts.

### Context References

Each context reference in a predicate may include:

- **Weight**: `context_name WEIGHT 0.8` — assigns relative importance for score combination
- **Temporal qualifier**: `context_name AT '2024-01-01'` or `context_name BETWEEN '2024-01-01' AND '2024-06-30'` — queries a context at a specific point or range in time (requires the context to be declared `TEMPORAL`)
- **Parameterized invocation**: `context_name(threshold := 100)` — passes arguments to a parameterized context

### THEN Chains

```sql
WHERE CONTEXT IN (context_a THEN context_b THEN context_c)
```

Requires contexts to match in sequence — entity must match `context_a`, then `context_b`, then `context_c` in temporal order. Used for process pattern detection.

### MCP Providers

```sql
WHERE CONTEXT IN (MCP(provider_name))
```

Resolves context membership from an external Model Context Protocol provider.

---

## 5. Expressions

### Arithmetic
`+`, `-`, `*`, `/`, `%`, `||` (concatenation)

### Comparison
`=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`

### Predicates
`BETWEEN ... AND ...`, `LIKE`, `IN (...)`, `IS [NOT] NULL`, `EXISTS (subquery)`, `NOT`, `AND`, `OR`

### CASE
```sql
CASE [expr]
  WHEN predicate THEN expr
  [WHEN predicate THEN expr ...]
  [ELSE expr]
END
```

### CAST
```sql
CAST(expr AS type_name)
```

### Context Functions

- `CONTEXT_SCORE()` — returns the relevance score [0.0, 1.0] of the matched context for the current row. Requires `WHERE CONTEXT IN (...)`.
- `CONTEXT_COUNT()` — returns the number of contexts matched for the current row. Requires `WHERE CONTEXT IN (...)`.

### Process Intelligence Functions

- `THROUGHPUT_TIME_BETWEEN(event_log, activity_a, activity_b) [USING EVENT LOG name]` — computes elapsed time between two activities for each case.

### General Functions

Standard SQL aggregate and scalar functions are supported via the general `function(args)` syntax.

### Window Functions

SQL window functions are supported via the `OVER` clause:

```sql
function(args) OVER (
  [PARTITION BY expr, ...]
  [ORDER BY expr [ASC|DESC], ...]
  [ROWS|RANGE|GROUPS BETWEEN frame_bound AND frame_bound]
)
```

### GLOBAL()

`GLOBAL(agg_expr)` expands to a full-dataset window aggregate. Useful for ratios and percentage-of-total calculations:

```sql
SELECT invoice_id, amount, amount / GLOBAL(SUM(amount)) AS pct_of_total
FROM invoices;
-- GLOBAL(SUM(amount)) → SUM(amount) OVER ()
```

### ZSCORE()

`ZSCORE(expr)` computes the standard score of a column across the full dataset. Useful for outlier detection in context definitions:

```sql
SELECT invoice_id, ZSCORE(amount) AS z FROM invoices;
-- Expands to: (amount - AVG(amount) OVER ()) / NULLIF(STDDEV_SAMP(amount) OVER (), 0.0)
```

---

## 6. CREATE CONTEXT

```sql
CREATE [OR REPLACE] CONTEXT name [(parameters)]
  ON entity_key
  [SCORE expr]
  [TEMPORAL (column, granularity)]
  [DESCRIPTION 'text']
  [TAGS ('tag1', 'tag2', ...)]
  [CLASSIFICATION level]
  [WITH (option = value, ...)]
AS
  select_stmt | COMPOSE (ctx1 [WEIGHT w], ctx2 [WEIGHT w], ...) WITH STRATEGY strategy
```

### Parameters

Contexts may accept parameters with typed defaults:

```sql
CREATE CONTEXT overdue(threshold INT DEFAULT 30)
  ON invoice_id
AS SELECT invoice_id FROM invoices WHERE days_overdue > threshold;
```

### Scoring

`SCORE expr` defines the relevance score for each entity. Values should be in [0.0, 1.0].

### Temporal Declaration

`TEMPORAL (column, granularity)` marks a context as time-varying. Granularity: `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`.

### Composition

```sql
CREATE CONTEXT combined ON entity_id AS
  COMPOSE (ctx_a WEIGHT 0.6, ctx_b WEIGHT 0.4) WITH STRATEGY WEIGHTED
```

Strategies: `UNION`, `INTERSECT`, `WEIGHTED`.

---

## 7. ALTER CONTEXT

```sql
ALTER CONTEXT name RENAME TO new_name
ALTER CONTEXT name SET DEFINITION AS select_stmt
ALTER CONTEXT name SET SCORE expr
ALTER CONTEXT name DROP SCORE
ALTER CONTEXT name SET DESCRIPTION 'text'
ALTER CONTEXT name SET TAGS ('tag1', ...)
ALTER CONTEXT name SET STATE 'state_name'
```

---

## 8. Context Lifecycle

```sql
DROP CONTEXT [IF EXISTS] name [CASCADE | RESTRICT]
SHOW CONTEXTS [LIKE 'pattern' | WHERE predicate]
DESCRIBE CONTEXT name
REFRESH CONTEXT name [WITH PARAMETERS (arg := value, ...)]
REFRESH ALL CONTEXTS [WHERE predicate]
VALIDATE CONTEXT name
```

---

## 9. Event Logs

```sql
CREATE EVENT LOG name
  FROM source_table
  ON case_column
  ACTIVITY activity_column
  TIMESTAMP timestamp_column
  [RESOURCE resource_column]
  [ATTRIBUTES (col1, col2, ...)]
  [DESCRIPTION 'text']
  [TAGS ('tag1', ...)]
```

```sql
ALTER EVENT LOG name SET RESOURCE column
ALTER EVENT LOG name SET DESCRIPTION 'text'
ALTER EVENT LOG name ADD ATTRIBUTES (col1, ...)
ALTER EVENT LOG name DROP ATTRIBUTES (col1, ...)
DROP EVENT LOG [IF EXISTS] name
SHOW EVENT LOGS [LIKE 'pattern']
DESCRIBE EVENT LOG name
```

---

## 10. Process Models

```sql
CREATE PROCESS MODEL name
  [FOR EVENT LOG log_name]
  EXPECTED PATH ('step1', 'step2', 'step3')
  [EXPECTED PATH ('alt1', 'alt2')]
```

```sql
DROP PROCESS MODEL [IF EXISTS] name
SHOW PROCESS MODELS
```

---

## 11. Provider Registration

### MCP Provider

```sql
REGISTER MCP PROVIDER name
  ENDPOINT 'url'
  TRANSPORT transport_type
  ENTITY_TYPE type_name
  ENTITY_KEY_TYPE key_type
  [RESOURCES (resource1, resource2, ...)]
  [TIMEOUT expr]
  [AUTH method 'credentials']
  [SYSTEM_PREFIX 'prefix']
  [DESCRIPTION 'text']
  [ON_FAILURE behavior]
  [CACHE strategy expr]
```

### Remote Provider

```sql
REGISTER REMOTE PROVIDER name
  ENDPOINT 'url'
  ...  -- same options as MCP
```

---

## 12. Security

```sql
GRANT privilege_list ON target TO principal

-- Targets:
--   CONTEXT name
--   NAMESPACE name
--   MCP PROVIDER name
--   REMOTE PROVIDER name

-- Principals:
--   ROLE role_name
--   USER user_name
--   SERVICE ACCOUNT account_name
```

```sql
CREATE NAMESPACE name OWNER ROLE role_name
```

---

## 13. Type System

ContextQL extends SQL types with context-specific types:

- **Entity key types**: `INT64`, `VARCHAR`, `UUID`, `COMPOSITE`
- **Context types**: Scored context (entity set + scores), Boolean context (entity set only)
- **Temporal granularity**: `SECOND` through `YEAR`

Entity key compatibility is enforced: a context with key type `VARCHAR` cannot be applied to a table with key type `INT64`.

---

## 14. Error Codes

| Range | Category |
|-------|----------|
| E001 - E099 | Syntax errors |
| E100 - E199 | Semantic errors |
| W001 - W499 | Warnings |

See `contextql/errors.py` for the full registry and `docs/TOOLING.md` for the implemented lint rules.

---

## 15. Literals

- Strings: `'single-quoted'`
- Integers: `42`, `-1`
- Numbers: `3.14`, `1.5e10`
- Booleans: `TRUE`, `FALSE`
- Null: `NULL`
- Dates: `DATE '2024-01-15'`
- Timestamps: `TIMESTAMP '2024-01-15 10:30:00'`
- Intervals: `INTERVAL '30' DAY`

---

## 16. Comments

```sql
-- line comment
/* block comment */
```
