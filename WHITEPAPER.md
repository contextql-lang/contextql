# ContextQL
### A Context-Native Query Language for Operational Intelligence

**Whitepaper v0.2**

Copyright (c) 2026 Anton du Plessis (github/adpatza)

Specification license: CC-BY-4.0
Implementation license: Apache 2.0

---

## Table of Contents

1. [Abstract](#abstract)
2. [Introduction](#1-introduction)
3. [Motivation](#2-motivation)
4. [Design Principles](#3-design-principles)
5. [Core Language Concepts](#4-core-language-concepts)
6. [Type System](#5-type-system)
7. [Context Set Algebra](#6-context-set-algebra)
8. [Defining Contexts](#7-defining-contexts)
9. [Querying Contexts](#8-querying-contexts)
10. [Context Ranking](#9-context-ranking)
11. [Context Windowing](#10-context-windowing)
12. [Event Log Data Model](#11-event-log-data-model)
13. [Built-in Operational Analytics](#12-built-in-operational-analytics)
14. [Process Intelligence Functions](#13-process-intelligence-functions)
15. [Conformance Checking](#14-conformance-checking)
16. [Retrieval Execution Model](#15-retrieval-execution-model)
17. [Physical Operators](#16-physical-operators)
18. [Query Optimization and Cost Model](#17-query-optimization-and-cost-model)
19. [Physical Storage Model](#18-physical-storage-model)
20. [Context Operations](#19-context-operations)
21. [Context Freshness and Consistency](#20-context-freshness-and-consistency)
22. [Scheduling and Incremental Maintenance](#21-scheduling-and-incremental-maintenance)
23. [Federated Context Providers](#22-federated-context-providers)
24. [Global Entity Namespace](#23-global-entity-namespace)
25. [Security and Threat Model](#24-security-and-threat-model)
26. [Privilege System and Access Control](#25-privilege-system-and-access-control)
27. [Context Classification and Row-Level Security](#26-context-classification-and-row-level-security)
28. [Audit Trail and Data Lineage](#27-audit-trail-and-data-lineage)
29. [Regulatory Compliance](#28-regulatory-compliance)
30. [Multi-Tenancy Isolation](#29-multi-tenancy-isolation)
31. [Interpreter Architecture](#30-interpreter-architecture)
32. [Developer Experience: CLI and REPL](#31-developer-experience-cli-and-repl)
33. [Python SDK](#32-python-sdk)
34. [Jupyter and Notebook Integration](#33-jupyter-and-notebook-integration)
35. [LLM Integration](#34-llm-integration)
36. [Error Model and Diagnostics](#35-error-model-and-diagnostics)
37. [Testing and Validation Framework](#36-testing-and-validation-framework)
38. [Connectivity Layer](#37-connectivity-layer)
39. [Implementation Strategy](#38-implementation-strategy)
40. [DDL Reference](#39-ddl-reference)
41. [SQL Conformance Declaration](#40-sql-conformance-declaration)
42. [Related Work](#41-related-work)
43. [Future Directions](#42-future-directions)
44. [Conclusion](#43-conclusion)
45. [Appendix A: Technical Glossary](#appendix-a-technical-glossary)

---

# Abstract

Modern operational analytics systems struggle to express **business situations** in a reusable and governed way. Traditional SQL excels at querying tables, but it lacks a native abstraction for **operational contexts** such as risk situations, delays, compliance violations, or process anomalies. Process mining systems partially address this challenge but often rely on proprietary query languages and tightly coupled execution environments.

This paper proposes **ContextQL**, a SQL-first query language that introduces **contexts as first-class query primitives** while remaining compatible with modern columnar analytics engines. ContextQL combines SQL-compatible syntax, reusable operational contexts, a formal context set algebra, process intelligence functions, retrieval-style ranking, columnar and vectorized execution, and a governed lifecycle for context management.

The architecture introduces a new operational layer called **Context Operations (Context Ops)** responsible for computing, caching, governing, and maintaining context membership. Together with a three-tier physical storage model (roaring bitmaps, Apache Arrow columnar tables, and Parquet files), these components form a scalable platform for **context-aware operational intelligence**, capable of millisecond-class retrieval for high-priority business situations.

```
+============================================================+
||               INTELLIGENCE LAYER                         ||
||                                                          ||
||  Queries    Ranking    Process     Conformance    LLM    ||
||  (SELECT)   (ORDER BY  Functions   Checking      Layer   ||
||             CONTEXT)   (9 funcs)   (CONFORMS_TO)         ||
+============================================================+
        |               |                |
        v               v                v
+============================================================+
||                CONTEXT LAYER                             ||
||                                                          ||
||  Context      Context     Lifecycle    Federation        ||
||  Algebra      Scoring     (9 states)   (MCP + REMOTE)   ||
||  (union,      (MAX, MIN,  Scheduling   Identity         ||
||   intersect,   WEIGHTED)  Refresh      Resolution       ||
||   negate,                 Monitoring                     ||
||   THEN)                                                  ||
+============================================================+
        |               |                |
        v               v                v
+============================================================+
||                  DATA LAYER                              ||
||                                                          ||
||  Tables     Event Logs    Bitmaps     Arrow     Parquet  ||
||  (DuckDB,   (flat, XES-   (roaring,   (warm     (cold   ||
||   Polars)    compatible)   hot tier)   tier)     tier)   ||
+============================================================+
```

*Figure 1: ContextQL introduces a dedicated Context Layer between raw data storage and intelligence queries, enabling reusable operational situation definitions as first-class query primitives.*

---

## Implementation Status (v0.2)

This whitepaper describes the complete ContextQL architecture. The following table clarifies what is implemented, what is specified as reference architecture, and what is deferred.

### Implemented

- Language grammar — `grammar/contextql.lark` (27 statement types, full expression grammar)
- Parser with error recovery — `contextql/parser.py`
- Semantic linter with 11 rules — `contextql/linter.py`
- Rich diagnostic renderer (Rust/Elm-style) — `contextql/diagnostics.py`
- Error code registry — `contextql/errors.py`
- Type system definitions — `contextql/types.py`
- Language Server Protocol server — `contextql/lsp/server.py`
- VS Code extension — `vscode-contextql/`

### Specified (Reference Architecture)

- Retrieval execution model (Sections 15-17)
- Physical storage model (Section 18)
- Context Operations lifecycle (Sections 19-21)
- Process intelligence functions (Sections 13-14)

### Designed (Protocol Surface)

- Federated context providers — MCP and REMOTE (Section 22)
- Global Entity Namespace and identity resolution (Section 23)
- Security, governance, and compliance (Sections 24-30)

### Deferred to v2

- Multi-node distribution
- Streaming integration
- OCEL support
- LLM-driven context synthesis (Section 34)

---

# 1. Introduction

Organizations increasingly require systems that answer questions such as:

- Which invoices require immediate attention?
- Which suppliers present the highest operational risk?
- Which orders are likely to breach SLA commitments?
- Which process variants indicate abnormal behavior?

Traditional SQL expresses **data relationships**, but these questions require expressing **situations** or **contexts**. Consider the traditional SQL approach:

```sql
SELECT *
FROM invoices
WHERE due_date < CURRENT_DATE
AND paid_date IS NULL
AND supplier_risk_score > 0.8;
```

The business meaning -- "Late invoices from risky suppliers" -- is implicit, buried in predicates that must be repeated across every query that needs this logic. In ContextQL, this becomes:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice, supplier_risk)
ORDER BY CONTEXT DESC;
```

Contexts become reusable semantic building blocks for operational intelligence: defined once, governed centrally, and composed freely.

---

# 2. Motivation

Existing analytics architectures follow a layered model:

```
data storage  ->  query engines  ->  semantic layers  ->  dashboards
```

Operational decision systems, however, require an additional abstraction:

```
data  ->  events  ->  objects  ->  contexts  ->  metrics  ->  queries
```

Contexts represent **situational conditions** that span multiple tables, events, or processes. Without contexts:

- Business logic is duplicated across queries, dashboards, and applications.
- Queries become difficult to maintain as operational rules evolve.
- Operational prioritization is difficult because ranking requires combining signals from multiple conditions.
- Governance is impossible because there is no named, versioned, auditable artifact representing a business situation.

ContextQL addresses this gap by elevating operational situations to first-class query primitives with formal algebra, lifecycle management, and federated composition.

---

# 3. Design Principles

ContextQL follows five guiding principles.

## 3.1 SQL-First

ContextQL extends SQL rather than replacing it. Existing SQL knowledge remains applicable. Standard SQL queries pass through to the underlying execution engine unchanged; only queries using context-specific syntax (`CONTEXT IN`, `CREATE CONTEXT`, process functions) activate the ContextQL pipeline.

## 3.2 Context-First Semantics

Operational situations are defined once and reused everywhere. A context is a named, governed, versioned abstraction that resolves to an entity membership set with optional scores.

## 3.3 Columnar-Native Execution

Execution targets modern analytical engines such as DuckDB, Polars, and Arrow. The engine processes data in columnar batches (Apache Arrow RecordBatch) with vectorized operators, enabling SIMD-friendly computation paths.

## 3.4 Retrieval-Oriented Design

Contexts resolve to **entity sets** stored as roaring bitmaps, enabling O(1) per-entity membership probes. Combined with context scores and ranking, this creates a retrieval model similar to information retrieval systems but operating on structured business data.

## 3.5 Progressive Extensibility

ContextQL is designed for incremental adoption alongside existing SQL. Organizations can start by wrapping existing WHERE clauses as named contexts, then progressively adopt scoring, process functions, federation, and lifecycle management. Each capability layer is independently useful.

---

# 4. Core Language Concepts

ContextQL operates on several core logical constructs.

## 4.1 Entities

An **entity** is a uniquely identifiable business object:

```
Entity := (entity_type, entity_id, attributes)
```

Where `entity_type` is an identifier (e.g., "invoice", "order", "vendor"), `entity_id` is the primary key value, and `attributes` are column values from the source table. Entities exist in underlying tables; ContextQL references them through SQL table references. Two entities are identical if and only if they share the same entity type and entity ID.

## 4.2 Events

An **event** is a timestamped activity record associated with an entity:

```
Event := (entity_id, activity, event_time, resource?, attributes?)
```

Events are the foundation for process intelligence functions. ContextQL assumes a flat event log model where each event is associated with exactly one case identifier, compatible with the XES standard (IEEE 1849-2016) and the majority of deployed process mining systems.

## 4.3 Contexts

A **context** is a named, reusable predicate over entities that resolves to a membership set and optional scoring function:

```
Context := (
  name        : CONTEXT_NAME,
  entity_key  : COLUMN_REF,
  definition  : SELECT_STMT,
  score_expr  : EXPRESSION | NULL,
  parameters  : LIST<PARAM_DEF>,
  temporal    : TEMPORAL_SPEC | NULL,
  dependencies: SET<CONTEXT_NAME>,
  metadata    : CONTEXT_METADATA
)
```

Evaluating a context against a database state produces a `CONTEXT_RESULT` containing the membership set (which entities are in the context), optional scores (how strongly each entity belongs), and operational metadata (evaluation timestamp, staleness, version).

## 4.4 Context Scores

A **context score** quantifies the degree, urgency, or relevance of an entity's membership. Scores are DOUBLE PRECISION values. The expected range is [0.0, 1.0], but the language does not enforce this; scores outside this range produce a warning (W100) but execution continues. A boolean context (no explicit score) assigns a default score of 1.0 to all members, allowing boolean and scored contexts to participate uniformly in ranking and composition.

```
  UNION (OR)                INTERSECTION (AND)
  score = MAX(s1, s2)       score = MIN(s1, s2)

  +-------+  +-------+     +-------+  +-------+
  | Ctx A |  | Ctx B |     | Ctx A |  | Ctx B |
  |  {1,  |  |  {2,  |     |  {1,  |  |  {2,  |
  |   2,  |  |   3,  |     |   2,  |  |   3,  |
  |   3}  |  |   4}  |     |   3}  |  |   4}  |
  +---+---+  +---+---+     +---+---+  +---+---+
      |          |              |          |
      +----+-----+              +----+-----+
           |                         |
      +----v----+              +-----v----+
      | {1,2,   |              |  {2, 3}  |
      |  3, 4}  |              +----------+
      +---------+

  NEGATION (NOT)            WEIGHTED
  score = 1 - s             score = MAX(w_i * s_i)

  +-------+  +-------+     +--------+  +--------+
  | Ctx A |  |  ALL  |     | Ctx A  |  | Ctx B  |
  |  {1,  |  | {1,2, |     | w=2.0  |  | w=0.5  |
  |   2,  |  |  3,4, |     | {1:0.8 |  | {2:0.6 |
  |   3}  |  |  5}   |     |  2:0.5}|  |  3:0.9}|
  +---+---+  +---+---+     +---+----+  +---+----+
      |          |              |            |
      +----+-----+              +-----+------+
           |                          |
      +----v----+              +------v------+
      | {4, 5}  |              | {1: 1.6,    |
      +---------+              |  2: 1.0,    |
                               |  3: 0.45}   |
  THEN (staged)                +-------------+
  score = last stage

  +-------+     +-------+
  | Ctx A |     | Ctx B |
  | {1,2, |     | (eval |
  |  3,4} |     |  only |
  +---+---+     |  on A)|
      |         +---+---+
      |             |
      +------+------+
             |
  Stage 1: resolve A --> {1,2,3,4}
  Stage 2: eval B scoped to A --> {2,4}
             |
      +------v------+
      | {2, 4}      |
      | score = s_B  |
      +-------------+
```

*Figure 2: The five context algebra operations -- union, intersection, negation, weighted composition, and THEN chains -- with their entity set semantics and scoring rules.*

---

# 5. Type System

## 5.1 Design Decision: Static Typing with Engine-Delegated Coercion

ContextQL is **statically typed at the context layer** and **delegates value-level type checking to the underlying execution engine** (DuckDB, Polars, Arrow). Context definitions, context scores, and context membership have well-defined types within ContextQL itself. Column types, expression types, and function return types within SQL subexpressions are resolved by the target engine's type system. ContextQL introduces no new scalar types; it introduces new composite types that wrap engine-native scalars.

## 5.2 Type Lattice

```
                      ANY
                       |
         +-------------+-------------+
         |             |             |
      SCALAR      CONTEXT_TYPE    SET_TYPE
         |             |             |
    (delegated    +----+----+    ENTITY_SET
     to engine)   |         |
              CONTEXT  CONTEXT_REF
                  |
            +-----+------+
            |            |
       SCORED_CTX   BOOLEAN_CTX
```

### 5.2.1 Primitive ContextQL Types

| Type | Domain | Description |
|------|--------|-------------|
| `ENTITY_ID` | Engine-native key type (INTEGER, VARCHAR, UUID, composite) | The join key that identifies an entity within a context, resolved from the `ON` clause. |
| `CONTEXT_SCORE` | DOUBLE PRECISION, range [0.0, 1.0] unless explicitly unbounded | A numeric value representing the degree or priority of context membership. |
| `CONTEXT_NAME` | Identifier conforming to SQL identifier rules | The name of a defined context. |
| `CONTEXT_MEMBERSHIP` | BOOLEAN | Whether an entity belongs to a context. NULL is not a valid membership value. |
| `CONTEXT_WINDOW_SIZE` | POSITIVE INTEGER | The maximum cardinality of a candidate entity set. |

### 5.2.2 Composite ContextQL Types

| Type | Structure | Description |
|------|-----------|-------------|
| `CONTEXT` | `(CONTEXT_NAME, definition_query, entity_key, score_expr?)` | A fully defined context object. |
| `ENTITY_SET` | `SET<ENTITY_ID>` | The materialized set of entity IDs belonging to a context. |
| `SCORED_ENTITY_SET` | `SET<(ENTITY_ID, CONTEXT_SCORE)>` | An entity set with associated scores. |
| `CONTEXT_RESULT` | `(ENTITY_SET, SCORED_ENTITY_SET?, metadata)` | The full result of evaluating a context. |

### 5.2.3 Function Return Types

| Function | Return Type | Domain |
|----------|-------------|--------|
| `PARETO_SUM(expr)` | DOUBLE PRECISION | [0.0, 1.0] |
| `HISTOGRAM(expr, bins)` | TABLE(bin_low, bin_high, count) | Set-returning function |
| `CLUSTER(expr, ...)` | TABLE(entity_id, cluster_id) | Set-returning function |
| `THROUGHPUT_TIME(entity_id)` | INTERVAL | Non-negative duration |
| `THROUGHPUT_TIME_BETWEEN(case, start, end)` | INTERVAL | Non-negative duration |
| `PATH_STRING(entity_id)` | VARCHAR | Arrow-delimited activity sequence |
| `PATH_CONTAINS(path, pattern)` | BOOLEAN | |
| `REWORK_COUNT(activity)` | INTEGER | Non-negative integer; NULL activity returns NULL |
| `CASE_START(entity_id)` | TIMESTAMP | |
| `CASE_END(entity_id)` | TIMESTAMP | |
| `VARIANT(entity_id)` | VARCHAR | Canonical path string for the trace variant |
| `ACTIVITY_COUNT(entity_id)` | INTEGER | Non-negative; returns 0 for empty cases |
| `CONTEXT_SCORE()` | DOUBLE PRECISION | [0.0, 1.0] or NULL |
| `CONTEXT_COUNT()` | INTEGER | Number of matching contexts |

## 5.3 NULL Semantics

**Rule N1 (Closed-world membership)**: Context membership is binary. If an entity's defining query returns NULL for a filter predicate, standard SQL three-valued logic applies within the defining query. The entity either appears in the result set (member) or does not (non-member). There is no "NULL membership."

**Rule N2 (NULL scores)**: If a score expression evaluates to NULL for a given entity, the entity IS a member with a NULL score. In ranking, NULL scores sort last regardless of ASC/DESC direction.

**Rule N3 (NULL propagation in aggregates)**: `PARETO_SUM`, `HISTOGRAM`, and `CLUSTER` follow standard SQL NULL handling: NULL inputs are ignored. If all inputs are NULL, the result is NULL.

**Rule N4 (NULL in process functions)**: If process functions receive an entity_id with no associated events, they return NULL. `PATH_CONTAINS(NULL, pattern)` returns NULL. `REWORK_COUNT(NULL)` returns NULL, following SQL null-propagation semantics -- treating unknown activity as unknown rework count rather than falsely returning 0.

**Rule N5 (NULL entity keys)**: An entity key that is NULL in the defining query's result set is silently dropped. A context cannot contain a NULL entity. Contexts represent identifiable business objects; a NULL key is not identifiable.

## 5.4 Type Coercion Rules

ContextQL introduces exactly two coercion contexts:

1. **Entity key coercion**: When a context is applied to a table via `WHERE CONTEXT IN (...)`, the entity key type must be join-compatible with the table's key column. "Join-compatible" is defined by the target engine's implicit cast rules. Incompatible types produce a compile-time error. Keys must be type-compatible, not column-name identical.

2. **Score coercion**: Context scores are always DOUBLE PRECISION. Integer or float score expressions are implicitly widened. Non-numeric expressions produce a compile-time error.

---

# 6. Context Set Algebra

## 6.1 Foundations

Let **E** be the universe of all entity IDs. A context **C** defines a subset `C.members` of E, and optionally a scoring function `C.score : E -> [0, 1] union {NULL}`. The context membership function is `mu_C(e) = 1` if `e` is in `C.members`, 0 otherwise. A boolean context (no explicit score) assigns a default score of 1.0 to all members.

## 6.2 Union

**Syntax**: `WHERE CONTEXT IN (c1, c2, ..., cn)`

**Membership**: `c1.members UNION c2.members UNION ... UNION cn.members`

**Scoring**: `score_union(e) = MAX(sigma_c(e))` for all contexts where `e` is a member. The maximum score preserves the most urgent signal across contexts.

## 6.3 Intersection

**Syntax**: `WHERE CONTEXT IN ALL (c1, c2, ..., cn)`

**Membership**: `c1.members INTERSECT c2.members INTERSECT ... INTERSECT cn.members`

**Scoring**: `score_intersect(e) = MIN(sigma_c(e))` for all contexts. The weakest signal governs the intersection, consistent with fuzzy logic intersection semantics: `mu(A intersect B) = min(mu(A), mu(B))`. The scoring strategy remains configurable via `ORDER BY CONTEXT USING`.

## 6.4 Negation

**Syntax**: `WHERE CONTEXT NOT IN (c1, c2, ..., cn)`

**Semantics**: Filters the FROM table to exclude entities in the union of c1...cn. Negated contexts have no score; the result is a filtered table, not a scored context.

## 6.5 Weighted Composition

**Syntax**: `WHERE CONTEXT IN (c1 WEIGHT 0.7, c2 WEIGHT 0.3)`

**Membership**: `c1.members UNION c2.members`

**Scoring**: `score_weighted(e) = MAX(w_i * sigma_ci(e))` for all contexts where `e` is a member. The weighted maximum is preferred over weighted sum because SUM allows many weak contexts to outweigh a strong one, which is undesirable for operational triage. WEIGHTED_MAX preserves the "strongest signal dominates" property while allowing weight modifiers to express relative importance. `WEIGHTED_SUM` remains available as a named scoring strategy via `ORDER BY CONTEXT USING WEIGHTED_SUM`.

**Constraints**: Weights must be non-negative DOUBLE PRECISION values. Default weight is 1.0. A weight of 0.0 means the context contributes to membership but not to scoring.

## 6.6 Conditional Context Application (THEN)

**Syntax**: `WHERE CONTEXT IN (c1 THEN c2)`

**Semantics**: Step 1: resolve c1.members. Step 2: evaluate c2 only over entities in c1.members. The resulting membership is the intersection where c2 is scoped to c1. The downstream context's score takes precedence: `score = sigma_c2(e)`.

THEN chains are unlimited and left-associative: `c1 THEN c2 THEN c3` evaluates as `(c1 THEN c2) THEN c3`. Each stage progressively narrows the candidate set and the final context's score dominates.

## 6.7 Algebra Summary

| Operator | Syntax | Membership | Default Score |
|----------|--------|------------|---------------|
| Union | `CONTEXT IN (c1, c2)` | c1 UNION c2 | MAX |
| Intersection | `CONTEXT IN ALL (c1, c2)` | c1 INTERSECT c2 | MIN |
| Negation | `CONTEXT NOT IN (c1, c2)` | FROM minus (c1 UNION c2) | N/A |
| Weighted | `CONTEXT IN (c1 WEIGHT w1, c2 WEIGHT w2)` | c1 UNION c2 | WEIGHTED_MAX |
| Conditional | `CONTEXT IN (c1 THEN c2)` | c1 INTERSECT c2(scoped) | Downstream score |

## 6.8 Algebraic Properties

- Union is commutative and associative.
- Intersection is commutative and associative.
- Negation is not commutative with union.
- Weighted composition is commutative for membership (union) but not for scoring when weights differ.
- Conditional (THEN) is not commutative.

---

# 7. Defining Contexts

## 7.1 Basic Context Definition

```sql
CREATE CONTEXT context_name
ON entity_key_column AS
select_statement;
```

The `select_statement` must return at least the column named in the `ON` clause. All rows returned are members of the context. The `ON` clause column must exist in the SELECT list, and the select statement must not contain `ORDER BY` (ordering is meaningless for membership definition).

## 7.2 Scored Context Definition

```sql
CREATE CONTEXT supplier_risk
ON vendor_id
SCORE risk_model_output AS
SELECT vendor_id, risk_model_output
FROM vendor_risk_scores
WHERE risk_model_output > 0.3;
```

The score expression is evaluated for each row to produce a `CONTEXT_SCORE`. It must evaluate to a numeric type and reference only columns available in the SELECT list.

## 7.3 Parameterized Context Definition

```sql
CREATE CONTEXT late_invoice (threshold INTEGER DEFAULT 30)
ON invoice_id AS
SELECT invoice_id
FROM invoices
WHERE due_date < CURRENT_DATE - INTERVAL :threshold DAY
AND paid_date IS NULL;
```

Parameters are bound at evaluation time using named binding syntax only (`:=`). Default values are used when a parameter is not supplied. Parameters with no default are required; omitting them is a compile-time error. Positional binding is not supported -- named binding maximizes clarity and resilience under parameter list changes.

**Invocation**: `WHERE CONTEXT IN (late_invoice(threshold := 90))`

## 7.4 Temporal Context Definition

```sql
CREATE CONTEXT overdue_order
ON order_id
TEMPORAL (due_date, DAY) AS
SELECT order_id, due_date
FROM orders
WHERE status != 'completed'
AND due_date < CURRENT_DATE;
```

Temporal filters operate on temporal column values, not historical database snapshots. The `AT` qualifier filters by the temporal column: `WHERE CONTEXT IN (overdue_order AT '2026-01-01')`. Range filtering is supported via `BETWEEN`.

## 7.5 Composite Context Definition

```sql
CREATE CONTEXT high_priority_invoice AS
COMPOSE (late_invoice, supplier_risk, disputed_invoice)
WITH STRATEGY UNION;
```

A composite context derives its membership and score from child contexts using the specified strategy. All children must share a compatible entity key type.

## 7.6 Context with Metadata

```sql
CREATE CONTEXT late_invoice
ON invoice_id
DESCRIPTION 'Invoices past their due date with no payment recorded'
TAGS ('finance', 'collections', 'priority')
AS
SELECT invoice_id FROM invoices
WHERE due_date < CURRENT_DATE AND paid_date IS NULL;
```

`DESCRIPTION` and `TAGS` are metadata-only; they support governance, discoverability, and documentation without affecting evaluation.

## 7.7 Context Dependencies

Contexts may reference other contexts in their defining queries, creating a directed acyclic graph (DAG). Cycles are detected at `CREATE CONTEXT` time using topological sort and rejected with a compile-time error reporting the full cycle path. Self-referential contexts are rejected. Evaluation follows topological order: leaf contexts first, then their dependents.

---

# 8. Querying Contexts

## 8.1 Basic Context Filtering

```sql
SELECT invoice_id, vendor_id, amount
FROM invoices
WHERE CONTEXT IN (late_invoice);
```

Execution: resolve each context to an entity set, compute the composite set per the algebra (union by default), filter the relation to rows whose entity key is in the composite set, apply additional WHERE predicates, then project the SELECT columns.

## 8.2 Multi-Table Context Binding

When a query involves multiple tables with different entity types, `CONTEXT ON` binds each context predicate to the correct table:

```sql
SELECT i.invoice_id, v.vendor_name
FROM invoices i
JOIN vendors v ON i.vendor_id = v.vendor_id
WHERE CONTEXT ON i IN (late_invoice)
  AND CONTEXT ON v IN (supplier_risk)
ORDER BY CONTEXT DESC;
```

Each `CONTEXT ON` clause resolves against the specified table alias. This enables multi-entity context filtering in a single query without subqueries or CTEs. Heterogeneous entity keys require explicit `CONTEXT ON table_alias` binding.

## 8.3 Operator Precedence

Context predicates bind at the same level as comparison operators in the WHERE clause:

```sql
WHERE CONTEXT IN (a, b) AND amount > 1000
-- Parses as: (CONTEXT IN (a, b)) AND (amount > 1000)

WHERE CONTEXT IN (a) OR CONTEXT IN (b) AND status = 'open'
-- Parses as: (CONTEXT IN (a)) OR ((CONTEXT IN (b)) AND (status = 'open'))
```

Context predicates participate in standard SQL Boolean logic and can be combined freely with AND, OR, and NOT.

## 8.4 CONTEXT_SCORE() and CONTEXT_COUNT()

Within a query using `WHERE CONTEXT IN (...)`, two special functions are available:

```sql
SELECT invoice_id, amount,
       CONTEXT_SCORE() AS priority,
       CONTEXT_COUNT() AS num_contexts
FROM invoices
WHERE CONTEXT IN (late_invoice, supplier_risk, disputed_invoice)
ORDER BY priority DESC;
```

`CONTEXT_SCORE()` returns the composite score for the current entity. `CONTEXT_COUNT()` returns the number of matching contexts. Both are valid only in queries with a `CONTEXT IN` predicate; using them without one is a compile-time error (E111).

---

# 9. Context Ranking

## 9.1 ORDER BY CONTEXT

```sql
ORDER BY CONTEXT [ASC | DESC]
```

Default direction is DESC (highest score first), because the primary use case is operational prioritization. `ORDER BY CONTEXT` requires a `WHERE CONTEXT IN (...)` predicate in the same query; using it without one is a compile-time error (E107). To rank without filtering, use `CONTEXT_SCORE()` explicitly in the ORDER BY clause.

## 9.2 Scoring Strategies

The default scoring strategy can be overridden via `ORDER BY CONTEXT USING`:

| Strategy | Description | Formula |
|----------|-------------|---------|
| `MAX` | Maximum score (default for union) | `MAX(sigma_ci(e))` |
| `MIN` | Minimum score (default for intersection) | `MIN(sigma_ci(e))` |
| `AVG` | Average across matching contexts | `AVG(sigma_ci(e))` |
| `SUM` | Sum of scores | `SUM(sigma_ci(e))` |
| `COUNT` | Number of matching contexts | count of contexts where `e` is a member |
| `WEIGHTED_MAX` | Weighted maximum (default with WEIGHT) | `MAX(w_i * sigma_ci(e))` |
| `WEIGHTED_SUM` | Weighted sum (normalized) | `SUM(w_i * sigma_ci(e)) / SUM(w_i)` |

## 9.3 Tie-Breaking Rules

When two entities have the same composite score:

1. **Primary**: Composite context score (as specified by ORDER BY CONTEXT).
2. **Secondary**: `CONTEXT_COUNT()` descending -- entities in more contexts rank higher.
3. **Tertiary**: Entity key value ascending -- deterministic for reproducibility.

Additional ORDER BY columns after `CONTEXT` serve as explicit tie-breakers and override the secondary and tertiary defaults.

## 9.4 Multi-Binding Score Aggregation

When a query contains multiple `CONTEXT ON` bindings, scoring follows a two-stage model:

**Stage 1 -- Within-binding**: Each binding produces one score using the standard context algebra rules (MAX for union, MIN for intersection, WEIGHTED_MAX for weighted composition).

**Stage 2 -- Cross-binding**: Binding scores are combined using MIN: `sigma(e) = MIN(sigma_b1(e), sigma_b2(e), ..., sigma_bn(e))`.

The entity is only as urgent as the weakest signal across dimensions, consistent with fuzzy logic intersection.

```
  Query:
    WHERE CONTEXT ON i IN (late_invoice, disputed WEIGHT 2.0)
      AND CONTEXT ON v IN (supplier_risk)

  STAGE 1: Within-Binding (per context algebra)
  =============================================

  Binding b1 (invoice_id):
    late_invoice     score = 0.7
    disputed         score = 0.4, weight = 2.0

    WEIGHTED_MAX(0.7 * 1.0, 0.4 * 2.0)
    = MAX(0.7, 0.8) = 0.8
                                  +--------+
                                  | s_b1 = |
                                  |  0.8   |
                                  +---+----+
                                      |
  Binding b2 (vendor_id):             |
    supplier_risk    score = 0.6      |
                                      |
    (single context, no combining)    |
                                  +---+----+
                                  | s_b2 = |
                                  |  0.6   |
                                  +---+----+
                                      |
  STAGE 2: Cross-Binding (MIN)        |
  ================================    |
                                      v
                              +-------+-------+
                              | final_score = |
                              | MIN(0.8, 0.6) |
                              |   = 0.6       |
                              +---------------+

  Rationale: Entity is only as urgent as
  its weakest signal across dimensions.
```

*Figure 3: Two-stage score aggregation for multi-binding queries.*

---

# 10. Context Windowing

## 10.1 Syntax

```sql
WITH CONTEXT WINDOW window_size
select_statement
```

## 10.2 Semantics

Context windowing limits the candidate entity set before ranking and projection. It is a performance optimization for large entity sets:

1. Resolve context membership (entity set).
2. If cardinality exceeds `window_size`, truncate to the top-scoring entities.
3. Apply remaining WHERE predicates, ORDER BY, and projection on the windowed set.

`WITH CONTEXT WINDOW` applies before additional WHERE predicates. `LIMIT` applies after ORDER BY. Both can coexist.

## 10.3 Windowing Without Scores

If contexts have no scores, windowing truncates by entity key ascending order. The engine emits a warning: `W101: CONTEXT WINDOW applied without scores; truncation order is by entity key ascending. Results may not reflect business priority.` This is configurable to an error via `SET contextql.window_requires_scores = 'error'`.

---

# 11. Event Log Data Model

## 11.1 Flat Event Log with Object-Centric Extension Path

ContextQL v1 operates on a flat event log model where each event is associated with exactly one case identifier. Object-centric event logs (OCEL 2.0), where a single event may relate to multiple object types, are deferred to v2. The `CREATE EVENT LOG` DDL is designed to be forward-compatible with OCEL extensions.

## 11.2 CREATE EVENT LOG

```sql
CREATE EVENT LOG invoice_events
FROM process_events
ON invoice_id
ACTIVITY activity_name
TIMESTAMP event_timestamp
RESOURCE performer
DESCRIPTION 'Invoice lifecycle events from SAP'
TAGS ('finance', 'accounts_payable');
```

This declares the mapping between a relational table and the event schema that process functions require. The `ON`, `ACTIVITY`, and `TIMESTAMP` clauses are required; `RESOURCE`, `ATTRIBUTES`, `DESCRIPTION`, and `TAGS` are optional. Constraints enforce non-NULL case IDs, activities, and timestamps. Rows violating these constraints are silently excluded with a per-query warning.

## 11.3 Event Log Binding

Process functions bind to declared event logs through the query's entity context. When a process function is invoked, the engine resolves the matching event log by entity key type compatibility. If multiple logs match, the query is ambiguous -- use `USING EVENT LOG` to disambiguate. A session-level default is also supported: `SET contextql.default_event_log = 'production_events'`.

## 11.4 ProcessTrace Pre-Computation

At execution time, the engine materializes the event log as a sorted Arrow table and pre-computes per-case structures:

```
ProcessTrace := (
  case_id    : ENTITY_ID,
  activities : LIST<VARCHAR>,
  timestamps : LIST<TIMESTAMP>,
  resources  : LIST<VARCHAR | NULL>,
  start_time : TIMESTAMP,
  end_time   : TIMESTAMP,
  event_count: INTEGER
)
```

```
  +-------------------------------------------+
  | Event Log Table (unsorted rows)           |
  |                                           |
  | case_id | activity       | timestamp      |
  | ORD-001 | Ship           | 2026-03-05     |
  | ORD-002 | Create Order   | 2026-03-01     |
  | ORD-001 | Create Order   | 2026-03-01     |
  | ORD-001 | Approve        | 2026-03-02     |
  | ORD-002 | Approve        | 2026-03-03     |
  | ORD-002 | Reject         | 2026-03-02     |
  +-------------------+-----------------------+
                      |
               Sort by (case_id, timestamp)
                      |                        O(n log n)
                      v
  +-------------------------------------------+
  | Sorted Event Log                          |
  |                                           |
  | ORD-001 | Create Order   | 2026-03-01     |
  | ORD-001 | Approve        | 2026-03-02     |
  | ORD-001 | Ship           | 2026-03-05     |
  | ORD-002 | Create Order   | 2026-03-01     |
  | ORD-002 | Reject         | 2026-03-02     |
  | ORD-002 | Approve        | 2026-03-03     |
  +-------------------+-----------------------+
                      |
               Group by case_id
                      |                        O(n)
                      v
  +-------------------------------------------+
  | ProcessTrace[] (one per case)             |
  |                                           |
  | ORD-001:                                  |
  |   activities: [Create Order,Approve,Ship] |
  |   start_time: 2026-03-01                  |
  |   end_time:   2026-03-05                  |
  |   event_count: 3                          |
  |                                           |
  | ORD-002:                                  |
  |   activities: [Create Order,Reject,       |
  |                Approve]                   |
  |   start_time: 2026-03-01                  |
  |   end_time:   2026-03-03                  |
  |   event_count: 3                          |
  +-------------------------------------------+

  Computed once per query. All process functions
  read from ProcessTrace with O(1) per case.
```

*Figure 4: The ProcessTrace extraction pipeline: raw event log rows are sorted, grouped into per-case traces, and cached for the query duration.*

## 11.5 Companion DDL

```sql
SHOW EVENT LOGS;
DESCRIBE EVENT LOG event_log_name;
ALTER EVENT LOG event_log_name SET RESOURCE new_resource_column;
DROP EVENT LOG [IF EXISTS] event_log_name;
```

`DROP EVENT LOG` removes only the declaration, not the underlying table.

---

# 12. Built-in Operational Analytics

## 12.1 PARETO_SUM

```sql
SELECT vendor_id,
       SUM(amount) AS total_amount,
       PARETO_SUM(amount) AS cumulative_share
FROM invoices
GROUP BY vendor_id
ORDER BY total_amount DESC;
```

Computes the cumulative proportion of the total for each group when ordered by value descending, enabling Pareto (80/20) analysis. Time complexity: O(n log n), dominated by the implicit sort. Return domain: [0.0, 1.0]. NULL values are ignored per standard SQL aggregate semantics.

## 12.2 HISTOGRAM

```sql
SELECT h.bin_low, h.bin_high, h.count, h.frequency
FROM HISTOGRAM(
  (SELECT cycle_time_hours FROM order_metrics),
  20
) AS h;
```

Computes an equi-width histogram with the specified number of bins. Returns a table with columns `(bin_low, bin_high, count, frequency)`. Time complexity: O(n). NULL values are excluded. Alternative histogram types (logarithmic, quantile) are deferred to v2.

## 12.3 CLUSTER

```sql
SELECT c.entity_id, c.cluster_id, c.distance
FROM CLUSTER(
  (SELECT order_id, cycle_time_hours, rework_count FROM order_metrics),
  K := 5
) AS c;
```

Assigns each entity to a cluster using k-means++ initialization with Lloyd's iteration. The `K` parameter specifies the number of clusters (default: `CEIL(SQRT(n/2))`). v1 supports k-means only; alternative algorithms are deferred to v2. Rows with NULL in any feature column are excluded from clustering and appear in the output with `cluster_id = NULL`.

---

# 13. Process Intelligence Functions

ContextQL provides nine process-aware functions that operate on declared event logs.

## 13.1 THROUGHPUT_TIME

```sql
SELECT order_id, THROUGHPUT_TIME(order_id) AS lead_time
FROM orders
WHERE THROUGHPUT_TIME(order_id) > INTERVAL '5 days';
```

Returns the elapsed wall-clock time from the first event to the last event for a case. Complexity: O(1) per case (reads from ProcessTrace). Returns NULL for cases with no events.

## 13.2 THROUGHPUT_TIME_BETWEEN

```sql
SELECT order_id,
       THROUGHPUT_TIME_BETWEEN(order_id, 'Approve', 'Ship') AS approval_to_ship
FROM orders;
```

Returns the elapsed time between the first occurrence of the start activity and the first occurrence of the end activity. Returns NULL if either activity is absent.

## 13.3 PATH_STRING

Returns the arrow-delimited activity sequence: `'Create Order -> Approve -> Ship'`. No escaping is applied to activity names containing the arrow delimiter.

## 13.4 PATH_CONTAINS

```sql
SELECT order_id
FROM orders
WHERE PATH_CONTAINS(order_id, '''Reject'' -> ''Approve''');
```

Returns TRUE if the case's activity sequence matches the pattern. Pattern matching uses NFA-based compilation: each pattern element becomes a state, `->` creates a direct-follow transition, and wildcard `*` creates epsilon/self-loop transitions. Complexity: O(m * p) per case where m = trace length, p = pattern states.

## 13.5 REWORK_COUNT

```sql
SELECT order_id, REWORK_COUNT('Approve Invoice') AS approval_count
FROM orders
WHERE REWORK_COUNT('Approve Invoice') > 1;
```

Returns the number of times the specified activity appears in a case's trace, minus one (the first occurrence is not rework). `REWORK_COUNT(NULL)` returns NULL, following SQL null-propagation semantics. Complexity: O(m) per case.

## 13.6 CASE_START and CASE_END

Return the timestamp of the first and last event for a case, respectively. Complexity: O(1) per case.

## 13.7 VARIANT

Returns a canonical path string representing the process variant (unique activity sequence). Cases with identical activity sequences share the same variant string.

## 13.8 ACTIVITY_COUNT

Returns the total number of events for a case. Returns 0 for empty cases (no events found).

## 13.9 Process-Aware Context Definitions

Process functions can appear within context definitions, enabling process-driven contexts:

```sql
CREATE CONTEXT slow_order ON order_id AS
SELECT order_id FROM orders
WHERE THROUGHPUT_TIME(order_id) > INTERVAL '10 days';

CREATE CONTEXT reworked_approval ON order_id AS
SELECT order_id FROM orders
WHERE REWORK_COUNT('Approve Invoice') > 1;
```

These contexts can then be composed with other contexts using the standard algebra:

```sql
SELECT order_id, amount, CONTEXT_SCORE() AS urgency
FROM orders
WHERE CONTEXT IN (slow_order, reworked_approval)
ORDER BY CONTEXT DESC
LIMIT 50;
```

---

# 14. Conformance Checking

## 14.1 CREATE PROCESS MODEL

```sql
CREATE PROCESS MODEL invoice_process AS
  'Create Invoice' -> 'Review' -> 'Approve' -> 'Pay'
  | 'Create Invoice' -> 'Review' -> 'Reject';
```

Declares a process model as a set of expected activity sequences. v1 supports variant matching only (explicit path enumeration). Token replay conformance and automatic process discovery are deferred to v2.

## 14.2 CONFORMS_TO

```sql
SELECT order_id, CONFORMS_TO(order_id, invoice_process) AS is_conformant
FROM orders;
```

Returns TRUE if the case's actual activity sequence matches any declared variant in the process model.

## 14.3 DEVIATION_SCORE and DEVIATION_TYPE

```sql
SELECT order_id,
       DEVIATION_SCORE(order_id, invoice_process) AS dev_score,
       DEVIATION_TYPE(order_id, invoice_process) AS dev_type
FROM orders
WHERE NOT CONFORMS_TO(order_id, invoice_process);
```

`DEVIATION_SCORE` returns a [0.0, 1.0] score quantifying the degree of deviation. `DEVIATION_TYPE` returns a categorical label describing the type of deviation (e.g., 'SKIP', 'INSERT', 'SWAP').

## 14.4 Conformance-Driven Contexts

```sql
CREATE CONTEXT non_conforming_order ON order_id
SCORE DEVIATION_SCORE(order_id, invoice_process) AS
SELECT order_id FROM orders
WHERE NOT CONFORMS_TO(order_id, invoice_process);
```

---

# 15. Retrieval Execution Model

## 15.1 Seven-Stage Pipeline

ContextQL uses a pull-based (Volcano-style) execution model with optional vectorized batching. The pipeline has seven stages:

```
  ContextQL Query Text
         |
  +------v-------+
  | 1. PARSE     |   text --> AST
  |   (Earley)   |   -- Lark grammar-driven parser, span tracking
  +------+-------+
         |
  +------v-------+
  | 2. ANALYZE   |   AST --> Annotated AST
  |   (semantic) |   -- resolve contexts, type-check
  +------+-------+   -- errors: E001-E112
         |
  +------v-------+
  | 3. PLAN      |   Annotated AST --> Logical Plan
  |   (logical)  |   -- extract CONTEXT predicates
  +------+-------+   -- inject score/window nodes
         |
  +------v-------+
  | 4. OPTIMIZE  |   Logical Plan --> Physical Plan
  |   (cost)     |   -- cost model, pushdown decisions
  +------+-------+   -- adapter selection
         |
  +------v-------+
  | 5. RESOLVE   |   Physical Plan --> Entity Sets  [#]
  |   (context)  |   -- bitmap lookup or inline eval
  +------+-------+   -- MCP federation, parallel eval
         |
  +------v-------+
  | 6. EXECUTE   |   Entity Sets + Adapter Data     [#]
  |   (pull)     |   -- bitmap probe, score compute
  +------+-------+   -- filter, rank, project
         |
  +------v-------+
  | 7. PROJECT   |   Final columns --> Arrow stream
  |   (output)   |   -- RecordBatch emission
  +------+-------+
         |
         v
  Arrow RecordBatch Stream

  [#] = Rust-accelerated in Phase 2
```

*Figure 5: The seven-stage query execution pipeline. Stages marked [#] are candidates for Rust acceleration via PyO3.*

Each operator implements a `next_batch()` interface returning columnar Arrow RecordBatches (default batch size: 8192 rows), enabling pipelined execution without full materialization of intermediate results. Independent contexts within a single query are resolved in parallel.

## 15.2 Stage Details

**PARSE**: Grammar-driven Earley parser (via Lark) producing concrete syntax trees with position tracking for context-specific productions.

**ANALYZE**: Resolves context names, checks entity key type compatibility, validates CONTEXT ON bindings, THEN chains, WEIGHT values, temporal qualifiers, parameter bindings (named only), and score function scope.

**PLAN**: Transforms the annotated AST into a logical plan using extended relational algebra operators (`LogicalContextResolve`, `LogicalContextCombine`, `LogicalContextFilter`, `LogicalContextScore`, `LogicalContextWindow`, `LogicalContextRank`).

**OPTIMIZE**: Cost-based decisions on materialize vs. recompute, bitmap vs. hash-join for filtering, scoring strategy selection, predicate pushdown, and context combination ordering (smallest contexts first).

**RESOLVE**: Resolves contexts to entity sets from hot/warm storage or via inline evaluation. MCP providers are resolved in this stage. Independent contexts are resolved in parallel.

**EXECUTE**: Pull-based evaluation with vectorized batch processing. Pipeline breakers: Sort, Aggregate, ContextWindow. Streaming operators: Filter, Project, ContextFilter, ContextScore.

**PROJECT**: Final column selection, alias application, and output serialization to Arrow.

---

# 16. Physical Operators

ContextQL defines twelve physical operators for context resolution and execution.

## 16.1 ContextResolveBitmap

Resolves a context name to a roaring bitmap from hot storage. O(1) amortized lookup.

## 16.2 ContextResolveInline

Evaluates a context's defining query at runtime via the adapter, extracts entity keys, and builds a bitmap. For non-integer keys, the entity ID dictionary maps keys to 32-bit integer surrogates.

## 16.3 ContextCombine

Combines multiple context bitmaps:

```
  Logical Algebra          Physical Operation
  ===============          ==================

  CONTEXT IN (A, B)        BitmapOr(A.bitmap, B.bitmap)
  -- union                 -- roaring_or: ~150ns per 10K

  CONTEXT IN ALL (A, B)    BitmapAnd(A.bitmap, B.bitmap)
  -- intersection          -- roaring_and: ~120ns per 10K

  CONTEXT NOT IN (A)       BitmapAndNot(all, A.bitmap)
  -- negation              -- roaring_andnot: ~130ns

  A THEN B                 ScopedEval:
  -- staged filter           1. resolve A.bitmap
                             2. scope B to A members
                             3. BitmapAnd(A, B_scoped)

  A WEIGHT 2.0             ScoreCompute:
  -- weighted                score[e] = 2.0 * A.score[e]
                             -- vectorized SIMD multiply

  Combined scoring:        VectorizedScoreCompute:
  -- MAX / MIN /             -- single pass over score
     WEIGHTED_MAX              arrays per entity
                             -- ~5ns per entity (Rust)

  Filter against table:    BitmapProbe:
                             -- lookup entity_id in bitmap
                             -- generate selection vector
                             -- ~2ns per row (SIMD)
```

*Figure 6: Mapping from logical context algebra to physical bitmap and score operations.*

## 16.4 ContextFilter

Filters a table scan to only rows whose entity key is in the bitmap. Uses SIMD-accelerated `contains_range()` for sorted integer keys. O(n) per batch.

## 16.5 ScoreCompute

Attaches context scores to filtered entities using the selected strategy (MAX, MIN, AVG, SUM, COUNT, WEIGHTED_MAX, WEIGHTED_SUM, THEN). Vectorized scoring: O(n * c) per batch where c = context count.

## 16.6 ContextWindow

Truncates the candidate set to top-k by score using introselect (O(n) time, optimal without full sort). Ties at the boundary are broken deterministically by entity ID ascending.

## 16.7 ContextRank

Sorts entities by composite context score with the three-tier tie-breaking rules (score, CONTEXT_COUNT, entity key).

## 16.8 Additional Operators

MultiTableContextFilter handles CONTEXT ON bindings across joined tables. TemporalContextResolve handles AT and BETWEEN qualifiers. FederatedContextResolve manages MCP and REMOTE provider resolution with circuit breaker integration.

---

# 17. Query Optimization and Cost Model

## 17.1 Materialize vs. Recompute

The optimizer decides whether to use a pre-materialized bitmap or evaluate inline based on the cost comparison:

```
cost_bitmap = bitmap_lookup_cost + bitmap_probe_cost_per_row * table_cardinality
cost_inline = adapter_query_cost + bitmap_build_cost + bitmap_probe_cost_per_row * table_cardinality
```

Materialized bitmaps are preferred when: the context is in MATERIALIZED state with acceptable staleness, the bitmap is in hot storage, and the adapter query cost is non-trivial.

## 17.2 Predicate Pushdown

Non-context predicates are pushed as close to the scan as possible. The optimizer identifies predicates that can be delegated to the adapter (e.g., `amount > 5000` pushed into the DuckDB scan) versus predicates that must remain in the ContextQL engine (e.g., `CONTEXT_SCORE() > 0.5`).

## 17.3 IN-List Pushdown

When a context's hot bitmap is small relative to the table, the optimizer may extract entity IDs and push them as an IN-list filter to the adapter. Threshold is adapter-specific: DuckDB 0.1% of table cardinality, Polars 0.5%, row-oriented engines 1-5%.

## 17.4 Context Combination Ordering

For union operations, the optimizer evaluates contexts in ascending cardinality order to minimize intermediate set sizes. For intersection, smallest-first enables early termination on empty containers.

## 17.5 Score Deferral

If the query has no ORDER BY CONTEXT and does not reference CONTEXT_SCORE(), score computation is deferred entirely, saving O(n * c) work.

---

# 18. Physical Storage Model

## 18.1 Three-Tier Architecture

```
  +===========================================================+
  ||                    HOT TIER                              ||
  ||  Format: Compressed Roaring Bitmaps (in memory / mmap)  ||
  ||  Access: O(1) bitmap probe per entity                   ||
  ||  Limit:  hot_storage_limit (default 1 GB)               ||
  ||  Max cardinality: 50M per context (EQ-7)                ||
  +============================+==============================+
          |          ^         |
   demotion|    promotion|     | snapshot
   (idle   |    (3+ hits |     | (every refresh)
    >30m)  |     in 5m)  |     |
          v          |         v
  +===========================================================+
  ||                   WARM TIER                             ||
  ||  Format: Apache Arrow IPC files (entity IDs + scores)   ||
  ||  Access: Columnar scan, hash join for large sets        ||
  ||  Limit:  warm_storage_limit (default 10 GB)             ||
  +============================+==============================+
          |          ^         |
   demotion|    promotion|     | archive
   (idle   |    (on      |     | (compaction schedule)
    >24h)  |     access) |     |
          v          |         v
  +===========================================================+
  ||                   COLD TIER                             ||
  ||  Format: Parquet files (historical snapshots)           ||
  ||  Access: Batch scan, daily/weekly/monthly compaction    ||
  ||  Limit:  Unbounded (filesystem)                         ||
  +===========================================================+
```

*Figure 7: The three-tier storage model with promotion/demotion flows driven by access frequency.*

**Hot tier**: Compressed roaring bitmaps in memory or memory-mapped. O(1) per-entity probe. Contexts exceeding 50M members are demoted to warm storage with hash join access.

**Warm tier**: Apache Arrow IPC files containing entity IDs and scores. Columnar scan for large-set operations.

**Cold tier**: Parquet files for historical context snapshots with unbounded retention.

## 18.2 Entity ID Dictionary

For non-integer entity keys (UUID, VARCHAR, composite), a dictionary maps each key to a 32-bit integer surrogate for bitmap storage. Composite keys use serialized key bytes for v1.

## 18.3 MVCC for Context Bitmaps

Context bitmaps use Multi-Version Concurrency Control (MVCC) with snapshot isolation. Concurrent refresh operations create new versions while queries continue to use previous versions. Garbage collection of old versions uses an adaptive grace period: `max(10 minutes, 5 * p95_query_duration)`.

---

# 19. Context Operations

Context Ops is the operational backbone of ContextQL, responsible for keeping contexts fresh, consistent, and performant.

## 19.1 Lifecycle State Machine

Every context has a lifecycle governed by a 9-state finite state machine:

```
                    CREATE CONTEXT
                         |
                         v
                    +---------+
                    |  DRAFT  |
                    +----+----+
                         |
            validate     | (semantic analysis)
                         v
                   +------------+
                   | VALIDATED  |
                   +-----+------+
                         |
        REFRESH or       | demand-based
        first access     |
                         v
                +----------------+
                | MATERIALIZING  |
                +-------+--------+
                        |
             success    |     failure
             +----------+---------+
             v                    v
      +--------------+      +---------+
      | MATERIALIZED |      |  ERROR  |---+
      +------+-------+      +----+----+   |
             |                    |        |
    +--------+--------+     retry |        |
    |        |        |          v         |
  query   stale    REFRESH  (MATERIALIZING)|
    |        |        |                    |
    |   +----v--+  +--v--------+           |
    |   | STALE |->| REFRESHING|           |
    |   +-------+  +-----+-----+           |
    |                     |                |
    |          success    |   failure      |
    |          +----------+-------+        |
    |          v                  v        |
    |   +--------------+    +---------+   |
    +-->| MATERIALIZED |    |  ERROR  |---+
        +------+-------+    +---------+
               |
         DROP CONTEXT
               |
               v
        +------------+
        |  RETIRING  |
        +-----+------+
              |
     queries  | drained
              v
        +-----------+
        |  RETIRED  |
        +-----------+
```

*Figure 8: The 9-state context lifecycle state machine.*

| State | Queries Allowed? | Description |
|-------|------------------|-------------|
| DRAFT | No | Definition accepted but not validated. |
| VALIDATED | No | Semantic analysis passed; no evaluation. |
| MATERIALIZING | No | First evaluation in progress. |
| MATERIALIZED | Yes | Bitmap exists in hot or warm storage. |
| REFRESHING | Yes (previous version) | Re-evaluation with MVCC. |
| STALE | Yes (with warning W010) | Staleness exceeds `max_staleness`. |
| ERROR | Conditional | Previous version served with warning, or error if none exists. |
| RETIRING | Draining only | No new queries; in-flight queries complete. |
| RETIRED | No | Decommissioned. Cold storage or deleted. |

## 19.2 Context Evaluation Pipeline

When materializing or refreshing:

1. **Resolve dependencies** -- ensure all upstream contexts are fresh.
2. **Transpile SQL** -- convert to adapter dialect.
3. **Execute query** -- run via adapter.
4. **Extract keys** -- pull entity_id column.
5. **Build bitmap** -- construct roaring bitmap.
6. **Build scores** -- construct score array (if scored).
7. **Write storage** -- atomic write to hot/warm storage.
8. **Update catalog** -- increment version, update timestamps and statistics.

Failure at any step transitions the context to ERROR. Steps 1-6 are idempotent. Step 7 uses atomic file operations (write-to-temp, rename) to prevent corruption.

## 19.3 Dependency Resolution and DAG

Context dependencies form a directed acyclic graph. Cycles are detected at CREATE CONTEXT time via DFS-based detection. Refresh propagation follows bottom-up topological ordering:

```
  Context Dependency DAG:

      late_invoice -----+
           |             |
           v             v
    risky_late_inv   high_value_late
           |             |
           v             v
    critical_investigation <-- MCP(fraud_model)
                               (external, no
                                scheduling dep)

  Topological Refresh Order:
  +---------------------------------------------------------+
  | Step 1:  late_invoice              (leaf, no deps)      |
  |                                                         |
  | Step 2:  risky_late_inv   \                             |
  |          high_value_late  / parallel (independent)      |
  |                                                         |
  | Step 3:  critical_investigation    (all deps fresh)     |
  |          + MCP(fraud_model) resolved at query time      |
  +---------------------------------------------------------+
```

*Figure 9: Context dependency DAG with topological refresh ordering.*

Deep dependency chains emit a warning at depth > 5 and an error at depth > 10 (configurable via `contextql.max_dependency_depth`).

## 19.4 Resource Governor

The Resource Governor prevents context operations from starving query execution:

| Resource | Default Limit |
|----------|---------------|
| Concurrent evaluations | 4 |
| Evaluation memory | 2 GB |
| Evaluation timeout | 5 minutes |
| Hot storage budget | 1 GB |
| Warm storage budget | 10 GB |
| Federation timeout | 30 seconds |

Demand-based evaluations (triggered by waiting queries) have higher priority than scheduled background refreshes.

---

# 20. Context Freshness and Consistency

## 20.1 Snapshot Isolation

ContextQL provides snapshot isolation for context reads. A query sees context membership as of the point in time when the query began. Concurrent refresh operations do not affect in-flight queries. Different queries may see different versions.

## 20.2 Freshness Model

```
staleness(ctx) = CURRENT_TIMESTAMP - ctx.last_evaluated
```

Each context has a configurable `max_staleness` (default: 5 minutes):

| Freshness | Condition | Behavior |
|-----------|-----------|----------|
| Fresh | staleness <= max_staleness | Serve from cache |
| Stale | staleness > max_staleness | Serve with warning W010, trigger async refresh |
| Very stale | staleness > 2 * max_staleness | Serve with warning W012; force sync refresh if `strict_freshness = true` |

## 20.3 DAG-Consistent Snapshots

When a query references contexts sharing a dependency chain, the query's snapshot timestamp establishes a consistent cut across the version history. If context B depends on A, the query sees a version of B computed from the same or newer version of A. Version mismatches between B's stored version and the visible A version emit informational warning W013.

---

# 21. Scheduling and Incremental Maintenance

## 21.1 Scheduling Strategies

Context Ops supports four scheduling strategies per-context:

**Time-based (cron)**: `WITH (refresh_schedule = 'every 5 minutes')` or standard cron expressions.

**Change-driven (CDC)**: Triggered when the underlying source table changes. Implemented via adapter notification mechanisms (DuckDB WAL polling, Polars file watches, external triggers).

**Demand-based**: Triggered when a query references a stale context. The query waits for evaluation to complete, subject to the evaluation timeout.

**Hybrid**: Combines cron with demand-based as fallback. Cron ensures periodic freshness; demand ensures the context is always available.

## 21.2 Incremental Context Maintenance

For contexts with append-only source data (e.g., event logs), incremental maintenance avoids full re-evaluation:

1. Track a high-water mark (last processed timestamp or row ID).
2. Query only rows newer than the high-water mark.
3. Add new entity IDs to the existing bitmap (OR operation).
4. Update scores for new and existing entities.

Eligibility criteria: the context's defining query must use monotonic filters that can be decomposed into an existing set plus a delta set. Contexts with joins, aggregations, or non-monotonic filters require full re-evaluation. The scheduling and orchestration of incremental versus full refresh is owned by Context Ops.

## 21.3 Streaming Context Updates

ContextQL v1 includes an embedded micro-batch processor for near-real-time context updates. External streaming connectors (Kafka, Flink) are deferred to v2. The micro-batch model processes buffered changes at configurable intervals (default: 1 second), maintaining bitmap and score array consistency through the same MVCC mechanism used for scheduled refreshes.

---

# 22. Federated Context Providers

ContextQL supports two distinct federated protocols with separate syntax and semantics:

| Role | Syntax | Returns | Used In |
|------|--------|---------|---------|
| Context Provider | `MCP(provider)` | `(ENTITY_ID, SCORE?)` | `WHERE CONTEXT IN (...)` |
| Data Source | `REMOTE(provider.resource)` | Relational table | `FROM` clause |

This separation preserves the context algebra as a pure entity-set operation.

```
            ContextQL Engine
           /                \
          /                  \
  MCP Protocol          REMOTE Protocol
  (context provider)    (data source)
         |                    |
         v                    v
  +--------------+    +----------------+
  | MCP Provider |    | REMOTE Provider|
  | (fraud_model)|    | (jira.issues)  |
  +--------------+    +----------------+
         |                    |
    Returns:             Returns:
    Entity IDs           Full rows
    + Scores             + Columns
         |                    |
         v                    v
  +--------------+    +----------------+
  | {entity_id,  |    | issue_id | ... |
  |  score}      |    | ISSUE-1  | ... |
  | (INV-42,0.9) |    | ISSUE-2  | ... |
  | (INV-87,0.7) |    | ISSUE-3  | ... |
  +--------------+    +----------------+
         |                    |
    Used in:             Used in:
    WHERE CONTEXT        FROM clause
    IN (MCP(...))        REMOTE(...)
         |                    |
         v                    v
  +--------------+    +----------------+
  | Bitmap +     |    | Arrow          |
  | Score Array  |    | RecordBatch    |
  +--------------+    +----------------+

  Key: MCP = lightweight entity sets (no ETL)
       REMOTE = full relational data (query pushdown)
```

*Figure 10: MCP providers return entity sets for context membership; REMOTE providers return relational data for FROM clauses.*

## 22.1 MCP Protocol

MCP providers return entity membership sets with optional scores. The wire format is JSON over HTTPS:

```json
{
  "entity_type": "invoice_id",
  "entity_key_type": "INT64",
  "members": [
    {"entity_id": 1045, "score": 0.92},
    {"entity_id": 2103, "score": 0.87}
  ],
  "data_as_of": "2026-03-17T10:00:00Z"
}
```

Entity type mismatches between the provider and the query produce a strict error (E302). No implicit coercion is applied.

**Circuit breaker**: MCP providers integrate with a circuit breaker that tracks consecutive failures. The timeout behavior is configurable with three modes: `warn` (default, treat as empty context with warning W050), `error` (abort query), or `empty` (silent empty context). Per-query fan-out is limited to 10 MCP providers (configurable via `contextql.max_mcp_providers_per_query`).

```
  Query        Context       Circuit     MCP Provider
  Engine       Ops           Breaker     (fraud_model)
    |            |              |              |
    | resolve    |              |              |
    | MCP(fraud) |              |              |
    |----------->|              |              |
    |            | check state  |              |
    |            |------------->|              |
    |            |              |              |
    |            |  {CLOSED}    |              |
    |            |<-------------|              |
    |            |              |              |
    |            | MCP Request  |              |
    |            |----------------------------->|
    |            |              |              |
    |            |              |     evaluate model
    |            |              |     select entities
    |            |              |              |
    |            |         MCP Response        |
    |            |<-----------------------------|
    |            |              |              |
    |            | validate     |              |
    |            | build bitmap |              |
    |            | build scores |              |
    |            |              |              |
    | bitmap +   |              |              |
    | scores     |              |              |
    |<-----------|              |              |
```

*Figure 11: MCP provider request sequence with circuit breaker integration.*

## 22.2 REMOTE Protocol

REMOTE providers return relational data accessible in the FROM clause:

```sql
SELECT i.invoice_id, j.status
FROM invoices i
JOIN REMOTE(jira.issues) j ON i.issue_id = j.issue_id
WHERE CONTEXT IN (late_invoice);
```

REMOTE providers support predicate pushdown: the engine pushes compatible filters to the provider to minimize data transfer. Schema negotiation occurs at registration time. Schema changes require provider re-registration.

## 22.3 Federated Event Sources

Event logs can reference REMOTE providers:

```sql
CREATE EVENT LOG issue_events
FROM REMOTE(jira.issue_history)
ON issue_id
ACTIVITY transition_name
TIMESTAMP transition_time
RESOURCE assignee;
```

---

# 23. Global Entity Namespace

## 23.1 Three Identity Levels

```
  SIMPLE MODE (default)         FEDERATED MODE (opt-in)
  No identity map needed        Full cross-system resolution

  +-------------------------+   +-------------------------+
  | Level 1: LOCAL KEYS     |   | Level 1: LOCAL KEYS     |
  | Raw database keys       |   | Raw database keys       |
  |                         |   |                         |
  | invoice_id = 42         |   | invoice_id = 42         |
  | customer_id = 1001      |   | customer_id = 1001      |
  +-------------------------+   +------------+------------+
                                             |
                                   system qualifier
                                             |
                                +------------v------------+
                                | Level 2: SYSTEM KEYS    |
                                | Prefixed, unambiguous    |
                                |                         |
                                | erp:INV-42              |
                                | jira:ISSUE-1045         |
                                | freshdesk:TKT-7890      |
                                +------------+------------+
                                             |
                                  entity_identity_map
                                  (confidence scored)
                                             |
                                +------------v------------+
                                | Level 3: GLOBAL KEYS    |
                                | Canonical cross-system  |
                                |                         |
                                | customer:C-1001 maps to:|
                                |   erp:CUST-1001  (1.0)  |
                                |   jira:ORG-ACME  (0.95) |
                                |   freshdesk:COMP (0.90) |
                                +-------------------------+
```

*Figure 12: Three-level identity model for cross-system entity resolution.*

**Simple mode** (default): Uses Levels 1 and 2 only. No cross-system resolution. Suitable for single-database deployments.

**Federated mode** (opt-in via `SET contextql.identity_mode = 'federated'`): Enables all three levels with the `entity_identity_map` for confidence-scored identity resolution. Supports many-to-many mappings between system keys. Composite identity keys use serialized key bytes.

## 23.2 Identity Resolution

The identity map is owned and maintained by Context Ops. It supports:

- Explicit mapping insertion via privileged DDL.
- Confidence scores for probabilistic matches.
- Many-to-many relationships between system keys.

Cross-tenant identity resolution is not supported in v1. Temporal identity mapping (where identities change over time) is deferred to v2.

---

# 24. Security and Threat Model

## 24.1 Assets Under Protection

| Asset | Sensitivity |
|-------|------------|
| Context definitions | Business logic IP |
| Context membership sets | Operational intelligence |
| Context scores | Risk/priority rankings |
| Entity identity map | Cross-system linkage |
| Event log data | Process data, potentially PII |
| Federation credentials | System access tokens |
| Audit logs | Compliance evidence |
| Adapter SQL (EXPLAIN VERBOSE) | Infrastructure exposure |

## 24.2 Key Attack Scenarios

**Membership inference**: Tracking context cardinality over time to infer business patterns. Mitigated by RLS-filtered member counts in DESCRIBE CONTEXT output.

**Definition exfiltration**: Using DESCRIBE CONTEXT to extract business logic. Mitigated by privilege separation between QUERY CONTEXT and VIEW DEFINITION.

**Score inflation via malicious MCP provider**: Compromised providers returning inflated scores. Mitigated by provider trust tiers and score validation.

**Cascade destruction**: Malicious DROP CONTEXT CASCADE destroying dependent contexts. Mitigated by requiring DROP privilege on every context in the transitive closure.

**Parameter injection**: SQL injection through parameterized context values. Mitigated by bind parameter execution (never string interpolation) and type enforcement at the ANALYZE stage.

**EXPLAIN VERBOSE leakage**: Adapter SQL revealing schema details. Mitigated by requiring VIEW DEFINITION privilege for EXPLAIN VERBOSE.

---

# 25. Privilege System and Access Control

## 25.1 Context Privileges

| Privilege | Description |
|-----------|-------------|
| `CREATE CONTEXT` | Create new context definitions within a namespace |
| `ALTER CONTEXT` | Modify existing context definitions |
| `DROP CONTEXT` | Delete context definitions |
| `QUERY CONTEXT` | Use a context in WHERE CONTEXT IN |
| `VIEW DEFINITION` | See defining SQL via DESCRIBE CONTEXT |
| `REFRESH CONTEXT` | Manually trigger REFRESH CONTEXT |
| `MANAGE LIFECYCLE` | Transition through lifecycle states |
| `GRANT CONTEXT` | Grant context privileges to other principals |

Federation privileges (REGISTER/ALTER/DROP/USE PROVIDER) and identity map privileges (MANAGE/VIEW IDENTITY MAP, ENABLE FEDERATED MODE) provide separate governance for federated operations. Event log privileges (CREATE/ALTER/QUERY EVENT LOG) control PII-bearing schema access.

## 25.2 Principal Model

Principals are Users, Roles, or ServiceAccounts. Four predefined roles provide graduated access:

| Role | Intended For |
|------|-------------|
| `context_reader` | Analysts querying contexts |
| `context_author` | Data engineers building contexts |
| `context_admin` | Context governance team |
| `system_admin` | System administrators |

## 25.3 Privilege Scoping

Privileges are scoped at three levels: System > Namespace > Context. Namespace grants propagate to all contexts within. Namespace creation requires the `CREATE NAMESPACE` privilege to prevent namespace squatting.

```sql
GRANT QUERY CONTEXT ON NAMESPACE finance TO ROLE finance_analysts;
GRANT CREATE CONTEXT ON NAMESPACE finance TO ROLE finance_engineers;
GRANT VIEW DEFINITION ON CONTEXT finance.fraud_detection TO ROLE fraud_team;
```

## 25.4 Privilege Enforcement

Privileges are enforced at ANALYZE (context resolution, DESCRIBE, provider references), DDL processing (CREATE/ALTER/DROP), and PROJECT (EXPLAIN VERBOSE). The system is fail-closed: if a privilege check cannot be completed, the operation is denied.

---

# 26. Context Classification and Row-Level Security

## 26.1 Classification Levels

| Classification | Description | Default Audit Level |
|---------------|-------------|-------------------|
| `PUBLIC` | No access restrictions beyond authentication | Query logging only |
| `INTERNAL` (default) | Organizational access | Query + definition change logging |
| `CONFIDENTIAL` | Business-sensitive; restricted roles | Full logging including result size |
| `RESTRICTED` | Regulatory-sensitive; compliance controls | Full logging + data lineage |

Classification propagates through composite contexts: `classification(composite) = MAX(classification(components))`. A dependent context cannot have a lower classification than its dependencies.

## 26.2 Row-Level Security

RLS filtering is applied after context membership resolution. The shared context bitmap represents complete membership; per-user filtering happens at query time:

```
  1. PARSE          Standard parsing, no security
        |
  2. ANALYZE        [!] Privilege check: QUERY CONTEXT
        |                per referenced context
        |           [!] Classification check
        |
  3. PLAN           Standard logical planning
        |
  4. OPTIMIZE       [!] RLS predicate injection
        |                Policy: region = session.region
        |                Appended to WHERE clause
        |
  5. RESOLVE        Standard context resolution
        |
  6. EXECUTE        [!] RLS filter applied
        |           +-----------------------------+
        |           | AdapterScan(invoices)       |
        |           |   WHERE region = 'EMEA'  <--+-- pushed
        |           | ContextFilter(bitmap)       |
        |           | ScoreCompute                |
        |           +-----------------------------+
        |
  7. PROJECT        [!] Audit log entry (async)
        |                Records: query, contexts,
        |                versions, principal
        v
  Result (RLS-filtered, audited)

  [!] = Security enforcement point
  Overhead: ~2us on critical path (< 0.2%)
```

*Figure 13: RLS enforcement points within the query execution pipeline.*

The `DESCRIBE CONTEXT` output shows RLS-filtered member counts per user, not global counts. `SHOW CONTEXTS` hides contexts the user cannot query, preventing information leakage from context name existence alone.

---

# 27. Audit Trail and Data Lineage

## 27.1 Audit Log Design

ContextQL maintains a hash-chained, append-only audit log in Parquet format. Every context query, DDL operation, and lifecycle transition is recorded. Audit events include:

- Query execution: principal, query text (parameterized template + parameters separately to prevent PII leakage), contexts referenced, context versions served, result cardinality, execution time.
- DDL operations: principal, operation type, affected context, before/after state.
- Lifecycle transitions: context name, from/to state, trigger, duration, error details.
- Federation events: provider name, request/response metadata, latency, success/failure.

The context snapshot version served for each query is recorded in the audit log, enabling post-hoc reproducibility for compliance investigations. Query text is stored as parameterized template plus separate parameters to prevent PII from leaking through query literals, aligning with GDPR data minimization (Article 5(1)(c)).

## 27.2 Hash Chain Integrity

Each audit record includes a cryptographic hash of the previous record, creating a tamper-evident chain. Any modification or deletion of audit records breaks the chain, detectable by verification.

## 27.3 Data Lineage

For RESTRICTED-classified contexts, full data lineage tracking records the dependency graph from source tables through intermediate contexts to final query results. PII propagation rules track whether personal data flows through the lineage chain.

---

# 28. Regulatory Compliance

## 28.1 GDPR

**Article 17 (Right to Erasure)**: Entity data must be removed from all three storage tiers upon erasure request:

```
  GDPR Erasure Request
  (entity_id = 42)
         |
         v
  +------+-------+
  | For each     |
  | context C    |
  | containing   |
  | entity 42:   |
  +------+-------+
         |
    +----+------------+-----------+
    |                 |           |
    v                 v           v
  [Hot Tier]      [Warm Tier]  [Cold Tier]
    |                 |           |
    v                 v           v
  bitmap.remove(42) Arrow IPC   Parquet
  score[42] = null  rewrite     rewrite
    |               (filter     (batched,
    |                out 42)     72h SLA)
    |                 |           |
    v                 v           |
  Immediate        Immediate     v
  (< 10us)         (< 50ms)    Batched
                                (< 72h)
         |                 |           |
         v                 v           v
  +------+-----------------+-----------+--+
  | MVCC: new version created without     |
  | entity 42. Old versions GC'd per      |
  | grace period. Audit log records       |
  | erasure with timestamp.               |
  +---------------------------------------+
```

*Figure 14: GDPR Article 17 erasure propagation across the three storage tiers.*

Hot tier bitmap removal is immediate. Warm tier Arrow rewrite is synchronous. Cold tier Parquet rewrite is batched within a 72-hour SLA, consistent with GDPR's "without undue delay" requirement.

**Article 15 (Right of Access)**: The audit trail enables subject access requests by querying all contexts containing a given entity ID.

**Article 25 (Data Protection by Design)**: Parameterized query binding, RLS enforcement, and classification-based audit levels implement privacy by design.

## 28.2 SOX

**Section 302/404**: Hash-chained audit logs provide tamper-evident records of context operations. Context version tracking in audit logs enables full reproducibility of operational decisions.

**Section 802**: Audit log retention policies ensure records are preserved for the required period.

## 28.3 HIPAA

Patient-related contexts (e.g., `patient_deterioration`) are classified as RESTRICTED, triggering full audit logging, data lineage tracking, and access logging for every query that touches the context.

---

# 29. Multi-Tenancy Isolation

ContextQL uses namespace-based tenant isolation. Each tenant operates within one or more dedicated namespaces. Context names, event logs, and providers are scoped to namespaces, preventing cross-tenant leakage.

**Storage isolation**: Each tenant's hot/warm/cold storage is logically partitioned. Bitmap files and Arrow IPC files are stored in tenant-specific directories.

**Resource quotas**: Per-tenant limits on concurrent evaluations, storage consumption, and MCP provider fan-out prevent noisy-neighbor effects.

**Cross-tenant identity resolution**: Not supported in v1, as it introduces complex governance and data isolation concerns.

---

# 30. Interpreter Architecture

```
+--------------------------------------------------------------+
|                    ContextQL System                           |
|                                                              |
|  +------------------+        +-------------------------+     |
|  |   Query Engine   |        |     Context Ops         |     |
|  |                  |        |                         |     |
|  |  Parser ------+  |        |  Lifecycle Manager      |     |
|  |  Analyzer     |  |<------>|  Scheduler              |     |
|  |  Planner      |  |        |  Dependency Resolver    |     |
|  |  Optimizer    |  |        |  Freshness Monitor      |     |
|  |  Executor     |  |        |  Incremental Engine     |     |
|  +--------+------+--+        |  Resource Governor      |     |
|           |                  +----------+--------------+     |
|           |                             |                    |
|  +--------v-----------+     +-----------v-----------+        |
|  | Federation Gateway |     |   Storage Manager     |        |
|  |                    |     |                       |        |
|  |  MCP Protocol      |     |  [Hot]  Roaring       |        |
|  |  REMOTE Protocol   |     |         Bitmaps       |        |
|  |  Circuit Breaker   |     |  [Warm] Arrow IPC     |        |
|  |  Health Monitor    |     |  [Cold] Parquet       |        |
|  +---------+----------+     |  Entity ID Dict       |        |
|            |                |  MVCC Version Mgr     |        |
|            |                +-----------+-----------+        |
+------------|----------------------------|---------+----------+
             |                            |         |
    +--------v--------+         +--------v------+  |
    | (( External ))  |         | Exec Adapters |  |
    | MCP Providers   |         |               |  |
    | REMOTE Sources  |         | DuckDB        |  |
    +-----------------+         | Polars        |  |
                                | Arrow Compute |  |
                                +---------------+  |
                                        |          |
                                +-------v-------+  |
                                | Arrow Result  |<-+
                                | (RecordBatch  |
                                |  stream)      |
                                +---------------+
```

*Figure 15: Internal component architecture of a ContextQL instance.*

## 30.1 Execution Adapter Framework

Adapters transpile SQL fragments to target engine dialects. v1 requires a single adapter per query. Each adapter declares its capabilities (predicate pushdown, aggregate pushdown, window function support, approximate counts), and the optimizer selects operator implementations accordingly. Target adapters: DuckDB (primary), Polars, Arrow compute.

## 30.2 EXPLAIN Plan

```sql
EXPLAIN SELECT * FROM invoices WHERE CONTEXT IN (late_invoice) ORDER BY CONTEXT DESC LIMIT 50;
```

`EXPLAIN` shows the logical plan with context names, operator types, and estimated costs. `EXPLAIN VERBOSE` adds the full physical plan including adapter SQL, bitmap sizes, and score strategies -- but requires VIEW DEFINITION privilege on all referenced contexts.

---

# 31. Developer Experience: CLI and REPL

## 31.1 Installation

```bash
pip install contextql
```

## 31.2 First Query (7 Lines)

```python
import contextql as cql

engine = cql.Engine("duckdb://finance.duckdb")
engine.execute("""
    CREATE CONTEXT late_invoice ON invoice_id AS
    SELECT invoice_id FROM invoices
    WHERE due_date < CURRENT_DATE AND paid_date IS NULL
""")
result = engine.execute("""
    SELECT invoice_id, amount, CONTEXT_SCORE() AS priority
    FROM invoices
    WHERE CONTEXT IN (late_invoice)
    ORDER BY CONTEXT DESC LIMIT 10
""")
result.show()
```

## 31.3 CLI (`cql`)

**Direct execution**: `cql "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice) LIMIT 10" --db duckdb://finance.duckdb`

**Interactive REPL**: `cql --repl --db duckdb://finance.duckdb`

The REPL supports dot-commands for context inspection:

```
cql> .contexts               -- list all contexts
cql> .describe late_invoice  -- show definition and metadata
cql> .providers              -- list federation providers
cql> .health                 -- show provider health
cql> .explain                -- toggle EXPLAIN before execution
```

**Watch mode**: `cql --watch "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice) ORDER BY CONTEXT DESC LIMIT 10"` re-executes the query whenever the context is refreshed, enabling terminal-based operational dashboards.

**Testing**: `cql test` discovers and runs `.cql.test` files, providing a native testing framework for context definitions without requiring Python.

```
  pip install contextql
         |
         v
  +------------------+
  | M1: First Query  |  7 lines of Python
  |                  |  import, connect, CREATE CONTEXT,
  | Engine + DuckDB  |  SELECT ... WHERE CONTEXT IN (...)
  +--------+---------+
           |
           v
  +------------------+
  | M2: Scoring      |  + ORDER BY CONTEXT, WEIGHT,
  |                  |    CONTEXT_SCORE()
  | + Ranking        |
  +--------+---------+
           |
           v
  +------------------+
  | M3: Process      |  + CREATE EVENT LOG,
  |                  |    THROUGHPUT_TIME, PATH_CONTAINS,
  | + Intelligence   |    REWORK_COUNT
  +--------+---------+
           |
           v
  +------------------+
  | M4: Federation   |  + MCP(...), REMOTE(...),
  |                  |    provider registration
  | + External Data  |
  +--------+---------+
           |
           v
  +------------------+
  | M5: Operations   |  + Lifecycle, scheduling,
  |                  |    materialization, monitoring
  | + Full Platform  |
  +------------------+

  Each milestone is independently usable.
  No milestone requires any later milestone.
```

*Figure 16: Progressive developer onboarding from pip install to full platform.*

---

# 32. Python SDK

## 32.1 Core API

```python
import contextql as cql

engine = cql.Engine("duckdb://finance.duckdb")
result = engine.execute("SELECT ...")

# Result interaction
result.show()               # Rich table display
result.to_pandas()          # Convert to pandas DataFrame
result.to_polars()          # Convert to Polars DataFrame
result.to_arrow()           # Get Arrow Table
result.to_dicts()           # List of dictionaries
print(result.row_count)     # Row count
print(result.columns)       # Column names
```

## 32.2 Catalog API

```python
catalog = engine.catalog

contexts = catalog.list_contexts()
info = catalog.describe_context("late_invoice")
catalog.refresh_context("late_invoice")
preview = catalog.preview_context("late_invoice", limit=20)
diff = catalog.diff_contexts("late_invoice@v3", "late_invoice@v4")
validation = catalog.validate_context("late_invoice")
```

## 32.3 Configuration

```python
engine = cql.Engine(
    "duckdb://finance.duckdb",
    config={
        "hot_storage_limit": "2GB",
        "max_staleness": "300s",
        "federation_timeout": "30s",
    }
)
```

## 32.4 Async API

The Python SDK provides an async API via `contextql.aio` for integration with asyncio-based applications. Phase 1 provides an async wrapper over synchronous execution to establish the API contract; Phase 2 Rust enables true async execution.

```python
import contextql.aio as cql_async

async def query():
    engine = await cql_async.Engine.connect("duckdb://finance.duckdb")
    result = await engine.execute("SELECT ...")
    return result.to_pandas()
```

---

# 33. Jupyter and Notebook Integration

## 33.1 Magic Commands

```python
%load_ext contextql

%cql_connect duckdb://finance.duckdb

%%cql
CREATE CONTEXT late_invoice ON invoice_id AS
SELECT invoice_id FROM invoices
WHERE due_date < CURRENT_DATE AND paid_date IS NULL;

%%cql
SELECT invoice_id, amount, CONTEXT_SCORE() AS priority
FROM invoices
WHERE CONTEXT IN (late_invoice)
ORDER BY CONTEXT DESC LIMIT 10
```

Cell magic (`%%cql`) executes ContextQL statements and renders results as rich HTML tables in the notebook. Results are also available as `_cql_result` for further analysis with pandas or matplotlib.

---

# 34. LLM Integration

## 34.1 Architecture

Large language models serve as interface layers, not execution engines. The critical separation between non-deterministic LLM translation and deterministic ContextQL execution ensures auditability:

```
  "Show me risky late invoices ranked by urgency"
         |
  -------+----------- Non-deterministic boundary
         v
  +------------------+
  | LLM Translation  |   May produce incorrect SQL.
  | (GPT-4, Claude)  |   Schema-constrained prompt.
  +--------+---------+   DDL blocked by default.
           |
           v
  Generated ContextQL query text
           |
  ---------+--------- Deterministic boundary
           |
           v
  +------------------+
  | 1. PARSE         |   Rejects malformed queries.
  +--------+---------+
           |
  +--------v---------+
  | 2. ANALYZE       |   Rejects undefined contexts,
  +--------+---------+   type mismatches, etc.
           |
  +--------v---------+
  | 3-7. Standard    |   Same pipeline as hand-written
  |    Execution     |   queries. Fully auditable.
  +--------+---------+
           |
           v
  Deterministic, auditable result
```

*Figure 17: LLM integration with a clear trust boundary between non-deterministic translation and deterministic execution.*

## 34.2 Python API

```python
translation = engine.from_natural_language(
    "show me late invoices from risky suppliers ranked by urgency"
)
print(translation.generated_query)
print(f"Confidence: {translation.confidence}")
result = translation.execute()
```

## 34.3 Guardrails

**Dry-run mode**: LLM-generated queries can be validated and displayed without execution, serving compliance-sensitive environments:

```python
translation = engine.from_natural_language("...", dry_run=True)
# Query is validated but never executed
```

**DDL restriction**: DDL statements are rejected by default unless `allow_ddl=True` is explicitly set.

**Post-generation validation**: Every generated query passes through the standard PARSE and ANALYZE stages before execution.

**Resource limits**: Stricter limits for LLM-generated queries (configurable max result rows, max contexts, max MCP providers).

## 34.4 Feedback Loop

```python
translation.reject(
    reason="Should use REWORK_COUNT, not PATH_CONTAINS",
    correct_query="SELECT ..."
)
# Rejection logged for prompt improvement
```

---

# 35. Error Model and Diagnostics

## 35.1 Design Philosophy

ContextQL error messages follow the Rust and Elm compiler tradition: every error identifies the exact location of the problem, explains why it is a problem, and suggests how to fix it.

## 35.2 Error Taxonomy

| Range | Category |
|-------|----------|
| E001-E099 | Syntax errors (parser failures, invalid grammar) |
| E100-E199 | Semantic errors (type mismatches, undefined references) |
| E200-E299 | Runtime errors (execution failures, timeouts) |
| E300-E399 | Federation errors (MCP/REMOTE provider failures) |
| E400-E499 | Lifecycle errors (state violations, dependency failures) |

Warning codes use a parallel scheme (W001-W499).

## 35.3 Diagnostic Output

```
  +-- error level       +-- error code
  |                     |
  v                     v
  error[E102]: entity key type mismatch for 'supplier_risk'
   --> query:4:7
    |
  2 | FROM invoices i
    |      -------- table 'invoices'
    |               has key 'invoice_id' (INT64)
  3 | WHERE CONTEXT IN (late_invoice,
  4 |       supplier_risk)
    |       ^^^^^^^^^^^^^
    |
    = context 'supplier_risk' is defined
      ON vendor_id (INT64), but the FROM
      table 'invoices' uses invoice_id

    = help: use CONTEXT ON to bind:
      |
      | WHERE CONTEXT ON i IN (late_invoice)
      |   AND CONTEXT ON v IN (supplier_risk)
      |
    = note: this query involves two
      entity types. Use explicit
      CONTEXT ON bindings.
```

*Figure 18: Anatomy of a ContextQL diagnostic message showing error level, code, source location, explanation, and actionable suggestion.*

## 35.4 Key Error Codes

| Code | Name | Description |
|------|------|-------------|
| E100 | UNDEFINED_CONTEXT | Referenced context does not exist (with "did you mean?" suggestion) |
| E102 | ENTITY_KEY_TYPE_MISMATCH | Context entity key incompatible with table |
| E103 | CIRCULAR_DEPENDENCY | Context dependency cycle detected |
| E105 | AMBIGUOUS_EVENT_LOG | Multiple event logs match; use USING EVENT LOG |
| E107 | MISSING_WHERE_CONTEXT | ORDER BY CONTEXT without WHERE CONTEXT IN |
| E112 | CONTEXT_RETIRED | Referenced context is in RETIRED state |
| E300 | MCP_PROVIDER_ERROR | MCP provider returned an error |
| E400 | CONTEXT_STATE_INVALID | Illegal lifecycle state transition |

## 35.5 Programmatic Error Handling

```python
from contextql.errors import (
    ContextQLSyntaxError,
    ContextQLSemanticError,
    ContextQLRuntimeError,
    ContextQLFederationError,
)

try:
    result = engine.execute("SELECT * FROM invoices WHERE CONTEXT IN (undefined)")
except ContextQLSemanticError as e:
    print(f"Code: {e.code}, Message: {e.message}")
    print(f"Suggestion: {e.suggestion}")
    print(f"Diagnostic:\n{e.diagnostic}")
```

When a query has multiple errors, all are collected and reported together (up to a configurable limit of 10).

---

# 36. Testing and Validation Framework

## 36.1 VALIDATE CONTEXT

```sql
VALIDATE CONTEXT late_invoice;
```

Performs comprehensive checks (parse, schema, entity key, score expression, dependencies, cycles, parameters, temporal, namespace) without evaluating the defining query.

## 36.2 Context Preview

```python
preview = engine.catalog.preview_context("late_invoice", limit=20)
print(f"Would contain {preview.total_count} members")
preview.show()
```

## 36.3 Context Diff

```python
diff = engine.catalog.diff_contexts("late_invoice@v3", "late_invoice@v4")
print(f"Added: {diff.added_count}, Removed: {diff.removed_count}")
```

## 36.4 pytest Integration

```python
import pytest
import contextql as cql

@pytest.fixture
def engine():
    e = cql.Engine("duckdb://:memory:")
    e.execute("CREATE TABLE invoices AS SELECT * FROM read_csv('tests/fixtures/invoices.csv')")
    e.execute("""
        CREATE CONTEXT late_invoice ON invoice_id AS
        SELECT invoice_id FROM invoices
        WHERE due_date < CURRENT_DATE AND paid_date IS NULL
    """)
    return e

def test_late_invoice_not_empty(engine):
    preview = engine.catalog.preview_context("late_invoice")
    assert preview.total_count > 0

def test_late_invoice_membership(engine):
    result = engine.execute("""
        SELECT invoice_id FROM invoices WHERE CONTEXT IN (late_invoice)
    """)
    member_ids = {row["invoice_id"] for row in result.to_dicts()}
    assert "INV-1001" in member_ids
```

## 36.5 Snapshot Testing

Snapshot testing captures context membership and detects drift:

```
FAILED test_late_invoice_snapshot - ContextQL Snapshot Mismatch

Snapshot: late_invoice_members
  Added (3 entities):   INV-3301, INV-3302, INV-3303
  Removed (1 entity):   INV-1045
  Unchanged: 12,480

Run with --snapshot-update to accept the new snapshot.
```

## 36.6 Federation Testing

```python
from contextql.testing import MockMCPProvider

mock_fraud = MockMCPProvider(
    name="fraud_model",
    entity_type="invoice_id",
    members=[("INV-1045", 0.92), ("INV-2103", 0.87)]
)
engine.register_mock_provider(mock_fraud)
```

---

# 37. Connectivity Layer

## 37.1 REST API

```bash
cql server --port 8080 --db duckdb://production.duckdb
```

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/query` | Execute a ContextQL query |
| POST | `/v1/query/explain` | Return execution plan |
| GET | `/v1/contexts` | List contexts |
| GET | `/v1/contexts/{name}` | Describe a context |
| POST | `/v1/contexts/{name}/refresh` | Trigger refresh |
| GET | `/v1/providers/{name}/health` | Check provider health |

Response formats: JSON, NDJSON, CSV, Arrow IPC stream. The API version (`/v1/`, `/v2/`) is independent of the ContextQL language spec version.

## 37.2 gRPC and Arrow Flight

For high-performance data transfer, the gRPC service supports Apache Arrow Flight for zero-copy streaming of large result sets.

## 37.3 JDBC/ODBC

A JDBC driver presents ContextQL as a SQL-compatible data source for BI tools (Tableau, Power BI, Looker). The driver uses a hybrid approach: contexts are exposed as virtual views for simple BI tool access, with query rewriting via comment hints for full context algebra capabilities. JDBC/ODBC drivers are Phase 2 deliverables.

---

# 38. Implementation Strategy

## 38.1 Phase 1: Python Reference Engine

- Grammar-driven Earley parser (Lark) with span tracking for error diagnostics.
- Semantic analyzer with full type checking and context validation.
- Cost-based optimizer with materialize vs. recompute decisions.
- DuckDB as primary execution adapter, Polars as secondary.
- Roaring bitmap library (`pyroaring`) for hot storage.
- Apache Arrow for in-memory columnar processing.
- REST server for remote access.
- pytest plugin for testing framework.
- CLI and REPL (`cql`).
- LSP server for IDE integration (live database with cached metadata fallback).

## 38.2 Phase 2: Rust Acceleration via PyO3

Performance-critical operations move to Rust:

- Bitmap operations (context resolve, combine, filter).
- Score computation (vectorized MAX/MIN/WEIGHTED_MAX).
- Ranking kernels (partial sort, top-k selection).
- Path encoding for process functions.
- Entity ID dictionary operations.

Rust modules integrate with Python via PyO3, maintaining the Python API surface. The `[#]` markers in the execution pipeline (RESOLVE and EXECUTE stages) indicate Phase 2 acceleration targets.

## 38.3 Module Decomposition

| Module | Language | Responsibility |
|--------|----------|---------------|
| `contextql.parser` | Python (Phase 1), Rust (Phase 2) | Grammar-driven parser (Lark/Earley), CST construction |
| `contextql.analyzer` | Python | Semantic analysis, type checking |
| `contextql.planner` | Python | Logical plan generation |
| `contextql.optimizer` | Python | Cost-based optimization |
| `contextql.executor` | Python + Rust | Physical operators, pull-based execution |
| `contextql.storage` | Python + Rust | Bitmap/Arrow/Parquet management, MVCC |
| `contextql.contextops` | Python | Lifecycle, scheduling, dependency management |
| `contextql.federation` | Python | MCP/REMOTE protocols, circuit breaker |
| `contextql.adapters` | Python | DuckDB, Polars, Arrow compute adapters |
| `contextql.security` | Python | Privilege enforcement, RLS, audit |
| `contextql.cli` | Python | CLI, REPL, server |
| `contextql.lsp` | Python | Language server protocol |

---

# 39. DDL Reference

## 39.1 Context DDL

```sql
-- Basic
CREATE CONTEXT name ON key_col AS select_stmt;

-- Scored
CREATE CONTEXT name ON key_col SCORE score_expr AS select_stmt;

-- Parameterized
CREATE CONTEXT name (param TYPE [DEFAULT value], ...) ON key_col AS select_stmt;

-- Temporal
CREATE CONTEXT name ON key_col TEMPORAL (ts_col, granularity) AS select_stmt;

-- Composite
CREATE CONTEXT name AS COMPOSE (children) WITH STRATEGY strategy;

-- With metadata and classification
CREATE CONTEXT name ON key_col
  DESCRIPTION 'text' TAGS ('t1', 't2') CLASSIFICATION level
  AS select_stmt;

-- Create or replace
CREATE OR REPLACE CONTEXT name ...;

-- Alter
ALTER CONTEXT name RENAME TO new_name;
ALTER CONTEXT name SET DEFINITION AS select_stmt;
ALTER CONTEXT name SET SCORE score_expr;
ALTER CONTEXT name DROP SCORE;

-- Drop
DROP CONTEXT [IF EXISTS] name [CASCADE | RESTRICT];

-- Inspect
SHOW CONTEXTS;
SHOW CONTEXTS LIKE 'pattern';
DESCRIBE CONTEXT name;
VALIDATE CONTEXT name;

-- Refresh
REFRESH CONTEXT name;
REFRESH ALL CONTEXTS;
```

## 39.2 Event Log DDL

```sql
CREATE EVENT LOG name FROM table ON case_col ACTIVITY act_col TIMESTAMP ts_col
  [RESOURCE res_col] [ATTRIBUTES (cols)] [DESCRIPTION 'text'] [TAGS ('t1')];

ALTER EVENT LOG name SET RESOURCE new_col;
DROP EVENT LOG [IF EXISTS] name;
SHOW EVENT LOGS;
DESCRIBE EVENT LOG name;
```

## 39.3 Process Model DDL

```sql
CREATE PROCESS MODEL name AS variant1 | variant2;
```

## 39.4 Namespace DDL

```sql
CREATE NAMESPACE name OWNER ROLE role_name;
```

## 39.5 Provider Registration

```sql
REGISTER MCP PROVIDER name
  ENDPOINT 'url'
  ENTITY_KEY_TYPE type
  [CLASSIFICATION level]
  [TIMEOUT interval]
  [CREDENTIALS 'ref'];

REGISTER REMOTE PROVIDER name
  ENDPOINT 'url'
  [CREDENTIALS 'ref'];
```

---

# 40. SQL Conformance Declaration

ContextQL extends **SQL:2016** (ISO/IEC 9075:2016), specifically Part 1 (Framework) and Part 2 (Foundation). ContextQL-specific extensions are clearly delineated and do not alter the semantics of standard SQL constructs. Standard SQL queries pass through unchanged to the execution adapter.

The Lark grammar for ContextQL is maintained in `grammar/contextql.lark` and covers: `select_stmt` with CONTEXT predicates, `create_context_stmt` in all forms, `create_event_log_stmt`, `create_process_model_stmt`, context algebra operators (IN, IN ALL, NOT IN, WEIGHT, THEN), temporal qualifiers (AT, BETWEEN), federation references (MCP, REMOTE), and all companion DDL (ALTER, DROP, SHOW, DESCRIBE, REFRESH, VALIDATE).

---

# 41. Related Work

## 41.1 Process Query Languages

**Celonis PQL**: Proprietary query language tightly coupled to the Celonis execution environment. ContextQL provides equivalent process intelligence functions with an open, SQL-compatible syntax and engine-agnostic execution. Migration tooling (`cql migrate`) supports converting PQL queries to ContextQL equivalents.

**Datalog-based systems**: Datalog's recursive query semantics are powerful for graph traversal but lack the retrieval-oriented ranking and lifecycle management that ContextQL provides.

## 41.2 Temporal Query Languages

**TSQL2**: Temporal SQL extensions for historical queries. ContextQL's temporal contexts provide a simpler, more targeted temporal model focused on context membership over time rather than full temporal database semantics.

## 41.3 Stream Processing

**CQL (Continuous Query Language)**: Designed for stream processing with window semantics. ContextQL's micro-batch context updates provide a lighter-weight alternative for operational intelligence use cases that do not require true stream processing.

## 41.4 Information Retrieval

ContextQL's ranking model draws from information retrieval: contexts are analogous to posting lists, entities are documents, and `ORDER BY CONTEXT` is analogous to ranked retrieval. The scoring strategies (MAX, MIN, WEIGHTED_MAX) parallel term-frequency aggregation models.

## 41.5 Materialized View Maintenance

Context materialization parallels materialized view maintenance. ContextQL's incremental maintenance for append-only sources and DAG-ordered refresh propagation build on established techniques while adding context-specific optimizations (bitmap algebra, score array updates).

---

# 42. Future Directions

The following capabilities are identified for v2 and beyond:

**Language extensions**: Object-centric event logs (OCEL 2.0), pattern repetition quantifiers for PATH_CONTAINS, logarithmic and quantile histogram variants, alternative clustering algorithms (DBSCAN, hierarchical), cross-binding score strategy override (CROSS_AVG, CROSS_MAX).

**Process intelligence**: Automatic process discovery, token replay conformance checking, social network analysis on resources.

**Execution**: Multi-adapter queries, distributed multi-node execution with shared-storage architecture, external streaming connectors (Kafka, Flink), temporal identity mapping.

**Federation**: Cross-tenant identity resolution, REMOTE schema evolution with automatic re-registration, provider trust tier automation.

**Developer experience**: VS Code extension with inline context visualization, graphical DAG explorer, PQL migration tooling enhancements, additional BI tool connectors.

---

# 43. Conclusion

ContextQL introduces a new abstraction for operational intelligence by elevating **contexts to first-class query primitives**. The formal context set algebra -- union, intersection, negation, weighted composition, and conditional chains -- provides a rigorous foundation for composing operational situations. The type system ensures correctness at the context layer while delegating value-level operations to proven execution engines.

The nine-state lifecycle, managed by Context Ops, ensures that contexts remain fresh, consistent, and governed across production environments. The three-tier physical storage model delivers millisecond-class retrieval through roaring bitmaps, while the MVCC concurrency model ensures query isolation during concurrent refreshes.

Process intelligence functions bring process mining capabilities directly into the query language, enabling process-aware contexts that combine event-driven insights with traditional business rules. Federation via MCP and REMOTE protocols extends context composition beyond organizational boundaries without requiring ETL.

The security model -- from privilege-based access control through classification propagation to hash-chained audit logs -- ensures that ContextQL is production-ready for regulated industries. The developer experience, from a 7-line first query to full LSP integration, ensures that the power of the language is accessible to practitioners.

By combining SQL compatibility, process intelligence, retrieval-style ranking, federated composition, and context lifecycle management, ContextQL enables scalable systems capable of answering the most important operational question:

> **Which situations matter most right now?**

---

# Appendix A: Technical Glossary

This glossary defines technical terms used throughout the whitepaper that assume domain-specific knowledge. Definitions are written for a software engineer with general experience but no specialist background in process mining, database internals, formal methods, security, or information retrieval. Each definition explains what the term means *in the context of ContextQL*.

---

**ABAC (Attribute-Based Access Control)** -- An access control model where permissions are granted based on attributes of the user, the resource, and the environment, rather than on fixed roles. ContextQL references ABAC as a complementary model to its role-based privilege system. *(Section 25)*

**Apache Arrow** -- An open-standard columnar memory format for flat and hierarchical data. In ContextQL, Arrow provides the in-memory data representation for query results (as RecordBatch streams) and serves as the warm-tier storage format via Arrow IPC files. *(Section 3.3)*

**Arrow Flight** -- A high-performance data transport protocol built on gRPC and Apache Arrow. ContextQL uses Arrow Flight for zero-copy streaming of large result sets between the server and clients. *(Section 37.2)*

**AST (Abstract Syntax Tree)** -- A tree-shaped data structure that represents the grammatical structure of a parsed query. In ContextQL, the parser converts query text into an AST, which is then annotated with type and context information during semantic analysis. *(Section 15.1)*

**Bitmap probe** -- A lookup operation that checks whether a specific entity ID exists in a roaring bitmap. In ContextQL, this is the core mechanism for filtering table rows against context membership, running at O(1) per entity. *(Section 16.1)*

**CDC (Change Data Capture)** -- A technique for detecting and capturing changes made to a data source so that downstream systems can react. ContextQL uses CDC as a scheduling trigger: when the source table changes, the dependent context is automatically refreshed. *(Section 21.1)*

**Circuit breaker** -- A fault-tolerance pattern borrowed from electrical engineering. When an external service (such as an MCP provider) fails repeatedly, the circuit breaker "opens" and stops sending requests for a cooldown period, preventing cascading failures. ContextQL applies this to all federated provider calls. *(Section 22.1)*

**Columnar execution** -- A data processing approach where values for each column are stored and processed together, rather than row by row. This enables efficient compression and CPU cache utilization. ContextQL targets columnar engines like DuckDB and Polars. *(Section 3.3)*

**Conformance checking** -- The process of comparing an observed sequence of activities (a case's actual process) against a reference process model to identify deviations. In ContextQL, the `CONFORMS_TO` function performs this comparison. *(Section 14)*

**Context** -- The central abstraction in ContextQL. A context is a named, reusable definition of a business situation (e.g., "late invoices" or "high-risk suppliers") that resolves to a set of entity IDs with optional scores. Contexts are defined once and can be composed, ranked, and governed through a managed lifecycle. *(Section 4.3)*

**Context algebra** -- The formal set of operations for combining contexts: union (OR), intersection (AND), negation (NOT), weighted composition, and conditional chaining (THEN). Each operation has defined rules for how entity membership and scores are computed. *(Section 6)*

**Context Ops (Context Operations)** -- The operational subsystem of ContextQL responsible for computing, caching, scheduling, refreshing, and governing contexts throughout their lifecycle. Context Ops manages the 9-state lifecycle, dependency resolution, incremental maintenance, and resource governance. *(Section 19)*

**Context score** -- A numeric value (DOUBLE PRECISION, typically in the range 0.0 to 1.0) that quantifies how strongly an entity belongs to a context. Scores enable ranking entities by urgency or relevance. A context without an explicit score assigns 1.0 to all members. *(Section 4.4)*

**Context window** -- A performance optimization that limits the number of candidate entities before ranking and projection. If the context membership set is very large, windowing truncates it to the top-scoring entities before applying further query logic. *(Section 10)*

**DAG (Directed Acyclic Graph)** -- A graph structure where edges have a direction and no cycles exist. In ContextQL, context dependencies form a DAG: context B can depend on context A, but circular dependencies are forbidden. Refresh and evaluation follow the DAG's topological order. *(Section 7.7)*

**Damerau-Levenshtein distance** -- A metric that measures the minimum number of insertions, deletions, substitutions, and transpositions needed to transform one string into another. Referenced in the context of deviation scoring for conformance checking. *(Section 14.3)*

**DPIA (Data Protection Impact Assessment)** -- A process required by GDPR (Article 35) to identify and minimize data protection risks of a project. ContextQL's classification and audit mechanisms support organizations conducting DPIAs for context-bearing workloads. *(Section 28)*

**EBNF (Extended Backus-Naur Form)** -- A notation for formally specifying the grammar of a language. ContextQL's grammar was originally written in EBNF; the canonical grammar is now in Lark format at `grammar/contextql.lark`, covering context statements, event log declarations, and query constructs. *(Section 40)*

**Entity set** -- The set of entity IDs that belong to a context after evaluation. Stored physically as a roaring bitmap in the hot tier. Set operations (union, intersection, difference) on entity sets correspond directly to the context algebra. *(Section 5.2.2)*

**Event log** -- A table of timestamped activity records, where each row records that a specific activity happened for a specific case at a specific time. Declared via `CREATE EVENT LOG`, event logs provide the data foundation for all process intelligence functions. *(Section 11)*

**Hash chain** -- A sequence of records where each record contains a cryptographic hash of the previous record, forming a tamper-evident chain. ContextQL uses hash chains in its audit log so that any modification or deletion of a record is detectable. *(Section 27.2)*

**Hot/warm/cold storage** -- ContextQL's three-tier physical storage model. Hot storage holds compressed roaring bitmaps in memory for sub-millisecond access. Warm storage holds Apache Arrow IPC files with entity IDs and scores for larger datasets. Cold storage holds Parquet files for historical snapshots with unbounded retention. Contexts are promoted and demoted between tiers based on access frequency. *(Section 18.1)*

**Identity resolution** -- The process of determining that records from different systems refer to the same real-world entity. In ContextQL's federated mode, the entity identity map links system-specific keys (e.g., `erp:CUST-1001` and `jira:ORG-ACME`) to a single canonical global key with confidence scores. *(Section 23.2)*

**Introselect** -- A selection algorithm that finds the k-th smallest (or largest) element in an unsorted array in O(n) time without fully sorting the data. ContextQL uses introselect for the ContextWindow operator to efficiently truncate large entity sets to the top-k by score. *(Section 16.6)*

**Left-associative** -- A property of an operator that determines grouping when the operator appears multiple times in sequence. `A THEN B THEN C` is left-associative, meaning it groups as `(A THEN B) THEN C`: the first two operands are combined first, then the result is combined with the third. *(Section 6.6)*

**LSP (Language Server Protocol)** -- A standardized protocol for communication between a code editor and a language server that provides features like auto-completion, diagnostics, and go-to-definition. ContextQL includes an LSP server implemented with pygls at `contextql/lsp/server.py`. *(Section 38.1)*

**Materialized view** -- A database object that stores the precomputed result of a query. ContextQL's context materialization is analogous: the entity set and scores are computed once and cached in bitmap/Arrow form, then served from cache until staleness triggers a refresh. *(Section 19.1)*

**MCP (Model Context Protocol)** -- In ContextQL, the federated protocol for external context providers. An MCP provider returns entity membership sets with optional scores (e.g., a fraud detection model returning invoice IDs and risk scores). MCP providers participate in context algebra like any local context. *(Section 22.1)*

**Micro-batch** -- A processing model that buffers incoming changes and processes them in small, frequent batches rather than one-at-a-time (streaming) or in large infrequent batches. ContextQL v1 uses micro-batch processing (default interval: 1 second) for near-real-time context updates. *(Section 21.3)*

**MVCC (Multi-Version Concurrency Control)** -- A technique where multiple versions of data coexist, allowing readers to see a consistent snapshot while writers create new versions. In ContextQL, MVCC ensures that context refreshes do not disrupt in-flight queries: a query continues reading the old bitmap version while a new version is being written. *(Section 18.3)*

**NFA (Nondeterministic Finite Automaton)** -- A theoretical computing model used for pattern matching. In ContextQL, the `PATH_CONTAINS` function compiles activity patterns into an NFA, where each pattern element becomes a state and transitions represent sequential activities, enabling efficient matching against process traces. *(Section 13.4)*

**Parquet** -- An open-source columnar file format optimized for read-heavy analytical workloads with efficient compression. In ContextQL, Parquet is the cold-tier storage format for historical context snapshots and the format for the append-only audit log. *(Section 18.1)*

**PEG (Parsing Expression Grammar)** -- A type of formal grammar that defines a language by specifying how to parse it, with ordered choice eliminating ambiguity. ContextQL's v1 implementation uses an Earley parser (via Lark) for practical grammar development; PEG remains a candidate for future performance optimization. *(Section 15.2)*

**PII (Personally Identifiable Information)** -- Data that can identify a specific individual (names, addresses, ID numbers, etc.). ContextQL addresses PII through classification levels, RLS filtering, parameterized query storage in audit logs, and GDPR erasure support. *(Section 27.1)*

**Posting list** -- In information retrieval, a list of document IDs that contain a given term. ContextQL draws an analogy: a context's entity set functions like a posting list, where the "term" is the operational situation and the "documents" are entities. *(Section 41.4)*

**Predicate pushdown** -- An optimization where filter conditions are pushed down from the query engine to the data source, so the source returns only matching rows. In ContextQL, non-context predicates like `amount > 5000` can be pushed to the underlying adapter (DuckDB, Polars), reducing data transfer. *(Section 17.2)*

**ProcessTrace** -- A pre-computed per-case data structure that ContextQL materializes at query time from an event log. It contains the ordered list of activities, timestamps, resources, and summary statistics for a single case, enabling all process intelligence functions to run in O(1) per case. *(Section 11.4)*

**PyO3** -- A Rust library for creating Python bindings, allowing Rust code to be called from Python. ContextQL's Phase 2 uses PyO3 to accelerate performance-critical operations (bitmap operations, score computation, ranking) in Rust while maintaining the Python API. *(Section 38.2)*

**RBAC (Role-Based Access Control)** -- An access control model where permissions are assigned to roles, and users are granted roles. ContextQL uses RBAC with four predefined roles (`context_reader`, `context_author`, `context_admin`, `system_admin`) for graduated access to context operations. *(Section 25.2)*

**REMOTE** -- In ContextQL, the federated protocol for external data sources that return full relational tables (rows and columns), used in FROM clauses. Unlike MCP providers which return entity sets, REMOTE providers supply data that can be joined, filtered, and projected like any local table. *(Section 22.2)*

**RLS (Row-Level Security)** -- A mechanism that restricts which rows a user can see based on their identity or attributes. In ContextQL, RLS predicates are injected during query optimization, filtering context membership results per-user without requiring separate bitmaps per user. *(Section 26.2)*

**Roaring bitmap** -- A compressed data structure for storing sets of integers that is both space-efficient and fast for set operations (union, intersection, difference). In ContextQL, roaring bitmaps are the primary representation for context entity sets in hot storage, enabling O(1) per-entity membership checks and sub-microsecond set operations. *(Section 3.4)*

**Snapshot isolation** -- A concurrency model where each query sees the database (or context state) as it existed at the moment the query started, unaffected by concurrent writes or refreshes. ContextQL provides snapshot isolation for context reads via MVCC. *(Section 20.1)*

**Surrogate key** -- An artificial integer key assigned to replace a non-integer or composite natural key. In ContextQL, when entity keys are UUIDs, strings, or composite values, the entity ID dictionary maps them to 32-bit integer surrogates so they can be stored in roaring bitmaps. *(Section 18.2)*

**THEN chain** -- A ContextQL-specific composition operator that applies contexts in stages. `A THEN B` first resolves context A, then evaluates context B only over the entities already in A. Each stage narrows the candidate set, and the final context's score dominates. THEN chains are left-associative and unlimited in length. *(Section 6.6)*

**Topological sort** -- An ordering of nodes in a directed acyclic graph such that every node comes after all nodes it depends on. ContextQL uses topological sort to determine the evaluation order for context dependencies: leaf contexts (no dependencies) are evaluated first, then their dependents. *(Section 7.7)*

**Type lattice** -- A hierarchy of types arranged from most general (ANY) to most specific (e.g., SCORED_CTX, BOOLEAN_CTX). In ContextQL, the type lattice defines the relationships between scalar types, context types, and set types, governing what operations and coercions are valid. *(Section 5.2)*

**Variant (process)** -- A unique activity sequence observed in an event log. Cases that follow the exact same sequence of activities (e.g., "Create Order -> Approve -> Ship") share the same variant. The `VARIANT()` function returns the canonical path string for a case, enabling grouping and analysis of process behavior patterns. *(Section 13.7)*

**Vectorized execution** -- A query processing approach where operations are applied to batches of values (vectors) at once rather than one row at a time. This enables modern CPUs to use SIMD instructions for parallel computation within a single core. ContextQL processes data in Arrow RecordBatch vectors (default 8,192 rows per batch). *(Section 15.1)*

**Volcano-style execution** -- A query execution model (also called the iterator model) where each operator in the query plan implements a `next()` method that pulls one batch of data from its input, processes it, and passes it up. ContextQL uses this pull-based model with vectorized batching for pipelined execution without full materialization of intermediate results. *(Section 15.1)*

**XES (eXtensible Event Stream)** -- An IEEE standard (IEEE 1849-2016) for representing event log data in process mining. ContextQL's flat event log model is compatible with XES, meaning XES-formatted event data can be imported and used with ContextQL's process intelligence functions. *(Section 4.2)*

---

*50 terms across 8 domains: ContextQL-specific (12), database internals (10), distributed systems (5), process mining (4), security/compliance (8), formal language/parsing (7), information retrieval (1), infrastructure/tooling (3).*

---

**End of Whitepaper v0.2**
