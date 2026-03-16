# ContextQL  
### A Context-Native Query Language for Operational Intelligence

**Draft Whitepaper v0.1**

Copyright (c) 2026 Anton du Plessis (github/adpatza)

---

# Abstract

Modern operational analytics systems struggle to express **business situations** in a reusable and governed way. Traditional SQL excels at querying tables, but it lacks a native abstraction for **operational contexts** such as risk situations, delays, compliance violations, or process anomalies.  

Process mining systems partially address this challenge but often rely on proprietary query languages and tightly coupled execution environments.

This paper proposes **ContextQL**, a SQL-first query language that introduces **contexts as first-class query primitives** while remaining compatible with modern columnar analytics engines.

ContextQL combines:

- SQL-compatible syntax
- reusable operational contexts
- process intelligence functions
- retrieval-style ranking
- columnar and vectorized execution
- optional AI and vector extensions

The architecture introduces a new operational layer called **Context Operations (Context Ops)** responsible for computing, caching, governing, and maintaining context membership.

Together, these components form a scalable platform for **context-aware operational intelligence**, capable of millisecond-class retrieval for high-priority business situations.

---

# 1. Introduction

Organizations increasingly require systems that answer questions such as:

- Which invoices require immediate attention?
- Which suppliers present the highest operational risk?
- Which orders are likely to breach SLA commitments?
- Which process variants indicate abnormal behavior?

Traditional SQL expresses **data relationships**, but these questions require expressing **situations** or **contexts**.

Example in traditional SQL:

```sql
SELECT *
FROM invoices
WHERE due_date < CURRENT_DATE
AND paid_date IS NULL
AND supplier_risk_score > 0.8;
```

The business meaning is:

> “Late invoices from risky suppliers.”

In ContextQL, this becomes:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice, supplier_risk)
ORDER BY CONTEXT DESC;
```

Contexts become reusable semantic building blocks for operational intelligence.

---

# 2. Motivation

Existing analytics architectures have the following layers:

```
data storage
query engines
semantic layers
dashboards
```

However, operational decision systems require an additional layer:

```
data
events
objects
contexts
metrics
queries
```

Contexts represent **situational conditions** that span multiple tables, events, or processes.

Without contexts:

- business logic is duplicated
- queries become difficult to maintain
- operational prioritization is difficult

ContextQL addresses this gap.

---

# 3. Design Principles

ContextQL follows five guiding principles.

## 3.1 SQL-first

ContextQL extends SQL rather than replacing it. Existing SQL knowledge remains applicable.

## 3.2 Context-first semantics

Operational situations are defined once and reused everywhere.

## 3.3 Columnar-native execution

Execution targets modern analytical engines such as DuckDB, Polars, and Arrow.

## 3.4 Retrieval-oriented design

Contexts resolve to **entity sets**, enabling efficient filtering and ranking.

## 3.5 Progressive extensibility

Advanced capabilities such as vector search and graph traversal are optional extensions.

---

# 4. Core Language Concepts

ContextQL operates on several core logical constructs.

## Entities

Business objects such as:

- orders
- invoices
- shipments
- payments
- vendors
- customers

## Events

Timestamped activities associated with entities.

Example:

```
entity_id
activity
event_time
resource
attributes
```

## Contexts

Named logical definitions that identify entities belonging to a particular operational situation.

## Context scores

Optional priority or risk scores associated with context membership.

---

# 5. Defining Contexts

Contexts are created using `CREATE CONTEXT`.

Example:

```sql
CREATE CONTEXT late_invoice
ON invoice_id AS
SELECT invoice_id
FROM invoices
WHERE due_date < CURRENT_DATE
AND paid_date IS NULL;
```

This definition creates a reusable semantic filter.

---

# 6. Querying Contexts

Contexts can be used directly inside queries.

Example:

```sql
SELECT invoice_id, vendor_id, amount
FROM invoices
WHERE CONTEXT IN (late_invoice);
```

Multiple contexts may be combined:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice, disputed_invoice, risky_vendor);
```

This resolves to the union of all context membership sets.

---

# 7. Context Ranking

Operational systems often require prioritization rather than simple filtering.

ContextQL introduces `ORDER BY CONTEXT`.

Example:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice, risky_vendor)
ORDER BY CONTEXT DESC
LIMIT 50;
```

The engine calculates a **context score** for each entity and ranks results accordingly.

Possible scoring strategies include:

- maximum context score
- weighted sum
- context-specific scoring models

---

# 8. Built-in Operational Analytics

ContextQL includes built-in analytical functions.

## Pareto analysis

```sql
SELECT vendor_id, PARETO_SUM(amount)
FROM invoices
GROUP BY vendor_id;
```

## Histogram generation

```sql
SELECT HISTOGRAM(cycle_time, 20)
FROM order_metrics;
```

## Clustering

```sql
SELECT CLUSTER(cycle_time, rework_count)
FROM order_metrics;
```

These functions support rapid operational diagnostics.

---

# 9. Process Intelligence Extensions

ContextQL supports process-aware functions.

Examples:

```
THROUGHPUT_TIME(entity_id)
PATH_STRING(entity_id)
PATH_CONTAINS(path, pattern)
REWORK_COUNT(activity)
CASE_START(entity_id)
CASE_END(entity_id)
```

These functions operate on event logs or object-centric process models.

---

# 10. Retrieval Execution Model

When the engine detects `CONTEXT`, execution follows a retrieval pipeline.

```
ContextQL query
      ↓
Parser
      ↓
AST
      ↓
Logical IR
      ↓
Context resolution
      ↓
Entity set combination
      ↓
Bitmap filtering
      ↓
Ranking
      ↓
Result projection
```

Contexts resolve to **entity sets** which are used for filtering and ranking.

---

# 11. Physical Storage Model

To achieve millisecond-class retrieval, contexts are stored in multiple forms.

## Hot storage

Compressed roaring bitmaps representing entity membership.

## Warm storage

Apache Arrow columnar tables containing entity IDs and scores.

## Cold storage

Parquet files storing historical context results.

DuckDB or similar engines orchestrate execution across these layers.

---

# 12. Context Operations (Context Ops)

Contexts require operational management similar to indexes or materialized views.

Context Ops manages:

- context definition validation
- dependency resolution
- scheduling and refresh
- caching strategies
- resource allocation
- monitoring and governance

## Context lifecycle

```
draft
 → validated
 → materialized
 → cached
 → monitored
 → retired
```

Context Ops ensures contexts remain fresh and performant.

---

# 13. Context Windowing

ContextQL supports candidate window limits.

Example:

```sql
WITH CONTEXT WINDOW 10000
SELECT *
FROM invoices
WHERE CONTEXT IN (operational_risk)
ORDER BY CONTEXT DESC;
```

This restricts candidate sets to manageable sizes before ranking.

---

# 14. Federated Context Providers

Contexts may originate from external systems.

Example:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (MCP(fraud_engine));
```

External context providers may include:

- ML models
- risk engines
- vector similarity services
- graph analytics systems

---

# 15. LLM Integration

Large language models are used as **interface layers**, not execution engines.

Typical workflow:

```
Natural language
    ↓
LLM translation
    ↓
ContextQL query
    ↓
deterministic execution
```

LLMs assist with:

- query generation
- context creation
- explanation of results

Execution remains deterministic and auditable.

---

# 16. Interpreter Architecture

ContextQL follows a standard query engine pipeline.

```
query text
  ↓
parser
  ↓
AST
  ↓
semantic analyzer
  ↓
logical IR
  ↓
planner
  ↓
execution adapters
```

Execution adapters may target:

- DuckDB
- Polars
- Arrow compute
- process mining libraries

---

# 17. Implementation Strategy

The recommended implementation path is:

### Phase 1 — Python reference engine

- rapid iteration
- simple packaging
- easy integration with data science workflows

### Phase 2 — Rust acceleration

Move performance-critical operations to Rust:

- bitmap operations
- context scoring
- ranking kernels
- path encoding

Rust modules integrate with Python using PyO3.

---

# 18. Advantages of ContextQL

ContextQL provides several advantages:

- reusable operational semantics
- faster operational prioritization
- SQL compatibility
- columnar-native performance
- AI-friendly abstraction layer

These capabilities bridge the gap between traditional SQL analytics and operational intelligence platforms.

---

# 19. Future Directions

Potential extensions include:

- graph traversal contexts
- streaming context updates
- automated context discovery
- vector-augmented contexts
- AI-driven context suggestions

---

# 20. Conclusion

ContextQL introduces a new abstraction for operational intelligence by elevating **contexts to first-class query primitives**.

By combining SQL compatibility, process intelligence, retrieval-style ranking, and context lifecycle management, ContextQL enables scalable systems capable of answering the most important operational question:

> **Which situations matter most right now?**

The introduction of a dedicated **Context Ops layer** ensures contexts remain accurate, performant, and governed across large data environments.

Together, these innovations establish the foundation for a new generation of context-aware analytics platforms.

---

**End of Draft v0.1**
