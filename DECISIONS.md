# ContextQL — Architectural Decisions Register
**File:** `DECISIONS.md`  
**Project:** ContextQL  
**Maintainer:** Anton du Plessis  
**Purpose:** Canonical record of resolved design decisions across all specification documents.

---

# How to Read This Document

This file consolidates all design decisions across the ContextQL specification layers.

Decision IDs correspond to the originating document:

| Prefix | Source Document |
|------|----------------|
| OQ | FORMALIST (language semantics) |
| EQ | ENGINE (query engine architecture) |
| PQ | PROCESSMINOR (process intelligence layer) |
| OPS | CONTEXTOPS (distributed operations layer) |
| AD | Architectural decisions (cross-cutting) |
| GQ | GUARDIAN (security, governance & compliance) |
| DX | DEVX (developer tooling & experience) |
| IM | Implementation decisions (parser, tooling) |

Each entry records:

```
Decision
Status
Resolution
Rationale
Version Scope
```

---

# FORMALIST Decisions (Language Semantics)

## OQ-1 — REWORK_COUNT(NULL) behavior

**Decision:**  
Return `NULL`.

**Rationale:**  
Follows SQL null-propagation semantics. Treats unknown activity as unknown rework count rather than falsely returning 0.

**Version:** v1

---

## OQ-2 — Intersection scoring strategy

**Decision:**  
Use `MIN`.

**Rationale:**  
Intersection semantics represent multiple conditions that must all hold.  
The weakest signal should dominate.

```
score_intersection = MIN(scores)
```

**Version:** v1

---

## OQ-3 — Score normalization enforcement

**Decision:**  
Warn when outside `[0,1]`, but do not enforce.

**Rationale:**  
Real-world scoring models may produce wider ranges.

**Version:** v1

---

## OQ-4 — CREATE EVENT LOG DDL

**Decision:**  
Include `CREATE EVENT LOG` in v1.

**Rationale:**  
Necessary for process intelligence functions to bind to event sources.

**Version:** v1

---

## OQ-5 — Multi-table context binding

**Decision:**  
Support `CONTEXT ON table_alias`.

Example:

```
WHERE CONTEXT ON i IN (late_invoice)
AND CONTEXT ON v IN (risky_vendor)
```

**Version:** v1

---

## OQ-6 — THEN chain length

**Decision:**  
Unlimited left-associative chains.

Example:

```
c1 THEN c2 THEN c3
```

**Version:** v1

---

## OQ-7 — ORDER BY CONTEXT without CONTEXT IN

**Decision:**  
Not allowed.

Use `CONTEXT_SCORE()` explicitly instead.

**Version:** v1

---

## OQ-8 — CONTEXT WINDOW without scoring

**Decision:**  
Allow but emit warning.

Configurable to error.

**Version:** v1

---

## OQ-9 — Temporal context semantics

**Decision:**  
Temporal filters operate on temporal column values, not historical database snapshots.

**Version:** v1

---

## OQ-10 — Context namespaces

**Decision:**  
Single-level namespaces.

Example:

```
finance.late_invoice
support.escalated_ticket
```

**Version:** v1

---

## OQ-11 — Parameter binding style

**Decision:**  
Named parameters only.

Example:

```
threshold := 30
```

**Version:** v1

---

## OQ-12 — Composite context key compatibility

**Decision:**  
Keys must be type-compatible, not column-name identical.

**Version:** v1

---

# ENGINE Decisions (Query Engine)

## EQ-1 — Federated provider timeout behavior

**Decision:**  
Configurable with three modes:

```
warn (default)
error
empty
```

**Version:** v1

---

## EQ-2 — IN-list pushdown threshold

**Decision:**  
Adapter-specific configuration.

Example defaults:

| Adapter | Threshold |
|--------|-----------|
DuckDB | 0.1% |
Polars | 0.5% |
Row engines | 1–5% |

**Version:** v1

---

## EQ-3 — Composite key dictionary storage

**Decision:**  
Use serialized key bytes for v1.

**Version:** v1

---

## EQ-4 — Mixed adapter execution

**Decision:**  
Single adapter per query.

Multi-adapter execution deferred.

**Version:** v1

---

## EQ-5 — MVCC garbage collection grace period

**Decision:**  

```
max(10 minutes, 5 × p95 query duration)
```

Adaptive GC period.

**Version:** v1

---

## EQ-6 — Parallel context resolution

**Decision:**  
Allow parallel evaluation for independent contexts.

**Version:** v1

---

## EQ-7 — Large context sets

**Decision:**  

```
hot_max_context_cardinality = 50M
```

Above threshold → warm storage + hash join.

**Version:** v1

---

## EQ-8 — EXPLAIN adapter SQL visibility

**Decision:**  
Visible only in `EXPLAIN VERBOSE`.

**Version:** v1

---

# PROCESSMINOR Decisions (Process Intelligence)

## PQ-1 — Default event log session variable

**Decision:**  
Allow both explicit `USING EVENT LOG` and session default.

**Version:** v1

---

## PQ-2 — Additional histogram types

**Decision:**  
Defer logarithmic and quantile histograms.

**Version:** v2

---

## PQ-3 — Alternative clustering algorithms

**Decision:**  
k-means only.

**Version:** v1

---

## PQ-4 — PATH_STRING escaping

**Decision:**  
No escaping.

**Version:** v1

---

## PQ-5 — Pattern repetition quantifiers

**Decision:**  
Not supported.

**Version:** v2

---

## PQ-6 — Token replay conformance

**Decision:**  
Variant matching only.

**Version:** v1

---

## PQ-7 — Automatic process discovery

**Decision:**  
Manual process model declaration only.

**Version:** v1

---

## PQ-8 — Explicit case_id parameter

**Decision:**  
Allowed as optional override.

**Version:** v1

---

## PQ-9 — ACTIVITY_COUNT empty case behavior

**Decision:**  
Return `0`.

**Version:** v1

---

## PQ-10 — Incremental maintenance specification

**Decision:**  
Specify supported functions in spec.

Scheduling handled by ContextOps.

**Version:** v1

---

## PQ-11 — OCEL support

**Decision:**  
Flat event logs only.

**Version:** v1

---

## PQ-12 — THROUGHPUT_TIME_BETWEEN

**Decision:**  
Include in v1.

**Version:** v1

---

# CONTEXTOPS Decisions (Operations Layer)

## OPS-1 — Dependency version mismatch warning

**Decision:**
Emit informational warning `W013: Context B computed from older version of dependency A`.

**Rationale:**
Under snapshot isolation, stale dependency versions are possible but not harmful. A warning gives operators visibility without blocking queries.

**Version:** v1

---

## OPS-2 — Incremental join maintenance

**Decision:**  
Not supported.

**Version:** v1

---

## OPS-3 — Streaming integration

**Decision:**  
Embedded micro-batch processor.

External connectors in v2.

**Version:** v1

---

## OPS-4 — Entity key inference

**Decision:**  
Explicit `CONTEXT ON` binding required.

**Version:** v1

---

## OPS-5 — Many-to-many identity mapping

**Decision:**  
Supported with confidence scoring.

**Version:** v1

---

## OPS-6 — Composite identity keys

**Decision:**  
Serialize composite keys.

**Version:** v1

---

## OPS-7 — Multi-node storage architecture

**Decision:**  
Shared-storage model.

**Version:** v2

---

## OPS-8 — REMOTE refresh strategy

**Decision:**  
Time-based refresh.

**Version:** v1

---

## OPS-9 — Temporal identity mapping

**Decision:**  
Not supported.

**Version:** v2

---

## OPS-10 — Namespace hierarchy

**Decision:**  
Single-level namespaces.

**Version:** v1

---

## OPS-11 — MCP provider fan-out

**Decision:**  

```
max_mcp_providers_per_query = 10
```

**Version:** v1

---

## OPS-12 — Context version pinning

**Decision:**  
Not supported.

Snapshot isolation used instead.

**Version:** v1

---

## OPS-13 — MCP entity type mismatch

**Decision:**
Strict error `E060`.

**Rationale:**
Type safety in entity identity is critical for correctness. No implicit coercion. If a provider registered with `ENTITY_KEY_TYPE INT64` returns VARCHAR strings, the query fails immediately.

**Version:** v1

---

## OPS-14 — REMOTE schema evolution

**Decision:**  
Require re-registration.

**Version:** v1

---

# Architectural Decisions (Cross-Cutting)

## AD-1 — MCP vs REMOTE syntax separation

**Decision:**
Two distinct federated roles with separate syntax:

| Role | Syntax | Returns | Used In |
|------|--------|---------|---------|
| Context Provider | `MCP(provider)` | `(ENTITY_ID, SCORE?)` | `WHERE CONTEXT IN (...)` |
| Data Source | `REMOTE(provider.resource)` | relational table | `FROM` clause |

**Rationale:**
Allowing a single syntax to return either entity sets or rows would break the principle that syntax maps to unambiguous semantics. The engine would have to infer behavior from query position, increasing parser complexity and cognitive load. Keeping them separate preserves the context algebra as a pure entity-set operation.

**Source:** `brochureware_halfway_qa.md` — Decision 1

**Version:** v1

---

## AD-2 — Global Entity Namespace

**Decision:**
Three identity levels:

```
Level 1: Local entity keys (raw database keys, single-system scope)
Level 2: System-qualified keys (e.g., jira:ISSUE-1045, globally unambiguous)
Level 3: Global keys (mapped through entity_identity_map to canonical IDs)
```

**Rationale:**
Cross-system context composition requires unambiguous entity identity. System-qualified keys prevent collisions across systems. The identity map enables joining contexts from systems with different key types without ETL.

**Source:** `brochureware_halfway_qa.md` — Decision 3

**Version:** v1

---

## AD-3 — MATCHES predicate delegation

**Decision:**
The `MATCHES` predicate is adapter-delegated. Passed through to the execution adapter unchanged.

**Rationale:**
Pattern matching semantics differ across engines (regex flavors, LIKE syntax). Delegating avoids forcing a lowest-common-denominator implementation.

**Source:** `brochureware_halfway_qa.md`

**Version:** v1

---

## AD-4 — Multi-binding score aggregation

**Decision:**
Two-stage scoring model:

```
Stage 1 — Within each binding:
    Use FORMALIST context algebra rules (MAX for union, MIN for intersection, WEIGHTED_MAX for weighted)

Stage 2 — Across bindings:
    σ(e) = MIN(σ_b1(e), σ_b2(e), ..., σ_bn(e))
```

**Rationale:**
Multi-binding queries represent multiple independent operational constraints (e.g., late invoice AND risky vendor). The entity is only as urgent as the weakest signal across those dimensions. This aligns with fuzzy logic intersection: `μ(A ∧ B) = min(μ(A), μ(B))`.

**Source:** `brochureware_halfway_qa2.md`

**Version:** v1

---

## AD-5 — WEIGHTED_MAX as default scoring strategy

**Decision:**
Weighted composition uses:

```
score_weighted(e) = MAX(w_i × σ_ci(e))
                    for all ci where e ∈ ci.members
```

Default weight is `1.0`. `WEIGHTED_SUM` is available as an alternative via `ORDER BY CONTEXT USING WEIGHTED_SUM`.

**Rationale:**
Weighted SUM can produce misleading results where many weak contexts outweigh a strong one. Operational triage usually wants the strongest signal to dominate. WEIGHTED_MAX preserves that property while allowing weight modifiers to express relative importance.

**Source:** `brochureware_halfway_qa2.md`

**Version:** v1

---

## AD-6 — Identity map operating modes

**Decision:**
Two modes:

```
Simple mode (default):
    No cross-system identity resolution
    Levels 1 and 2 only (local keys, system-qualified keys)
    No entity_identity_map required

Federated mode (opt-in):
    Full cross-system identity resolution enabled
    entity_identity_map activated
    All three identity levels available
    Enabled via: SET contextql.identity_mode = 'federated'
```

**Rationale:**
The identity map is powerful but heavy for v1 adoption. Simple mode minimizes friction for single-database deployments. Organizations graduate to federated mode when they need cross-system context composition.

**Source:** `contextops_processminor_01.md` review

**Version:** v1

---

## AD-7 — ProcessTrace terminology

**Decision:**
The pre-computed case-level structure is named `ProcessTrace`, not `CaseTrace`.

```
ProcessTrace {
    case_id     : ENTITY_ID
    activities  : LIST<VARCHAR>
    timestamps  : LIST<TIMESTAMP>
    resources   : LIST<VARCHAR | NULL>
    start_time  : TIMESTAMP
    end_time    : TIMESTAMP
    event_count : INTEGER
}
```

**Rationale:**
Aligns with process mining literature where "trace" is the established term for a sequence of events belonging to a case.

**Source:** `contextops_processminor_01.md` — Recommendation 1

**Version:** v1

---

# GUARDIAN Decisions (Security, Governance & Compliance)

## GQ-1 — CREATE NAMESPACE privilege

**Decision:**
Required. Namespace creation is a privileged operation.

**Rationale:**
Namespaces define security and organizational boundaries for contexts. Uncontrolled namespace creation could lead to namespace squatting or organizational confusion.

**Version:** v1

---

## GQ-2 — SHOW CONTEXTS visibility filtering

**Decision:**
Hide inaccessible contexts from `SHOW CONTEXTS` output.

**Rationale:**
Users should not see metadata about contexts they cannot query. Prevents information leakage — even knowing that a `fraud_investigation` context exists could be sensitive.

**Version:** v1

---

## GQ-3 — GDPR cold storage erasure timing

**Decision:**
Batched erasure for cold storage (Parquet), not synchronous per-request.

**Rationale:**
Parquet files are immutable columnar storage — synchronous single-entity deletion is impractical. Batched rewrite is the standard approach. Must comply with GDPR Article 17 "without undue delay" — recommend maximum batch interval of 72 hours for cold tier.

**Version:** v1

---

## GQ-4 — Cross-tenant identity resolution

**Decision:**
Not supported.

**Rationale:**
Cross-tenant identity mapping introduces complex governance and data isolation concerns. Defer to v2 after multi-tenancy model is validated in production.

**Version:** v2

---

## GQ-5 — Context definition encryption at rest

**Decision:**
Not supported at application level. Rely on filesystem/volume encryption.

**Rationale:**
Production deployments use encrypted-at-rest filesystems (e.g., EBS encryption, LUKS). Application-level encryption adds complexity without significant security benefit for v1.

**Version:** v1

---

## GQ-6 — WEIGHT usage restrictions

**Decision:**
No restrictions. User-controlled.

**Rationale:**
Weights are part of the query algebra and affect ranking, not access control. Restricting them would add governance complexity without clear threat mitigation. Users can assign any weights in their queries.

**Version:** v1

---

## GQ-7 — Audit log query storage format

**Decision:**
Store parameterized template + parameters separately. Do not store full query text with interpolated values.

**Rationale:**
Prevents PII from leaking into audit logs through query literals. Aligns with GDPR data minimization (Article 5(1)(c)). Ad-hoc queries without bind parameters should be flagged for PII review.

**Version:** v1

---

## GQ-8 — Context version in audit logs

**Decision:**
Record the context snapshot version that was served in the audit log.

**Rationale:**
Enables post-hoc reproducibility of query results for compliance investigations. This is a read-only audit record — distinct from OPS-12 (no user-facing version pinning). The audit captures which snapshot was actually used, not a user-specified pin.

**Version:** v1

---

# DEVX Decisions (Developer Tooling & Experience)

## DX-1 — LLM dry-run mode

**Decision:**
Support dry-run mode where LLM-generated queries are validated but not executed.

**Rationale:**
Safety guardrail for LLM integration. Users can review generated queries before execution. Essential for building trust in the `from_natural_language()` API.

**Version:** v1

---

## DX-2 — LSP database connectivity

**Decision:**
Live database connection by default, with fallback to cached metadata when disconnected.

**Rationale:**
Live metadata provides accurate autocompletion (context names, member counts, column types). Cache fallback ensures the LSP remains functional during development without a live database.

**Version:** v1

---

## DX-3 — JDBC driver strategy

**Decision:**
Hybrid approach: context views (contexts exposed as virtual tables for BI tool compatibility) + query rewriting (full ContextQL syntax translated to adapter SQL).

**Rationale:**
BI tools (Tableau, Power BI, Looker) need table-like access via JDBC. Views provide that. Query rewriting supports full ContextQL syntax for programmatic access. Hybrid covers both use cases.

**Version:** v1

---

## DX-4 — API versioning

**Decision:**
API version is independent of the ContextQL language version.

**Rationale:**
Decouples SDK releases from language spec versions. Allows SDK bug fixes and improvements without implying language changes. Standard practice for language tooling.

**Version:** v1

---

## DX-5 — CLI watch mode

**Decision:**
Support `--watch` flag for auto-refreshing query results at a configurable interval.

**Rationale:**
Useful for operational monitoring and dashboarding. Aligns with the context freshness model — users can observe context membership changes in real time.

**Version:** v1

---

## DX-6 — Async Python API

**Decision:**
Provide async API (`contextql.aio`) alongside the synchronous API.

**Rationale:**
Modern Python applications increasingly use asyncio. Essential for production web services, batch pipelines, and notebook environments that benefit from non-blocking I/O.

**Version:** v1

---

## DX-7 — Native CLI testing framework

**Decision:**
Provide `cql test` as a built-in CLI command for running context validation tests.

**Rationale:**
Context definitions are logic — they need testing like code. A native test runner integrates with CI/CD pipelines and provides context-aware assertions (membership checks, score ranges, regression detection).

**Version:** v1

---

## DX-8 — Celonis PQL migration tooling

**Decision:**
Provide migration tooling for converting Celonis PQL queries to ContextQL.

**Rationale:**
Celonis PQL is the primary competitor in process-aware querying. Migration tooling lowers the adoption barrier for organizations already invested in Celonis. Strategic positioning move.

**Version:** v1

---

# Strategic Principles

The following principles guide all decisions in this document.

### v1 priorities

```
simplicity
deterministic semantics
minimal grammar surface
strong performance guarantees
```

### v2 expansion areas

```
OCEL event logs
advanced clustering
streaming connectors
process discovery
distributed execution
```

---

# Status

```
Total recorded decisions: 60
```

Future decisions should be appended to this document.

---

# Implementation Decisions (Parser & Tooling)

## IM-1 — Parser technology: Lark (Earley) instead of PEG

**Decision:**
Use Lark with Earley parser for v1 implementation.

**Rationale:**
The whitepaper specifies PEG parsing (Section 15.2). However, Lark provides a practical, well-maintained grammar-driven parser with good error messages, `.lark` grammar files, and position tracking out of the box. Earley handles the grammar's ambiguities (e.g., `STAR` as wildcard vs. multiplication, `THEN` in CASE vs. context chains) without requiring the grammar to be rewritten for LL/LR compatibility. This is a v1 pragmatic choice; migration to LALR (still Lark) or a hand-written parser is possible once the grammar stabilizes.

**Version:** v1 (may revisit for v2 performance)

---

## IM-2 — Error code scheme alignment

**Decision:**
Parser uses `E001`-`E099` for syntax errors. Linter uses `E100`-`E199` for semantic errors and `W001`-`W499` for warnings. This aligns with the whitepaper's Section 35 error taxonomy.

**Rationale:**
The original scaffold used `CTX001`-`CTX007` for lint rules. The whitepaper defines a comprehensive error code scheme. Aligning to the whitepaper scheme ensures consistency between specification and implementation, and reserves space for future error categories (E200+ runtime, E300+ federation, E400+ lifecycle).

**Version:** v1

---

## IM-3 — Public runtime alias: `ContextQL`

**Decision:**
`Engine` remains the implementation class name. `ContextQL` is added as a
public alias (`ContextQL = Engine`) exported from `contextql/__init__.py`.
`__all__` is explicitly defined for the first time.

**Rationale:**
The runtime was implemented under the name `Engine` for clarity during
development. Aliasing as `ContextQL` aligns the primary user-facing name
with the product brand without breaking any existing imports. An explicit
`__all__` makes the public surface unambiguous for tooling and documentation
generators.

**Version:** v1

---

# End of DECISIONS.md