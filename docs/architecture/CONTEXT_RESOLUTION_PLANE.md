# The Context Resolution Plane

**Architecture Vision for ContextQL v0.3 → v2**

Copyright (c) 2026 Anton du Plessis

---

## 1. The Problem

Modern operational analytics stacks have a well-understood layered architecture: raw data at the bottom, semantic layers in the middle (dbt, Cube, Looker), and applications at the top. This stack standardizes the **meaning** of data — what "revenue" means, how "churn" is calculated, which table holds "customers."

But semantic layers answer a different question than operational systems need:

- A semantic layer tells you: *"amount means invoices.amount_usd, after FX conversion."*
- An operational system asks: *"Which invoices are overdue, disputed, AND flagged by the fraud model — and how urgent is each one?"*

The second question requires **context resolution** — determining which operational situations apply to which entities, from which evidence, with what scores, across which systems. No semantic layer provides this. The result is that every team builds ad-hoc context resolution in application code: scattered WHERE clauses, manual joins to risk tables, hardcoded thresholds, duplicated business logic.

## 2. The Context Resolution Layer

ContextQL introduces a dedicated **context resolution layer** that sits alongside the semantic layer:

```
┌───────────────────────────────────────────────────────────────────┐
│  APPLICATIONS / DASHBOARDS / LLM AGENTS / WORKFLOWS              │
│  "Show me the 20 most urgent invoices across all risk factors"   │
├───────────────────────────────────────────────────────────────────┤
│  CONTEXT RESOLUTION LAYER                                         │
│                                                                   │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │
│  │  Context     │  │  Context     │  │  Federation              │ │
│  │  Algebra     │  │  Lifecycle   │  │  MCP: membership + score │ │
│  │  union       │  │  9 states    │  │  REMOTE: relational data │ │
│  │  intersect   │  │  refresh     │  │  Identity: cross-system  │ │
│  │  negate      │  │  schedule    │  │  resolution              │ │
│  │  WEIGHT      │  │  govern      │  │                          │ │
│  │  THEN        │  │              │  │                          │ │
│  └─────────────┘  └──────────────┘  └──────────────────────────┘ │
├───────────────────────────────────────────────────────────────────┤
│  SEMANTIC LAYER          │  DATA LAYER                            │
│  Meaning standardization │  Tables · Events · Streams             │
│  (dbt, Cube, Looker)     │  (DuckDB, PostgreSQL, Polars, etc.)    │
└───────────────────────────────────────────────────────────────────┘
```

The context resolution layer does not replace the semantic layer. It resolves a different concern: **which situations apply to which entities right now, with what urgency, from what evidence.**

## 3. Key Architectural Distinction

| Concern | Semantic Layer | Context Resolution Layer |
|---------|---------------|--------------------------|
| Question answered | "What does this metric mean?" | "What situations apply to this entity?" |
| Primary abstraction | Metrics, dimensions, relationships | Contexts (named operational situations) |
| Composition model | SQL joins, aggregations | Formal algebra (union, intersect, WEIGHT, THEN) |
| Scoring | No native support | `CONTEXT_SCORE()`, `ORDER BY CONTEXT` |
| Cross-system | Single warehouse | MCP (membership) + REMOTE (data) federation |
| Lifecycle | Schema versioning | 9-state governance (draft → materialized → retired) |
| Identity | Schema-level foreign keys | Global Entity Namespace (3-level identity) |
| Freshness | Batch refresh | Context-aware staleness tracking |
| Output | Tables, views | Ranked entity sets with provenance |

## 4. Two-Component Architecture

ContextQL is implemented as two components with distinct responsibilities:

### contextql — The Context Resolution Engine

The engine is the algebra, execution, and scoring runtime. It:

- Parses ContextQL queries (Lark grammar, Earley parser)
- Performs semantic analysis (type checking, context validation)
- Resolves contexts to entity sets (membership + scores)
- Composes contexts using formal algebra (union, intersect, negate, WEIGHT, THEN)
- Ranks entities by composite context score
- Federates with MCP providers (membership bitmasks) and REMOTE providers (relational data)
- Bridges entity identities across tables via identity maps

The engine is embeddable. It runs in-process as a Python library, in a Jupyter notebook, or behind the server.

### contextql-server — The Context Resolution Plane

The server is the operational control plane. Its responsibilities (current and planned):

| Responsibility | Status | Description |
|---------------|--------|-------------|
| Query execution | **Implemented** | HTTP interface to the engine (`POST /query`) |
| Health monitoring | **Implemented** | Engine status and catalog introspection (`GET /health`) |
| Provider registration | **Implemented** | Built-in + mock MCP/REMOTE providers |
| Context catalog | **Planned (v0.3)** | Persistent registry for context definitions |
| Federation broker | **Planned (v2)** | Distributed routing to MCP/REMOTE targets |
| Identity resolution | **Planned (v2)** | Global Entity Namespace service |
| Lifecycle management | **Planned (v2)** | Background refresh scheduling, staleness tracking |
| Multi-tenant governance | **Planned (v2)** | RBAC, RLS, audit trail, classification |

## 5. Federation Model: Membership vs. Data

The core federation insight: **context resolution requires membership evidence, not data movement.**

ContextQL separates two federated concerns:

```
┌──────────────────┐              ┌──────────────────┐
│  MCP Provider    │              │  REMOTE Provider  │
│                  │              │                   │
│  Returns:        │              │  Returns:         │
│  - Entity IDs    │              │  - Full rows      │
│  - Scores        │              │  - Columns        │
│  - data_as_of    │              │  - Relational     │
│                  │              │    data            │
│  Example:        │              │  Example:         │
│  Fraud model     │              │  Jira issues      │
│  Risk scorer     │              │  External CRM     │
│  ML classifier   │              │  SAP records      │
└────────┬─────────┘              └────────┬──────────┘
         │ entity_ids + scores              │ DataFrame
         v                                  v
┌──────────────────────────────────────────────────────┐
│                 CONTEXT RESOLUTION ENGINE             │
│                                                      │
│  1. Resolve MCP → entity membership bitmask          │
│  2. Materialize REMOTE → temp table in DuckDB        │
│  3. Execute base SQL (FROM/JOIN/WHERE)               │
│  4. Apply context filter (bitmap probe)              │
│  5. Score + rank (CONTEXT_SCORE, ORDER BY CONTEXT)   │
│  6. Project final columns                            │
└──────────────────────────────────────────────────────┘
```

MCP providers vote on **which entities belong** to a situation. REMOTE providers supply **relational data for enrichment**. The engine combines both: MCP narrows the result set, REMOTE enriches it. Minimal data moves.

## 6. Identity Resolution

Contexts often span entity boundaries: a fraud model scores vendors, but the query is over invoices. ContextQL resolves this via identity maps:

```
Level 1: LOCAL KEYS (default)
  invoice_id = 42, vendor_id = 7

Level 2: SYSTEM KEYS (federated mode)
  erp:INV-42, jira:ISSUE-1045

Level 3: GLOBAL KEYS (planned, v2)
  customer:C-1001 → {erp:CUST-1001, jira:ORG-ACME}
```

Currently implemented: Level 1 with explicit identity maps (`register_identity_map()`). The engine uses these to bridge entity keys across tables during context resolution.

## 7. Specialist Agent Architecture

The design of ContextQL is guided by 9 specialist agents, each responsible for a distinct architectural domain:

| Agent | Domain | Drives |
|-------|--------|--------|
| FORMALIST | Grammar, types, algebra | Language specification |
| ENGINE | Execution pipeline, storage | Runtime behavior |
| PROCESSMINOR | Event logs, process functions | Process intelligence |
| CONTEXTOPS | Lifecycle, federation, identity | Operational backbone |
| GUARDIAN | Security, governance, compliance | Enterprise readiness |
| DEVX | CLI, SDK, LSP, diagnostics | Developer experience |
| ARCHIVIST | Documentation accuracy | Documentation quality |
| GLOSSARIST | Technical glossary | Accessibility |
| VISUALIST | Diagrams, visual language | Communication |

Agent specifications live in `agents/specs/`. Their design outputs live in `agents/drafts/`. These documents are the authoritative source for architectural decisions beyond what the whitepaper covers.

## 8. Implementation Roadmap

### v0.2 (current)
- Full grammar (27 statement types, ~480 lines)
- Hybrid executor (DuckDB base SQL + Python context algebra)
- MCP/REMOTE federation runtime (in-process providers)
- Identity maps (Level 1)
- 361 tests, LSP, CLI, Jupyter magic, VS Code extension

### v0.3 (in progress)
- Strategic reframing as context resolution layer
- `EXPLAIN CONTEXT` for execution tracing and provenance
- Persistent context catalog (SQLite-backed, on server)
- `REGISTER PROVIDER` grammar execution
- Federation integration tests (MCP membership + REMOTE data)
- MCP/REMOTE protocol specification document

### v2 (planned)
- Federation broker (distributed MCP/REMOTE routing, circuit breaker)
- Global Entity Namespace (3-level identity, confidence scoring)
- Context lifecycle manager (9-state machine, background refresh)
- Multi-tenant governance (18 privileges, RBAC, RLS, audit)
- Roaring bitmap storage (hot/warm/cold tiers, MVCC)
- Rust acceleration via PyO3 (bitmap ops, scoring, ranking)
- Process intelligence functions (9 functions, conformance checking)
- Streaming context updates

## 9. Design Principles

1. **SQL-first** — ContextQL extends SQL; it does not replace it. Every ContextQL query is recognizable to a SQL practitioner.

2. **Contexts are objects, not queries** — A context has a name, a lifecycle, scores, governance, and federation provenance. It is not a view or a CTE.

3. **Federation over integration** — Resolve context across systems without moving data. MCP for membership, REMOTE for enrichment.

4. **Algebra over ad-hoc** — Context composition uses a formal algebra (union, intersect, negate, WEIGHT, THEN) with defined semantics, not string concatenation.

5. **Progressive adoption** — Every feature is usable independently. Start with `WHERE CONTEXT IN (...)`, add scoring later, add federation later, add governance later.

---

*This document describes the architectural vision for ContextQL. For the formal language specification, see [SPEC.md](../../SPEC.md). For the comprehensive design rationale, see [WHITEPAPER.md](../../WHITEPAPER.md). For implementation decisions, see [DECISIONS.md](../../DECISIONS.md).*
