# Phase 5 — Gap Classification and Decision Preparation

## Outcome

Phase 5 reduces Phase 4's claim-level evidence into 30 stable, decision-sized material gaps. It assigns severity, confidence, current maturity, affected dimensions, closure streams, recommended disposition, dependencies, and acceptance evidence.

The result preserves exact coverage of:

- all 77 Phase 4 conflict claims;
- all 32 adversarial findings;
- all 15 high-confidence partial claims;
- reverse implementation findings that have no whitepaper claim ID.

No product behavior, normative specification, decision entry, or whitepaper prose was changed.

## Gap profile

### Severity

| Severity | Gaps |
|---|---:|
| P0 | 1 |
| P1 | 25 |
| P2 | 4 |
| **Total** | **30** |

### Classification

| Classification | Gaps |
|---|---:|
| Semantic divergence (`V`) | 9 |
| Documentation lag (`D`) | 6 |
| Partial vertical slice (`P`) | 6 |
| Cross-document conflict (`C`) | 4 |
| Test/evidence gap (`T`) | 2 |
| Implementation lag (`I`) | 1 |
| Specification gap (`S`) | 1 |
| Roadmap/status leakage (`R`) | 1 |

Twenty-five gaps require a design-authority decision. Five can proceed as factual or evidence repair without selecting new semantics.

### Closure streams

A material gap may require more than one stream:

| Closure stream | Gaps |
|---|---:|
| Contract repair | 26 |
| Hardening | 23 |
| Vertical-slice completion | 14 |
| Truth repair | 14 |

These are closure requirements, not automatic commitments to build every reference-architecture feature.

## Normalization discipline

The gap register uses one row per disposition unit rather than one row per stale sentence. Claims are combined only when one decision and acceptance suite can close them together.

Examples:

- temporal semantics are one gap because OQ-9 and CS-16/CS-22 cannot remain concurrently active for the same syntax;
- diagnostic code ownership is separate from scoring formulas because compatibility and closure evidence differ;
- explain execution safety is separate from general HTTP versioning;
- provider registration is joined to runtime activation/health rather than generic parse-only DDL;
- process-function runtime is separate from process/event DDL persistence.

All IDs are validated against Phases 1–4. Two generator runs produced byte-identical output with SHA-256:

`905B09C3F2729B296F27C8FCD2070A2839D0AE694628DB9CAAE4D9E1614CC883`

## Factual truth-repair queue

Eighteen bounded repairs are recommended independently of future design choices:

- 1 P0 public-integrity repair;
- 16 P1 public-contract/status repairs;
- 1 P2 evidence repair.

The P0 requires removing or qualifying current production-ready security/compliance assurance. The architecture may remain as future design, but the pinned server has no authentication, RBAC, RLS, tenant enforcement, or tamper-evident audit chain.

Other factual repairs include:

- maturity-aware implementation status;
- generated grammar/linter/decision/test counts;
- parser error-reporting rather than recovery;
- the unintegrated type system;
- DuckDB execution versus Polars/Arrow output roles;
- an explicit tested SQL-subset profile instead of pass-through/SQL:2016 assurance;
- bounded benchmark/storage wording;
- process runtime status;
- current server routes and reference-control-plane scope;
- package/application version inconsistency;
- generic streaming versus connector synchronization;
- executing explain behavior;
- provider and identity registry boundaries;
- optional-dependency test nondeterminism.

These repairs disclose current truth. They do not decide the target design named alongside each row.

## Decision dockets

Twenty-one packets prepare the design-authority handoff.

### Language and semantics — 10 packets

| Packet | Topic | Specialist recommendation |
|---|---|---|
| `CQL-P5-LANG-001` | Authority and supersession | Add explicit decision states and replacement links; keep intended authority separate from behavioral evidence. |
| `CQL-P5-LANG-002` | `AT` / `BETWEEN` | Use membership-history replay as current meaning and explicitly supersede OQ-9, or introduce distinct syntax. |
| `CQL-P5-LANG-003` | `THEN` | Reserve for candidate-scoped staged evaluation; use a distinct process/temporal construct for ordered events. |
| `CQL-P5-LANG-004` | Score algebra | Approve one algebra, keep negation unscored, warn rather than clamp, and expose only evidenced strategies. |
| `CQL-P5-LANG-005` | `CONTEXT WINDOW` | Retain legal-with-W001 plus configurable error, with deterministic pre-predicate truncation. |
| `CQL-P5-LANG-006` | Diagnostics | Make a generated registry canonical and explicitly supersede conflicting meanings. |
| `CQL-P5-LANG-007` | DDL maturity | Publish a parse/lower/analyze/execute/persist profile; label non-executable forms reserved. |
| `CQL-P5-LANG-008` | SQL profile | Replace pass-through/SQL:2016 claims with a tested ContextQL SQL-subset profile. |
| `CQL-P5-LANG-009` | Parameters/composition | Keep syntax provisional at M2 until binding and materialization work end to end. |
| `CQL-P5-LANG-010` | Multi-statement input | Make `Engine.execute` single-statement and reject extras; design a separate script API if needed. |

### Platform, operations, and security — 11 packets

| Packet | Topic | Specialist recommendation |
|---|---|---|
| `CQL-P5-PLAT-001` | Security/compliance truth | Immediate factual truth repair; retain architecture as future design. |
| `CQL-P5-PLAT-002` | Lifecycle/freshness | Four durable governance states with orthogonal runtime status; active-only ordinary query gating. |
| `CQL-P5-PLAT-003` | Explain safety | Make explain dry; expose executing trace through an explicitly named bounded endpoint. |
| `CQL-P5-PLAT-004` | Provider activation/health | Registry-driven allowlisted activation, active probes, desired/observed state, fail-closed routing. |
| `CQL-P5-PLAT-005` | Identity scope | Call current behavior deterministic identity binding; defer global/confidence resolution. |
| `CQL-P5-PLAT-006` | API/release versions | Separate server, HTTP-major, core, and language/spec versions; publish a compatibility matrix. |
| `CQL-P5-PLAT-007` | Context updates | Reject currently ignored fields until atomic semantics exist. |
| `CQL-P5-PLAT-008` | Storage/isolation/performance | Bound claims to reference implementations and reproducible scenarios. |
| `CQL-P5-PLAT-009` | Streaming | Do not equate one connector synchronizer with generic streaming. |
| `CQL-P5-PLAT-010` | Audit integrity | Call current data operational event history; design a separately verified integrity profile. |
| `CQL-P5-PLAT-011` | Server maturity | Document the implemented single-node reference control plane and explicit non-capabilities. |

All semantic/compatibility/product recommendations remain awaiting decision. Two platform packets are marked factual truth repair because they only correct current-release statements.

## Recommended decision order

The dockets should be decided in this sequence to minimize rework:

1. Authority, decision status, and supersession metadata.
2. Immediate P0 security/compliance truth repair.
3. Temporal, `THEN`, scoring, window, and diagnostic semantics.
4. Lifecycle/freshness/query gating and explain safety.
5. Executable/reserved DDL and SQL/multi-statement contracts.
6. Provider activation, identity scope, and context-update semantics.
7. API/version compatibility.
8. Storage/performance, streaming, and audit-integrity scope.
9. Process intelligence, native parameters/composition, and other vertical-slice commitments.

Choosing an option does not close a gap. Closure also requires the packet's specified conformance, migration, negative, restart, security, or benchmark evidence.

## Phase 5 exit assessment

Phase 5 classification is complete:

- every material Phase 4 conflict has one disposition home;
- every adversarial finding is preserved;
- strong partial evidence is not omitted;
- maturity, severity, confidence, dependencies, and closure streams are explicit;
- factual repairs are separated from owner decisions;
- decision packets state options, recommendations, consequences, and minimum evidence;
- deterministic generation and cross-reference validation are available.

The next methodology phase is adversarial decision review and design-authority adjudication. No traceability claim should move to matched until the corresponding decision is recorded and its acceptance evidence passes.
