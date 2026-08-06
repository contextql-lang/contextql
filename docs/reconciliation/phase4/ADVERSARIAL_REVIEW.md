# Phase 4 — Adversarial High-Risk Review

## Review outcome

This review challenges the strongest public and architectural claims at the frozen pair:

- core `a054c8fcc576f3913d98d664ddf71eeea56d9755`;
- server `78c9565c33237a21dbf87f11d92ac6c7f29a846e`.

It covers all 50 claims marked `high` risk in the Phase 1 corpus and adds reverse findings for material implementation behavior that those claims do not capture. The structured result is [`high_risk_findings.csv`](high_risk_findings.csv).

The review produced 32 findings:

| Severity | Count | Meaning here |
|---|---:|---|
| P0 | 1 | Current wording asserts production security/compliance controls that are absent at enforcement boundaries. |
| P1 | 27 | Public semantics, compatibility, lifecycle, API, implementation-status, or performance claims materially disagree with evidence. |
| P2 | 4 | Important evidence, status-labeling, compatibility-research, or CI gaps. |

Gap classifications are: 9 semantic divergences (`V`), 7 partial vertical slices (`P`), 6 documentation lags (`D`), 5 cross-document conflicts (`C`), 2 test/evidence gaps (`T`), 1 implementation lag (`I`), 1 specification gap (`S`), and 1 roadmap-orphan/status-leakage finding (`R`). Every Phase 1 high-risk claim ID appears in at least one finding; no claim was silently dropped.

This phase establishes facts and decision questions. It does not resolve product intent.

## Murder-board conclusions

### 1. The current security conclusion is unsafe

The whitepaper says ContextQL is production-ready for regulated industries because of privilege enforcement, classification propagation, row-level security, tenant isolation, and hash-chained audit logs. None of those controls exists at the public server boundary:

- HTTP routes have no authentication or authorization dependency;
- `GRANT` is grammar-only in core;
- namespaces and classification are metadata rather than enforcement;
- the SQLite audit log is mutable and neither signed nor hash chained;
- there is no tenant-isolation or RLS abuse suite.

This is finding `CQL-FND-HR-020`, the sole P0. It must be handled as truth repair before publication, not softened through vague implementation-status wording. The reference security architecture can remain, but operational assurance and compliance claims must be removed until controls and independent evidence exist.

### 2. Several central language operators have incompatible or absent semantics

The highest-risk semantic conflicts are not obscure edge cases:

- `AT`/`BETWEEN` means temporal-column filtering in the whitepaper and older decision, but membership-history replay in current SPEC/later decisions and the executor.
- `THEN` is documented as staged, candidate-scoped, left-associative evaluation, but executes as intersection.
- seven score strategies are accepted by grammar, while execution uses fixed aggregation behavior.
- score range, null, weighting, normalization, and negation do not form one executable contract.
- `CONTEXT WINDOW` legality, ordering, and warning code disagree across whitepaper, tooling, SPEC, and errors.

These require design-authority decisions before broad prose edits. Rewording alone cannot preserve compatibility when the same query can return different entities or scores.

### 3. The apparent language surface is substantially larger than the executable language

The grammar's top-level statement list is not an implementation matrix. Phase 3 proves the ladder breaks at different points:

| Surface | Parse | Dedicated model | Execute |
|---|---:|---:|---:|
| SELECT | Yes | Yes | Yes |
| EXPLAIN CONTEXT | Yes | No (`UNKNOWN`) | No |
| CREATE EVENT LOG | Yes | Yes | No |
| CREATE PROCESS MODEL | Yes | Yes | No public DDL path |
| REGISTER MCP PROVIDER | Yes | No (`UNKNOWN`) | No |
| GRANT | Yes | No (`UNKNOWN`) | No |
| CREATE NAMESPACE | Yes | No (`UNKNOWN`) | No |
| SET | Yes | No (`UNKNOWN`) | No |

Composite contexts and native context parameters fail later: they have semantic models and catalog representation but do not produce a complete query-execution path. These are partial vertical slices, not implemented language features.

The whitepaper's “27 statement types” is also stale: the current top-level grammar exposes 22 alternatives. Counts and capability status should be generated from a statement conformance matrix, not maintained by prose.

### 4. SQL compatibility is a profile, not transparent pass-through

Both the design-principles section and conformance declaration say standard SQL passes unchanged to the adapter. Phase 3's smallest counterexample, `SELECT 1;`, fails parsing because the ContextQL grammar requires `FROM`. Every statement must pass the Lark grammar and supported lowering path before DuckDB sees it.

The correct claim is likely that ContextQL offers a tested SQL-like/DuckDB-oriented subset plus ContextQL extensions. The exact profile remains a decision. Until conformance fixtures exist, “SQL:2016” and “passes through unchanged” are unsupported public-contract claims.

### 5. Storage and performance evidence is real but much narrower than the conclusion

The repository has meaningful post-whitepaper engineering:

- set and Roaring64 membership stores;
- immutable versions, atomic publication, history, retention, and checksum validation;
- bitmap relation pushdown;
- a committed, provenance-checked 10M post-trade benchmark scenario;
- strong single-process and SQLite failure-injection tests.

It does not have the claimed warm Arrow and cold Parquet tiers, automatic promotion/demotion, general cost optimizer, or general MVCC query-isolation evidence. Phase 3 did not regenerate the benchmark. O(1), sub-microsecond, and platform-wide millisecond wording must be restricted to exactly measured operations and environments.

### 6. Lifecycle and freshness are two conflicting contracts

The whitepaper promises a nine-state lifecycle and freshness states/codes W010, W012, and W013. The pinned product has:

- four stored states (`draft`, `validated`, `active`, `retired`);
- core CREATE initially setting `active`;
- server create immediately changing that value to `draft`;
- no query-time state gate—a focused probe queried a retired context successfully;
- `stale_after`, W100 for stale snapshot, and W101 for failed refresh;
- an optional server scheduler, not the documented strict/async/very-stale state policy.

Lifecycle cannot be described as governance until the state model, transitions, create behavior, query visibility, restart behavior, and freshness interaction are one tested contract.

### 7. The server is ahead of its README and behind its control-plane claims

Generated OpenAPI exposes 27 operations, including durable catalog, refresh, history, provider, identity, audit, and explain APIs. This is significantly more capable than the server README suggests. However:

- all routes are unversioned while the whitepaper specifies `/v1`;
- FastAPI/OpenAPI reports 0.3.0, package metadata/README report 0.1.0, and the paired core is 0.2.0;
- provider CRUD is not connected to runtime provider instantiation after restart;
- stored provider health is not an active health probe;
- identity confidence/matching mode are metadata only;
- context update accepts fields the service silently ignores;
- `/query/explain` executes the query and returns rows rather than returning a dry plan.

This is not evidence that the server is merely a thin HTTP wrapper. It is evidence of a credible single-node reference control plane whose public contracts have not caught up with its implementation boundaries.

### 8. Explicit future claims must be protected from present-tense design leakage

OCEL, LLM synthesis, multi-adapter/distributed execution, external streaming, and temporal identity are correctly identified as future or deferred in several high-risk claims. They are not implementation gaps solely because they are absent. The problem is that nearby detailed sections often use present-tense API and architectural wording.

The most direct contradiction is micro-batching: the paper says v1 includes an embedded one-second processor while its own status sections and accepted decision defer streaming. The DeepSee incremental synchronizer is useful connector-specific engineering, but it does not establish a general streaming engine.

Future material should carry an explicit decision state—committed, incubating, illustrative, or rejected—and must not appear in the current capability appendix.

## High-risk claim coverage

The 50 Phase 1 high-risk claims are covered as follows. Multiple claims are grouped only where they compete with the same evidence and require the same adjudication.

| Finding | High-risk claims covered | Review disposition |
|---|---:|---|
| HR-001 | 4 | Three-tier/performance/MVCC conclusion exceeds scoped evidence. |
| HR-002 | 1 | Grammar count/status is stale and conflates parse with implementation. |
| HR-003 | 1 | Error recovery is not present. |
| HR-004 | 1 | Linter count is stale; generate it. |
| HR-005 | 13 | Named components exist, but one “Implemented” label hides sharply different maturity. |
| HR-006 | 1 | Type definitions are unintegrated/duplicated. |
| HR-007 | 2 | Transparent standard-SQL pass-through is false. |
| HR-008 | 1 | Polars/Arrow are outputs, not execution adapters. |
| HR-009 | 1 | Score warning code conflicts. |
| HR-010 | 2 | Score type/range/null semantics lack one contract. |
| HR-011 | 2 | Temporal-column versus membership-history semantics conflict. |
| HR-012 | 1 | CONTEXT_SCORE scope code conflicts. |
| HR-013 | 1 | Window warning code/legality/order conflict. |
| HR-014 | 1 | DEVIATION_SCORE contract lacks a process runtime. |
| HR-015 | 3 | OCEL deferral is aligned; forward-compatibility is unproved. |
| HR-016 | 3 | Nine-state lifecycle claims conflict with four-state ungated behavior. |
| HR-017 | 4 | Freshness states and warning codes conflict. |
| HR-018 | 6 | Current v1 micro-batch claims contradict deferral and implementation. |
| HR-019 | 2 | Future labeling is correct but needs decision-state isolation. |
| **Total** | **50** | **All high-risk claim IDs are linked in the CSV.** |

Findings HR-020 through HR-032 come from the required reverse review of major core/server divergence and include claims outside the original high-risk set where available.

## Required decision docket

The following order minimizes the risk of editing one document around an undecided contract.

### Immediate truth repair

1. Remove or explicitly qualify regulated-industry readiness, enforced security controls, hash-chained audit, tenant isolation, and RLS claims (`HR-020`).
2. Replace transparent SQL:2016/pass-through language with an explicitly unverified profile pending fixtures (`HR-007`).
3. Scope three-tier, MVCC, O(1), sub-microsecond, and millisecond claims to actual measured/present behavior (`HR-001`).
4. Mark micro-batching as deferred and distinguish connector synchronization (`HR-018`).
5. Replace the undifferentiated implementation list with capability/maturity status (`HR-002`–`HR-006`).

These actions repair factual public status without choosing new language semantics.

### Normative adjudication before code or architectural prose

1. Temporal column filtering versus membership-history replay (`HR-011`).
2. `THEN` staged/candidate/temporal semantics (`HR-028`).
3. Score aggregation, range, normalization, null, negation, and strategy formulas (`HR-010`, `HR-029`).
4. `CONTEXT WINDOW` legality and ordering (`HR-013`).
5. Lifecycle states, transitions, create semantics, query visibility, and freshness model (`HR-016`, `HR-017`).
6. Error/warning code ownership and supersession (`HR-009`, `HR-012`, `HR-013`, `HR-017`).
7. Native composition and parameter semantics (`HR-027`).
8. Reserved versus v1 executable DDL surface (`HR-030`).
9. Dry plan versus executing trace endpoint (`HR-021`).

### Server public-contract adjudication

1. Server version and paired compatibility identity (`HR-024`).
2. Route versioning and stability policy (`HR-032`).
3. Provider registry activation and health semantics (`HR-022`).
4. Exact-path mapping versus global identity resolution (`HR-023`).
5. Mutable context fields and explicit rejection of unsupported updates (`HR-025`).
6. Multi-statement rejection/execution/result contract (`HR-026`).

## Minimum next evidence

No P1 capability should be called aligned merely because the current tests pass. The next evidence should be selected to falsify the strongest claim cheaply:

- a generated statement ladder for every grammar top-level alternative;
- a SQL dialect conformance suite;
- golden temporal, `THEN`, scoring, and window result fixtures;
- a lifecycle transition/query-visibility matrix across core, HTTP, and restart;
- no-side-effect explain spies on adapters and providers;
- configured-provider restart-to-query integration tests;
- one persisted-and-engine-observed test for every API request field;
- authenticated cross-tenant, RLS, audit-tamper, and privilege-denial abuse cases before any production security claim;
- a clean full-suite matrix with and without every optional dependency;
- benchmark regeneration with raw artifacts and concurrent refresh/read correctness checks;
- OpenAPI compatibility diff and version-consistency checks in CI.

## No-action and correctly bounded observations

Adversarial review should also record where evidence supports bounded claims:

- the diagnostic renderer, error registry, semantic lowerer, DuckDB adapter, synchronous SDK, QueryBuilder, narrow CLI, LSP helpers, and provider protocols all exist;
- the core and server test suites provide strong evidence for the specific behaviors they assert, despite one core import-order test defect;
- SQLite snapshot publication, checksum validation, last-good preservation, restart hydration, and DeepSee synchronization have credible M5 reference evidence;
- OCEL, LLM synthesis, multi-adapter/distributed execution, external connectors, and temporal identity are not defects when retained as explicit future work;
- the server has materially advanced beyond its README in durable catalog and runtime-state behavior.

These observations prevent the reconciliation from becoming indiscriminate downgrading. The issue is claim scope and maturity, not the absence of meaningful implementation.

## Phase 4 exit assessment

The evidence is sufficient to begin adjudication and closure planning, but not sufficient to publish a reconciled whitepaper. One P0 and 27 P1 findings remain open. In particular, security, SQL compatibility, temporal semantics, lifecycle, scoring, and explain safety require explicit human decisions or immediate truth repair.

The next integration step should merge these findings with the Phase 4 traceability join, deduplicate by capability/claim ID, and create decision packets. Product code and architectural prose should not be changed to imply resolution until those decisions are recorded.
