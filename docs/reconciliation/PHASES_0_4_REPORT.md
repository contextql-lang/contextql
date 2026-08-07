# ContextQL Reconciliation — Phases 0–4 Report

## Executive outcome

The whitepaper and implementation have now been compared through a frozen, reproducible evidence model rather than an impressionistic document review.

The cycle produced:

- 13 source-authority rules;
- 753 atomic whitepaper claims covering all 43 numbered chapters;
- 43 core capabilities and 36 server capabilities;
- 27 targeted executable probes;
- fresh execution of 556 core tests and 109 server tests;
- a one-to-one traceability row for every claim;
- an independent adversarial review of all 50 claims initially marked high risk.

Phase 4 disposition is:

| Result | Claims | Interpretation |
|---|---:|---|
| Matched | 14 | Bounded claim is directly supported at the frozen pair. |
| Partial | 488 | Related capability exists but does not prove the whole claim or maturity. |
| Conflict | 77 | Reviewed evidence contradicts semantics, syntax, maturity, status, or public contract. |
| Unmatched | 174 | No direct reviewed evidence link was found; absence is not yet proven. |
| **Total** | **753** | |

The most important result is not the low matched count by itself. It is that the repository now distinguishes architecture, accepted intent, parsed surface, semantic model, executable behavior, durable control-plane behavior, tests, and operational evidence.

## Phase completion

### Phase 0 — Charter and frozen baseline: complete

The audit is pinned to:

- core `a054c8fcc576f3913d98d664ddf71eeea56d9755` from `origin/main`;
- server `78c9565c33237a21dbf87f11d92ac6c7f29a846e` from `main`.

Discovery Bank presentation commits and the pre-existing local presentation modification are excluded. The current pair is used only as an audit compatibility baseline.

Authority is claim-specific:

- current accepted decisions and specification establish intended semantics;
- grammar and generated API establish accepted surfaces;
- runtime code and executable probes establish observed behavior;
- tests establish only the behavior they assert;
- the whitepaper explains architecture;
- future plans do not become commitments without an explicit decision state.

### Phase 1 — Atomic claim corpus: complete

The extraction covers all 43 numbered whitepaper chapters with 753 stable content-derived claim IDs.

| Domain | Claims |
|---|---:|
| Language semantics | 175 |
| Product architecture | 148 |
| Storage and lifecycle | 99 |
| Process intelligence | 96 |
| Security and governance | 76 |
| Developer platform | 70 |
| Federation and identity | 52 |
| Execution and optimization | 37 |

Fifty claims were seeded as high risk because the source material already exposed a concrete conflict or especially strong public assurance. The corpus does not use the high-risk note alone as proof of a Phase 4 conflict.

### Phase 2 — Independent implementation inventories: complete

The core inventory contains 43 capabilities:

- 8 at M2 surface maturity;
- 6 at M3 executable maturity;
- 19 at M4 integrated maturity;
- 10 at M5 verified/hardened maturity.

The server inventory contains 36 capabilities:

- 2 at M1 design maturity;
- 3 at M2 surface/partial maturity;
- 20 at M4 integrated maturity;
- 11 at M5 verified/hardened maturity.

The inventories demonstrate that the implementation is both ahead of and behind the paper:

- executable context DDL, immutable snapshot identity, history, bitmap pushdown, SQLite durability, hardening, query bounds, and the DeepSee reference connector are materially ahead of the whitepaper's status section;
- process intelligence, full scoring semantics, composition, native parameters, SQL conformance, governed lifecycle, provider-registry activation, global identity, security, and distributed operations are behind the architectural claims.

### Phase 3 — Executable evidence: complete

#### Test evidence

- Core default run: 554 passed, 1 failed, 1 skipped from 556 tests.
- The failure was an import-order-sensitive optional-Polars test, not a product exception.
- Controlled core rerun with Polars imported before collection: 555 passed, 1 skipped.
- Server: 109 passed with one Starlette/httpx deprecation warning.

Both core outcomes are retained. The first reveals nondeterministic test logic; the controlled run establishes that no additional core test failed.

#### Behavioral probes

The 27 probes established, among other things:

- `SELECT 1;` does not pass the ContextQL grammar because `FROM` is required;
- malformed input is not recovered so a later valid statement can continue;
- whitepaper composite and process-model DDL examples conflict with current grammar forms;
- `EXPLAIN CONTEXT`, provider registration, `GRANT`, `CREATE NAMESPACE`, and `SET` do not have complete lower/execute paths;
- event-log and process-model models exist without public execution support;
- observed `THEN` behavior is indistinguishable from intersection;
- multi-statement input lowers to multiple statements but `Engine.execute` returns the first only;
- generated OpenAPI has 27 operations, is unversioned, and reports 0.3.0 while package metadata reports server 0.1.0 and core 0.2.0;
- the decision register contains 101 decision headings.

### Phase 4 — Claim-to-evidence join: complete

Every Phase 1 claim appears exactly once in the traceability matrix. The join references 77 of 79 implementation capabilities and 23 of 27 probes.

The join is conservative:

- matched and conflict rows require reviewed claim-specific evidence;
- curated section-level associations remain partial and low confidence;
- unmatched claims remain visible rather than being treated as absent features;
- no fuzzy or embedding similarity is used to manufacture confidence.

The regenerated matrix has SHA-256:

`7C9AD6368BBC711EB9B8E4D9A75139C04270DD173A191F38AC33C28DA906ED71`

## Adversarial high-risk outcome

The independent review consolidated the 50 seeded high-risk claims and reverse implementation gaps into 32 findings:

| Severity | Findings |
|---|---:|
| P0 | 1 |
| P1 | 27 |
| P2 | 4 |

Gap classifications are 9 semantic divergences, 7 partial vertical slices, 6 documentation lags, 5 cross-document conflicts, 2 test/evidence gaps, and one each of implementation lag, specification gap, and roadmap/status leakage.

### P0 — Unsupported security and compliance assurance

The whitepaper's production-ready/regulated-industry conclusion is not supported at the public server boundary:

- HTTP routes have no authentication or authorization dependency;
- `GRANT` is grammar-only;
- classification and namespaces are metadata rather than enforcement;
- RBAC, RLS, and tenant isolation are not implemented;
- the SQLite audit log is neither signed nor hash chained.

The reference security architecture may remain as design, but current operational assurance must be removed or explicitly qualified before publication.

### Principal P1 decision areas

1. Temporal-column filtering versus membership-history replay.
2. `THEN` staged/temporal semantics versus intersection behavior.
3. Scoring strategies, weighting, normalization, nulls, and negation.
4. `CONTEXT WINDOW` legality, ordering, and diagnostic code.
5. Four-state implementation versus nine-state governed lifecycle.
6. Error and warning code collisions across paper, specification, linter, and runtime.
7. Broad SQL:2016/pass-through claims versus the implemented grammar profile.
8. Parse-only/process/security DDL represented as implemented language.
9. Dry explain versus the server endpoint executing and returning rows.
10. Persistent provider metadata versus registry-driven runtime activation.
11. Exact path maps versus global/confidence-based identity resolution.
12. Unversioned HTTP routes and inconsistent package/application versions.
13. Broad storage, MVCC, O(1), and millisecond claims versus scoped evidence.
14. General streaming/micro-batching claims versus connector-specific synchronization.

## Code-only and under-documented capabilities

The evidence also prevents the review from becoming an indiscriminate downgrade. Strong bounded implementation exists for:

- executable context DDL and dependency/version handling;
- set and Roaring64 membership stores;
- copy-and-promote snapshots, checksums, history, retention, and restart hydration;
- bitmap membership pushdown and bounded REMOTE filtering;
- MCP/REMOTE provider contracts and traces;
- query/intermediate/response bounds;
- a persistent single-node server catalog;
- scheduler and failure recording;
- hardened DeepSee snapshot/change-feed reference behavior;
- SDK, builder, CLI, notebook, LSP, and diagnostics surfaces within their actual scope.

The two capability records without a direct whitepaper claim are multi-statement parsing and the server health endpoint. They are code-only candidates for later documentation or explicit non-contract status.

## Phase boundary

Phases 0–4 establish evidence and traceability. They do not decide which side should change.

No whitepaper architecture, normative specification, accepted decision, or product implementation has been changed in this cycle. Phase 5 must:

1. turn the P0 and P1 findings into decision packets;
2. check decision acceptance and supersession explicitly;
3. assign each conflict to paper repair, specification repair, implementation repair, relabeling, deferral, or retirement;
4. attach acceptance evidence before any traceability row moves to matched.

The recommended order is immediate public truth repair, normative semantic adjudication, server public-contract adjudication, then wider closure planning.
