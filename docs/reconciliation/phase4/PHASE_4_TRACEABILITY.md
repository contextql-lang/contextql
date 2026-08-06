# Phase 4 — Claim-to-Evidence Traceability

## Outcome

Phase 4 joins all **753 whitepaper claims** to the Phase 2 implementation
inventories, the Phase 3 executable probes, and the intent/authority references
seeded in Phase 1.

The machine-readable result is [`traceability.csv`](traceability.csv). It has
exactly one row for every Phase 1 claim and preserves all claims for which no
implementation evidence was found.

| Join status | Claims | Meaning |
|---|---:|---|
| `matched` | 14 | Individually reviewed evidence supports the same bounded claim at the pinned baselines. |
| `partial` | 488 | Related implementation exists, but the evidence covers only part of the claim, a weaker maturity, or an adjacent layer. |
| `conflict` | 77 | Individually reviewed implementation/probe evidence contradicts the claim, its maturity label, syntax, code assignment, or stated semantics. |
| `unmatched` | 174 | No reviewed implementation capability or probe directly addresses the claim. This is not proof of absence. |
| **Total** | **753** | |

The deliberately small `matched` population reflects the evidence rule: a
shared section or term cannot establish semantic equivalence.

## Frozen evidence inputs

| Input | Records | Role |
|---|---:|---|
| `phase1/claims.csv` | 753 | Atomic claims, provenance, initial SPEC/DECISIONS/document references, and risk flags. |
| `phase2/core_capabilities.csv` | 43 | Reachable core surfaces, maturity, positive evidence, and negative evidence. |
| `phase2/server_capabilities.csv` | 36 | Reachable server/API/persistence/connector surfaces and explicit absences. |
| `phase3/probes.json` | 27 | Claim-targeted observations at the frozen core/server pair. |
| `source_authority.csv` | 13 authority rules | Determines intended authority and conflict disposition by claim type. |

The implementation evidence is pinned to core
`a054c8fcc576f3913d98d664ddf71eeea56d9755` and server
`78c9565c33237a21dbf87f11d92ac6c7f29a846e`, as recorded by Phase 3.

## Output schema

Each traceability row contains:

- the original `claim_id`, source path/lines, section, atomic claim, class, and
  domain;
- zero or more `capability_ids` from the two Phase 2 inventories;
- zero or more `probe_ids` from Phase 3;
- `intended_authority_reference`, combining the applicable `AUTH-*` rule with
  the claim's existing SPEC/DECISIONS/README/architecture references;
- `join_status` and `evidence_confidence`;
- `match_method`, which states whether the link came from an exact reviewed rule,
  a curated section scope, a reviewed glossary restatement, or explicit lack of
  evidence; and
- notes explaining evidence boundaries and preserving every Phase 1 drift flag.

## Join method and safeguards

The reproducible generator is
[`../tools/join_claims.py`](../tools/join_claims.py). From the repository root:

```powershell
python docs/reconciliation/tools/join_claims.py
```

The implementation uses four evidence levels:

1. **Reviewed exact status lines.** The 17 “Implemented” inventory rows and key
   maturity rows are mapped individually to capability IDs. These can become
   matched or conflict with high confidence.
2. **Reviewed claim rules.** A bounded pattern plus a reviewed source-line scope
   connects particular claims to relevant negative evidence and probes. Examples
   include the two pass-through claims, warning codes, temporal semantics, DDL
   forms, process functions, lifecycle states, security enforcement, and REST
   versioning.
3. **Curated section scopes.** Specialists selected the capability families that
   can bear on each whitepaper section. These links remain `partial` with low
   confidence; they never become semantic matches merely because the subject is
   similar.
4. **Reviewed glossary links.** Only an explicit term dictionary links appendix
   restatements to capabilities. These are partial/low-confidence references,
   not semantic equality.

There is no fuzzy matching, token similarity, embedding similarity, or
keyword-derived confidence score. Phase 1 conflict notes are retained but cannot
create a Phase 4 conflict without Phase 2 or Phase 3 evidence.

The script also fails if a rule references an unknown capability/probe ID, or if
the output is not a one-to-one join with the claim corpus.

## Coverage

### Status by domain

| Domain | Total | Matched | Partial | Conflict | Unmatched |
|---|---:|---:|---:|---:|---:|
| Developer platform | 70 | 5 | 45 | 8 | 12 |
| Execution and optimization | 37 | 2 | 28 | 1 | 6 |
| Federation and identity | 52 | 1 | 37 | 2 | 12 |
| Language semantics | 175 | 1 | 141 | 10 | 23 |
| Process intelligence | 96 | 1 | 41 | 22 | 32 |
| Product architecture | 148 | 2 | 84 | 14 | 48 |
| Security and governance | 76 | 2 | 56 | 6 | 12 |
| Storage and lifecycle | 99 | 0 | 56 | 14 | 29 |
| **Total** | **753** | **14** | **488** | **77** | **174** |

### Confidence

| Disposition and confidence | Claims |
|---|---:|
| Conflict, high | 75 |
| Conflict, medium | 2 |
| Matched, high | 13 |
| Matched, medium | 1 |
| Partial, high | 15 |
| Partial, medium | 2 |
| Partial, low | 471 |
| Unmatched, none | 174 |

Additional coverage checks:

- 579 claims have at least one capability or probe association.
- 31 claims are linked to executable probes.
- 77 of 79 implementation capabilities are linked to at least one whitepaper
  claim.
- 23 of 27 probes are linked to at least one whitepaper claim.
- 361 rows carry an initial SPEC/DECISIONS/README/architecture cross-reference
  from Phase 1; all rows carry an `AUTH-*` authority route.

The two implementation capabilities without a corresponding whitepaper claim
are multi-statement parsing (`CQL-CORE-QRY-002`) and the server health endpoint
(`CQL-SRV-API-001`). These are candidate **code-only capabilities**, not join
failures. The four unlinked probes concern `EXPLAIN CONTEXT` execution,
multi-statement lowering/execution, and decision-count metadata; none has a
direct atomic whitepaper claim in the current corpus.

## High-risk joins

### Normative semantics and diagnostics

| Claim(s) | Evidence | Disposition |
|---|---|---|
| `CQL-WP-7D5BDE108A84`, `CQL-WP-63B55DC68187` — temporal qualifiers filter source timestamp columns | `CQL-CORE-HIS-001`; current SPEC/CS decisions | **Conflict, high.** Verified runtime resolves `AT`/`BETWEEN` against recorded membership history. |
| `CQL-WP-9A51E8EA195F` — score-range warning is W100 | `CQL-CORE-ERR-001`, `CQL-CORE-LNT-001` | **Conflict, high.** Current registry uses W003; W100 means stale snapshot. |
| `CQL-WP-F6B02C9B10FA` — scoreless window warning is W101 | `CQL-CORE-WIN-001`, `CQL-CORE-ERR-001` | **Conflict, high.** Current registry uses W001; W101 means failed refresh. |
| `CQL-WP-945AA0A449F8` — score/count scope failure is E111 | `CQL-CORE-ERR-001`, `CQL-CORE-LNT-001` | **Conflict, high.** Current scope error is E108; E111 is score-expression type failure. |
| `CQL-WP-FFCA3426B13A`, `CQL-WP-1A867374DEE3`, `CQL-WP-3FCC4C832E5F`, `CQL-WP-9B44A1743052` — W010/W012/W013 freshness/dependency warnings | registry, snapshots, dependencies | **Conflict, high.** W010/W012 are absent; W013 remains decision-only. |

### Language surface and execution reachability

| Claim(s) | Evidence | Disposition |
|---|---|---|
| `CQL-WP-A6BF8026647E` — parser with error recovery | `CQL-CORE-PAR-001`, `P3-PAR-001` | **Conflict, high.** Malformed input aborts instead of recovering to a later statement. |
| `CQL-WP-9E08C9D03070`, `CQL-WP-46D55A59C26F` — unchanged standard-SQL pass-through | parser/query/adapter capabilities, `P3-SQL-001` | **Conflict, high.** `SELECT 1` fails because parsing requires `FROM`, before DuckDB can receive it. |
| `CQL-WP-E2801958841C` — composite DDL without `ON` | composition/DDL capabilities, `P3-DDL-001/002` | **Conflict, high.** Whitepaper form fails; the current grammar/spec form with `ON` parses. |
| `CQL-WP-9A9FF97A9B5A` and Chapter 14 process-model claims | process capability and `P3-PRO/LAD/EXE` probes | **Conflict, high.** Current syntax uses `EXPECTED PATH`; process-model execution is not established. |
| `CQL-WP-CFB95979A8CF` and REMOTE registration form | provider capabilities and `P3-LAD-015/P3-EXE-016` | **Partial, high.** Registration parses, lowers to `UNKNOWN`, does not execute, and durable provider records are not activated. |

### Maturity, operations, and platform

| Claim(s) | Evidence | Disposition |
|---|---|---|
| `CQL-WP-7B607BB80EA6` — physical storage is only “specified” | verified bitmap/snapshot and server persistence capabilities | **Conflict, high.** The whitepaper now understates implemented maturity, although not every three-tier claim is realized. |
| `CQL-WP-BF1F3291584C` plus the nine state rows | core/server lifecycle capabilities | **Conflict, high.** Executable lifecycle is a four-string metadata model, not the governed nine-state machine. |
| Chapter 12–14 runtime functions (22 conflicts in the process domain) | `CQL-CORE-EVT-001`, `CQL-CORE-PRC-001` | **Conflict, high.** Parser/lowerer scaffolding exists, but dedicated operational/process function execution does not. Deferred-to-v2 statements remain unmatched rather than being mislabeled conflicts. |
| `CQL-WP-8D44BBF86B38` — DuckDB, Polars, and Arrow execution targets | `CQL-CORE-ADP-001`, `CQL-CORE-RES-001` | **Conflict, high.** DuckDB is the only executable adapter; Polars/Arrow are result conversions/contracts. |
| `CQL-WP-55617C0F6BB4`, `CQL-WP-5300CA5B3607` — broad millisecond retrieval | `CQL-CORE-BEN-001`, `CQL-CORE-BMP-001` | **Conflict, high.** Evidence is strong for one scoped 10M scenario, not the generalized platform claim. |
| Runtime security/RLS/tenant/hash-chain assertions | security syntax, `CQL-SRV-SEC-001`, `CQL-SRV-AUD-002`, GRANT probes | **Conflict, high.** The server has no authentication/RBAC/RLS/tenant enforcement; audit rows are not hash chained. |
| REST `/v1/*` rows and independent API-version claim | bounded query routes, `CQL-SRV-VER-001`, `P3-API-001` | **Conflict, high.** Generated OpenAPI is unversioned and application/package versions disagree. |

## Phase 1 high-risk disposition

All 50 claims marked high risk in Phase 1 remain visible:

| Phase 4 result | Phase 1 high-risk claims |
|---|---:|
| Conflict | 20 |
| Matched | 11 |
| Partial | 13 |
| Unmatched | 6 |

The 11 matched rows are mainly direct implementation inventory assertions such
as the error registry, DuckDB adapter, provider interfaces, builder, Jupyter
magic, CLI, and LSP. Their original risk notes remain in the CSV because such
inventory claims are version-sensitive even after verification at the pinned
commit.

## Limitations and handoff

- `matched` means the bounded atomic claim is supported at the frozen commits;
  it is not a release guarantee and does not validate neighboring prose.
- `partial` is intentionally broad. Most partial rows are low confidence because
  the capability is section-relevant but individual semantics still need a
  conformance test or decision.
- `unmatched` does not prove that code is absent. It means neither inventory nor
  targeted probes supplied a reviewed direct link.
- Server evidence reflects the reference SQLite implementation and in-process
  tests. It does not establish deployment, authorization, or external connector
  production maturity.
- Cross-references inherited from Phase 1 route adjudication but do not establish
  that every referenced decision is still accepted. Phase 5 must check decision
  status and supersession explicitly.
- The corpus excludes most illustrative code blocks. A probe may therefore be
  intentionally unlinked when its only target is an example that never became an
  atomic claim.

Phase 5 should prioritize the 77 conflicts, beginning with the 20 that were
already high risk in Phase 1. Each adjudication should select an authority,
record whether code or documentation changes, preserve the claim ID, and attach
acceptance evidence before the row can move to matched.
