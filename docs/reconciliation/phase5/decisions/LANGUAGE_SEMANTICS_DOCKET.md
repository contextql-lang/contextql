# Phase 5 Language and Semantics Decision Docket

**Baseline:** core `a054c8fcc576f3913d98d664ddf71eeea56d9755`; server `78c9565c33237a21dbf87f11d92ac6c7f29a846e`
**Prepared from:** Phase 1 claim corpus, Phase 2 capability inventories, Phase 3 probes, Phase 4 traceability and high-risk findings
**Owner for every packet:** design authority
**State for every packet:** `recommended-awaiting-decision`

## Purpose and decision discipline

This docket prepares, but does not make, normative decisions. A recommendation below is a specialist proposal for the design authority. It does not change `DECISIONS.md`, `SPEC.md`, `WHITEPAPER.md`, the grammar, or product behavior.

The identifiers are closed over the Phase 0-4 evidence. `None assigned` means Phase 3 produced no probe with a stable probe ID for that question; it does not mean that evidence is absent. In those cases the packet names the existing test or static evidence and requires a stable conformance probe before closure.

The phrase **current accepted decisions** below means entries currently written as decisions in `DECISIONS.md`. The register has no per-entry acceptance, supersession, decision-date, or replacement metadata, so Phase 5 must confirm their status rather than assuming that a later file position silently supersedes an earlier entry.

No traceability row should move to `matched` merely because the design authority chooses an option. Closure also requires the minimum acceptance evidence stated in the packet and the corresponding document/code disposition.

## Queue summary

| Packet | Question | Principal findings | Recommendation in one line |
|---|---|---|---|
| CQL-P5-LANG-001 | Authority and supersession | CQL-FND-HR-002, 009, 011, 030 | Adopt explicit decision states and replacement links; intent authority and behavioral evidence remain separate. |
| CQL-P5-LANG-002 | `AT` / `BETWEEN` | CQL-FND-HR-011 | Make membership-history replay the current meaning and explicitly supersede OQ-9, or introduce distinct syntax before retaining both models. |
| CQL-P5-LANG-003 | `THEN` | CQL-FND-HR-028 | Reserve `THEN` for candidate-scoped staged evaluation; use a distinct process/temporal construct for ordered events. |
| CQL-P5-LANG-004 | Scoring, negation, normalization | CQL-FND-HR-010, 029 | Approve one score algebra, keep negation unscored, warn rather than clamp, and expose only strategies with golden execution evidence. |
| CQL-P5-LANG-005 | `CONTEXT WINDOW` | CQL-FND-HR-013 | Retain legal-with-W001 plus configurable error, but require actual deterministic pre-predicate truncation. |
| CQL-P5-LANG-006 | Error and warning codes | CQL-FND-HR-009, 012, 013, 017 | Make a generated registry canonical: W001/W003/W100/W101 and E108/E111 retain their implemented meanings. |
| CQL-P5-LANG-007 | Composite/process/provider/security DDL | CQL-FND-HR-002, 030 | Publish a generated parse/lower/analyze/execute/persist profile and label non-executable forms reserved. |
| CQL-P5-LANG-008 | SQL conformance | CQL-FND-HR-007 | Replace SQL:2016/pass-through claims with a tested ContextQL SQL-subset profile. |
| CQL-P5-LANG-009 | Native parameters and composition | CQL-FND-HR-027 | Preserve the designed syntax as provisional, relabel it M2, then implement end-to-end binding and composite materialization before availability claims. |
| CQL-P5-LANG-010 | Multi-statement contract | CQL-FND-HR-026 | Make `Engine.execute` single-statement and reject extras; design a separate script API if needed. |

---

## CQL-P5-LANG-001 - Authority and supersession

**Exact question.** What artifact establishes intended syntax and semantics, what evidence establishes shipped behavior, and how must a later decision replace an earlier conflicting decision?

**Evidence IDs.**

- Claim IDs: `CQL-WP-95325AAF2949`, `CQL-WP-21CE7759130B`, `CQL-WP-98F4FAD2C112`
- Finding IDs: `CQL-FND-HR-002`, `CQL-FND-HR-009`, `CQL-FND-HR-011`, `CQL-FND-HR-030`
- Capability IDs: `CQL-CORE-PAR-001`, `CQL-CORE-ERR-001`, `CQL-CORE-HIS-001`
- Probe IDs: `P3-LAD-009`, `P3-EXE-010`, `P3-LAD-015`, `P3-EXE-016`
- Authority IDs: `AUTH-002`, `AUTH-003`, `AUTH-010`, `AUTH-012`, `AUTH-013`

**Competing evidence.** The whitepaper calls the Lark grammar canonical and says it covers all companion DDL. The grammar calls itself a non-normative-complete scaffold. The decision register says it consolidates decisions, but entries have no status or supersession fields. Later CS-16/CS-22 contradict earlier OQ-9 without saying that they replace it. Grammar accepts provider and administration statements that lower to `UNKNOWN` and fail execution (`P3-LAD-015`, `P3-EXE-016`); therefore grammar acceptance cannot establish semantics or maturity. Phase 0 authority rules already distinguish intent authority from behavior and verification evidence.

**Current accepted decisions and supersession issue.** `DECISIONS.md` documents a decision structure and ID namespaces but not acceptance metadata. `IM-1` explicitly replaces the whitepaper's PEG parser choice with Lark, demonstrating replacement in prose but without a machine-checkable supersession link. `OQ-9` and `CS-16`/`CS-22` remain simultaneously presented as v1/v0.3 decisions despite incompatible temporal meanings.

**Options.**

1. Treat the newest decision by file order/version as implicitly authoritative. This is low-overhead but non-auditable and unsafe when versions overlap.
2. Add explicit `proposed`, `accepted`, `superseded`, `deferred`, and `retired` states to decision records, with `supersedes`, `superseded_by`, accepted date, owner, scope, and compatibility note. Preserve the Phase 0 authority split: decisions/spec determine intent; implementation/tests prove behavior.
3. Declare the current implementation de facto normative and update documents to it. This resolves ambiguity quickly but permits accidental behavior to set language design.

**Recommendation.** Choose option 2. Establish precedence as: accepted decision for scoped intent; normative specification where no accepted decision overrides it; grammar as accepted-input evidence only; implementation plus tests/probes as behavior evidence; whitepaper as architecture/explanation and claim source; history as provenance only. Never use implementation drift as implicit supersession. **Proposed disposition:** governance repair, followed by specification/paper/code dispositions in the dependent packets.

**Compatibility and migration impact.** This is metadata-first and does not itself alter query behavior. It may expose previously hidden breaking decisions. Every newly marked supersession must state affected versions, whether old syntax/behavior is rejected or deprecated, and whether persisted definitions require migration.

**Minimum acceptance evidence.** A reviewed authority table; a decision-record schema; all currently conflicting entries linked bidirectionally; CI validation for duplicate active scopes and dangling replacement IDs; one rendered active-decision index; traceability regeneration showing each normative conflict points to exactly one active decision. The temporal conflict must be the first end-to-end acceptance fixture for the process.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-002 - Temporal semantics for `AT`, `BETWEEN`, and `AT VERSION`

**Exact question.** Does `AT <timestamp>` filter a context definition's temporal column or replay materialized membership history, what does `BETWEEN` return and score, and how is this distinguished from immutable `AT VERSION`?

**Evidence IDs.**

- Claim IDs: `CQL-WP-63B55DC68187`, `CQL-WP-7D5BDE108A84`, `CQL-WP-31665F0088EE`
- Finding IDs: `CQL-FND-HR-011`
- Capability IDs: `CQL-CORE-HIS-001`, `CQL-CORE-SNP-001`, `CQL-CORE-BMP-001`
- Probe IDs: none assigned; Phase 4 cites `tests/test_hardening_plan.py:258-458`

**Competing evidence.** Whitepaper section 7.4 and OQ-9 say `AT`/`BETWEEN` filter values in the declared temporal column, not historical snapshots. SPEC sections 6.3/6.4, CS-16, CS-22, `contextql/history.py`, and hardening tests implement event-time membership-history replay. Current runtime resolves `AT VERSION` directly to an immutable snapshot; timestamp `AT` replays to an instant; inclusive `BETWEEN` uses ever-present membership and the maximum score observed in the interval. These meanings can return different entities for identical text.

**Current accepted decisions and supersession issue.** `OQ-9` (v1) establishes temporal-column filtering. `CS-16` and `CS-22` (v0.3) establish membership-history replay and version selection. No record says that CS-16/CS-22 supersede OQ-9, and the whitepaper still reflects OQ-9. The conflict is semantic and compatibility-sensitive, not editorial.

**Options.**

1. Keep OQ-9: evaluate the context definition against rows selected by its temporal column; migrate or remove the history-replay runtime.
2. Make CS-16/CS-22 current: `AT <timestamp>` and inclusive `BETWEEN` replay retained membership history, while `AT VERSION` selects a snapshot; explicitly supersede OQ-9.
3. Preserve both models with distinct syntax, for example history-oriented `AT`/`BETWEEN` plus an explicit definition/event-column filter construct. Do not overload the same text.

**Recommendation.** Choose option 2 for the current release because it matches the integrated M5 storage/history path and current SPEC, then evaluate option 3 as a separate language proposal if temporal-column evaluation remains a validated use case. Specify UTC conversion, inclusive boundaries, retained-anchor failure (`E202`), reversed ranges (`E203`), entities added and removed inside the range, and the maximum-observed-score rule. **Proposed disposition:** explicit decision supersession plus whitepaper repair; implementation repair only for uncovered boundary cases.

**Compatibility and migration impact.** Queries written under OQ-9 can change membership and scores silently. Release notes must flag the semantic break. Persisted temporal contexts need no storage migration if the history model is accepted, but applications expecting row-time filtering require rewritten definitions or new syntax. Old snapshots outside retention must fail deterministically rather than fall back to current state.

**Minimum acceptance evidence.** A conformance table whose same source rows distinguish the two models; UTC/time-zone and granularity fixtures; exact-boundary and reversed-range tests; add/remove/re-add and score-change histories; retention-anchor failure; `AT VERSION` immutability; restart-persistent server tests; one stable Phase 5 probe ID for each temporal mode; accepted supersession metadata linking OQ-9 to CS-16/CS-22.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-003 - Normative meaning of `THEN`

**Exact question.** Is `c1 THEN c2` candidate-scoped staged context evaluation, ordered event-time matching, or only set intersection with downstream-score preference?

**Evidence IDs.**

- Claim IDs: `CQL-WP-A9DD818D939D`, `CQL-WP-09F05D8E1348`, `CQL-WP-CD2EFEC6AEEC`, `CQL-WP-0BBB9FF0628A`, `CQL-WP-C1B33A3FEAAC`, `CQL-WP-DC0357CD8F3E`
- Finding IDs: `CQL-FND-HR-028`
- Capability IDs: `CQL-CORE-CTX-003`
- Probe IDs: `P3-CTX-001`

**Competing evidence.** The whitepaper describes unlimited left-associative staged evaluation: resolve `c1`, evaluate `c2` only on those candidates, and let the downstream score dominate. SPEC section 4 instead says entities match contexts in temporal order for process pattern detection. OQ-6 decides only chain length and associativity. Runtime maps `sequence_mode` to intersection both in bitmap pushdown and dataframe masks; its scoring comment explicitly says staged scoping is not enforced. `P3-CTX-001` returns the intersection result but cannot demonstrate provider call scoping or temporal order.

**Current accepted decisions and supersession issue.** `OQ-6` accepts unlimited left-associative chains but leaves operator semantics unresolved. No accepted decision chooses between whitepaper candidate scoping and SPEC temporal sequence. Current intersection-like runtime is not an accepted decision and must not silently decide the language.

**Options.**

1. Candidate-scoped staged evaluation: each stage receives only survivors from the prior stage; chains are left-associative; final-stage score dominates.
2. Ordered temporal sequence: `THEN` means evidence that contexts became true in event-time order. This requires a common clock/history and explicit interval semantics.
3. Define `THEN` as an alias for intersection and deprecate it because it adds no set-semantic power; provide distinct constructs for staged provider calls and process sequences.

**Recommendation.** Choose option 1 for `THEN`, matching the language's conditional-composition explanation and creating an operationally useful bounded-provider primitive. Introduce a distinct process/history operator if ordered temporal matching is approved later. Specify whether a stage may have side effects (recommended: no), how failures and timeouts short-circuit, how aliases/entity keys propagate, and that only the final stage contributes score. **Proposed disposition:** specification and implementation repair; whitepaper largely retained after precision edits.

**Compatibility and migration impact.** Membership may equal intersection for pure deterministic contexts, but provider calls, latency, failures, audit traces, and scores can change. Any caller depending on all stages seeing the full population will observe different behavior. Plan/explain output must make candidate propagation visible.

**Minimum acceptance evidence.** An instrumented MCP/native context whose second-stage result and call arguments depend on the candidate set; two- and three-stage associativity fixtures; downstream-score golden results; mixed native/MCP/snapshot cases; empty-stage short-circuit; timeout/failure behavior; entity-key mismatch rejection; trace evidence proving candidate counts per stage; a negative test proving `THEN` is not implemented as ordinary intersection.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-004 - Score algebra, nulls, negation, and normalization

**Exact question.** What membership/score algebra is normative for union, intersection, weighted composition, multiple bindings, null/missing scores, and negation; which `ORDER BY CONTEXT USING` strategies are actually public; and are scores warned, normalized, clamped, or rejected outside `[0,1]`?

**Evidence IDs.**

- Claim IDs: `CQL-WP-1A9CBE1C60B3`, `CQL-WP-57402B0A131A`, `CQL-WP-90C1D0CF874A`, `CQL-WP-656B8DB419DD`, `CQL-WP-526B2E1EC29D`, `CQL-WP-1D2DDED23602`, `CQL-WP-3FA78AA50272`, `CQL-WP-1454AD1EFB36`, `CQL-WP-8CBE358B6C1D`, `CQL-WP-1055CD7F34B6`
- Finding IDs: `CQL-FND-HR-010`, `CQL-FND-HR-029`
- Capability IDs: `CQL-CORE-SCR-001`, `CQL-CORE-CTX-002`, `CQL-CORE-LNT-001`
- Probe IDs: none assigned; Phase 4 cites `tests/test_executor.py` and the executor scoring TODO

**Competing evidence.** The whitepaper defines boolean-member score `1.0`, union `MAX`, intersection `MIN`, weighted `MAX(w*s)`, null score preservation, and unscored negation. OQ-2, OQ-3, AD-4, and AD-5 mostly reinforce that model. Grammar advertises `MAX`, `MIN`, `AVG`, `SUM`, `COUNT`, `WEIGHTED_MAX`, and `WEIGHTED_SUM`. Runtime accumulates scores and has intersection-specific ad hoc addition; it does not dispatch on every strategy. Missing scores are converted to `0.0` or membership-derived `1.0`; weights and sums can exceed 1. The warning is heuristic, not a domain/type guarantee.

**Current accepted decisions and supersession issue.** `OQ-2`: intersection uses `MIN`. `OQ-3`: warn outside `[0,1]`, do not enforce. `AD-4`: within-binding uses MAX/MIN/WEIGHTED_MAX and cross-binding uses MIN. `AD-5`: weighted default is WEIGHTED_MAX, with WEIGHTED_SUM as an alternative. `GQ-6`: weights are user controlled. These decisions are mutually usable but incompletely implemented. The whitepaper's `[0,1] or NULL` type wording conflicts with the accepted unbounded-warning model unless `[0,1]` is explicitly a recommended range rather than the type domain.

**Options.**

1. Adopt the existing decision algebra exactly: boolean members score 1; union MAX; intersection MIN; weighted WEIGHTED_MAX; cross-binding MIN; null remains null; negation has no score; out-of-range warns without clamp. Implement every advertised strategy before claiming it.
2. Normalize every composite result to `[0,1]` and define scored negation as `1-score`. This provides fuzzy-set closure but changes existing intent and makes unbounded model scores lossy.
3. Publish a small v1 executable profile (MAX, MIN, WEIGHTED_MAX), reject other strategy tokens semantically, and add AVG/SUM/COUNT/WEIGHTED_SUM only through later accepted extensions.

**Recommendation.** Combine options 1 and 3: accept the decision algebra as the normative target, but expose only strategies that have golden execution evidence in the current conformance profile. Treat `[0,1]` as recommended/default-provider range, not an enforced type bound; emit W003 without clamping. Preserve a member's SQL NULL score rather than converting it to zero, place NULL last for both sort directions, and keep negation membership-only. Define zero weights as membership-only/W004 and reject negative weights/E110. **Proposed disposition:** specification clarification, implementation repair, and temporary capability relabeling.

**Compatibility and migration impact.** Correcting sum-like runtime to MAX/MIN changes rankings and potentially window membership. Preserving NULL rather than substituting zero changes ordering. Rejecting advertised but unsupported strategies converts silent wrong results into explicit failures. Version the score contract and provide before/after examples for stored queries.

**Minimum acceptance evidence.** A golden matrix for every public strategy across union, IN ALL, multiple bindings, weights, boolean/scored mixtures, ties, NULL/missing, zero/negative/out-of-range values, snapshots, MCP and native sources; explicit result-domain assertions; stable public diagnostics; property tests for commutativity/associativity only where promised; documentation generated from the supported strategy registry; no strategy may be public solely because grammar accepts it.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-005 - `WITH CONTEXT WINDOW`

**Exact question.** Is windowing on unscored contexts legal, when is the top-k applied, what deterministic order is used without scores, and is the warning-to-error setting part of the contract?

**Evidence IDs.**

- Claim IDs: `CQL-WP-0E13830F9ACB`, `CQL-WP-F4B108FC980E`, `CQL-WP-CE6F4B903EF2`, `CQL-WP-5C43588D39FE`, `CQL-WP-4988B499ED48`, `CQL-WP-76008D58AA00`, `CQL-WP-F6B02C9B10FA`
- Finding IDs: `CQL-FND-HR-013`
- Capability IDs: `CQL-CORE-WIN-001`, `CQL-CORE-ERR-001`
- Probe IDs: none assigned; Phase 4 cites `tests/test_linter.py:126-138`

**Competing evidence.** OQ-8 and the whitepaper allow unscored windowing with a warning and configurable error; SPEC and LSP hover say scoring is required. The whitepaper allocates W101 and promises entity-key ascending truncation. Implementation allocates W001, while W101 means refresh failure. The linter emits W001. The executor lowers `context_window`, but the main execution path applies context filters, scoring/order, SQL LIMIT and OFFSET without referencing `query.context_window`; existing tests do not prove actual top-k or phase ordering.

**Current accepted decisions and supersession issue.** `OQ-8` accepts legal-with-warning and configurable error. `IM-2` defines only code ranges, not identities. `CS-15` assigns W101 to refresh failure, colliding with the whitepaper's window warning. No accepted decision specifies exact unscored order or proves the `SET contextql.window_requires_scores` setting exists operationally.

**Options.**

1. Require scored contexts; make unscored use a semantic error and remove warning/configuration prose.
2. Allow unscored use with W001, truncate by canonical entity-key ascending order, and support a setting that promotes W001 to an error.
3. Allow unscored use silently and define source order as the tie-breaker. This is simple but nondeterministic across adapters and snapshots.

**Recommendation.** Choose option 2, preserving OQ-8. Apply the context window after context membership/scoring is resolved but before remaining ordinary WHERE predicates, final `ORDER BY`, projection, LIMIT and OFFSET, as the whitepaper intends. Define stable score ordering as score descending, then canonical entity key ascending; for unscored contexts use entity key ascending. Either implement the error-promotion setting or explicitly defer and remove that claim until it exists. **Proposed disposition:** error-code paper repair plus executor/spec/tooling repair.

**Compatibility and migration impact.** Implementing the currently inert clause can reduce returned rows and alter downstream results. The release must call this out as activation of previously unfulfilled syntax. W101 consumers must migrate to W001 for windowing; W101 remains refresh failure. The error-promotion setting needs a default and configuration-scope definition.

**Minimum acceptance evidence.** End-to-end fixtures over deliberately unsorted keys and tied scores; cardinality above/below/equal to window; proof of ordering relative to ordinary WHERE, ORDER BY, LIMIT and OFFSET; scored, unscored, mixed and NULL-score cases; W001 warning payload; configuration-to-error test if retained; snapshot/native/MCP parity; explain/trace record of pre/post-window cardinality.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-006 - Canonical error and warning codes

**Exact question.** Which registry owns diagnostic identity, and what stable meanings should be assigned to the currently conflicting W001/W003/W100/W101 and E108/E111 codes?

**Evidence IDs.**

- Claim IDs: `CQL-WP-9A51E8EA195F`, `CQL-WP-945AA0A449F8`, `CQL-WP-F6B02C9B10FA`, `CQL-WP-FFCA3426B13A`, `CQL-WP-1A867374DEE3`, `CQL-WP-3FCC4C832E5F`, `CQL-WP-9B44A1743052`
- Finding IDs: `CQL-FND-HR-009`, `CQL-FND-HR-012`, `CQL-FND-HR-013`, `CQL-FND-HR-017`
- Capability IDs: `CQL-CORE-ERR-001`, `CQL-CORE-LNT-001`, `CQL-CORE-WIN-001`, `CQL-CORE-SNP-001`, `CQL-SRV-SCH-001`
- Probe IDs: none assigned; Phase 4 cites `tests/test_linter.py:72-82,126-138` and static registry evidence

**Competing evidence.** Whitepaper uses W100 for score range, W101 for unscored windowing, and E111 for `CONTEXT_SCORE` scope; other freshness sections use W010/W012/W013. SPEC and `contextql/errors.py` use W001 window-without-score, W003 score-out-of-range, W100 stale snapshot, W101 refresh failure, E108 score scope, and E111 score-expression type. Tests cover some linter emitters, while several runtime paths construct code strings directly and many advertised codes have no emitter.

**Current accepted decisions and supersession issue.** `IM-2` accepts category ranges, not code-level meanings. `OQ-3` and OQ-8 require warnings but do not allocate codes. `CS-15` allocates W100/W101 to snapshot state. No accepted record resolves collisions with earlier whitepaper allocations or declares the implementation registry canonical.

**Options.**

1. Preserve whitepaper allocations and renumber current runtime diagnostics, including freshness codes.
2. Preserve implemented/SPEC allocations and publish a migration map: W001 window, W003 score range, W100 stale, W101 failed refresh, E108 score scope, E111 score type.
3. Create namespaced/category codes and deprecate every existing numeric identifier. Cleaner long-term, but maximally disruptive.

**Recommendation.** Choose option 2. Make one structured registry the canonical index and generate SPEC/whitepaper/reference tables from it. Keep symbolic names stable as well as numeric codes. Add lifecycle fields (`introduced`, `deprecated`, `replacement`) and emitter/test references. Do not provide ambiguous numeric aliases: old W100 cannot simultaneously mean score range and staleness. **Proposed disposition:** governance/specification/whitepaper repair; implementation repair for direct-string emitters and absent registered codes.

**Compatibility and migration impact.** Clients following the whitepaper may misclassify warnings today. Publish an explicit mapping and version boundary. Consumers should match canonical code plus name, not parse messages. Any code with no emitter must be labeled reserved, not supported. Renumbering implemented codes should be avoided unless telemetry shows no consumers.

**Minimum acceptance evidence.** Generated registry artifacts; uniqueness and range validation; one public-API emission test per active code; message-template argument validation; documentation consistency CI; negative proof that no direct unregistered code string is emitted; migration table for all conflicting whitepaper codes; owner-approved policy for reserved/deprecated codes.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-007 - Composite, process, provider, and security/administration DDL surface

**Exact question.** Which grammar-accepted DDL forms are normative executable v1 language, which are reserved syntax, and which exact syntax wins where the whitepaper, SPEC and grammar differ?

**Evidence IDs.**

- Claim IDs: `CQL-WP-E2801958841C`, `CQL-WP-0D29D6A2491C`, `CQL-WP-9A9FF97A9B5A`, `CQL-WP-CFB95979A8CF`, `CQL-WP-6AF540D3C3E9`, `CQL-WP-CC3B700640DA`, `CQL-WP-21CE7759130B`
- Finding IDs: `CQL-FND-HR-002`, `CQL-FND-HR-030`
- Capability IDs: `CQL-CORE-CMP-001`, `CQL-CORE-EVT-001`, `CQL-CORE-PRC-001`, `CQL-CORE-SEC-001`, `CQL-CORE-PAR-002`
- Probe IDs: `P3-DDL-001`, `P3-DDL-002`, `P3-PRO-001`, `P3-PRO-002`, `P3-LAD-009`, `P3-EXE-010`, `P3-LAD-011`, `P3-EXE-012`, `P3-LAD-013`, `P3-EXE-014`, `P3-LAD-015`, `P3-EXE-016`, `P3-LAD-017`, `P3-EXE-018`, `P3-LAD-019`, `P3-EXE-020`, `P3-LAD-021`, `P3-EXE-022`

**Competing evidence.** The whitepaper labels forms in section 39 as supported. Composite whitepaper syntax omits `ON`; grammar/SPEC require it (`P3-DDL-001/002`). Whitepaper process models use arrow/variant syntax; grammar uses `EXPECTED PATH` (`P3-PRO-001/002`). CREATE EVENT LOG and CREATE PROCESS MODEL have models and structural checks but fail execution. Provider registration, GRANT, CREATE NAMESPACE, SET and EXPLAIN CONTEXT parse but lower to `UNKNOWN` and fail execution. The grammar's broad surface therefore overstates reachable capability.

**Current accepted decisions and supersession issue.** `OQ-4` says CREATE EVENT LOG is included in v1. `AD-1` and `CS-12` preserve distinct vendor-neutral MCP/REMOTE roles. `CS-20` says executable DDL is the catalog write path. There is no accepted syntax decision for process model variants or composite `ON`, and no accepted release-profile decision for reserved statements. Security syntax exists without the enforcement boundary needed to make security claims.

**Options.**

1. Treat every grammar alternative as normative and complete all runtime/persistence/security vertical slices before the next release.
2. Publish a generated statement maturity profile. Only statements with parse, lower, analyze, execute, error, persistence/restart and authorization evidence are `supported`; retain other tokens as explicitly `reserved` and fail with a stable unsupported-statement diagnostic.
3. Remove all non-executable alternatives from grammar until their vertical slices are ready. This gives the smallest truthful surface but creates repeated syntax churn and harms forward parsing/tooling.

**Recommendation.** Choose option 2. Keep grammar scaffolding only if unsupported forms fail explicitly rather than lower to `UNKNOWN`. Adopt grammar/SPEC composite `ON` as provisional current syntax pending CQL-P5-LANG-009. Retain `EXPECTED PATH` as provisional process-model syntax and mark arrow examples superseded. Keep OQ-4 as a v1 target, not a shipped claim, until execution/persistence exists. Provider/admin/security DDL remains reserved; Python/REST provider registration is a separate current surface. GRANT/namespace syntax must never be labeled security support without enforcement and authorization tests. **Proposed disposition:** paper/status repair immediately; specification decisions for exact syntax; implementation roadmap for approved vertical slices.

**Compatibility and migration impact.** Existing parse-only files may begin failing with a clearer unsupported code rather than later generic executor failure. Choosing `ON` and `EXPECTED PATH` makes whitepaper examples invalid and requires migration notes. Persisted definitions should not be created for reserved statements. Future activation of security DDL is compatibility-sensitive because currently accepted text may have produced no effect.

**Minimum acceptance evidence.** A machine-generated row for every top-level grammar statement with parse/lower/analyze/execute/persist/restart/auth states; positive and negative syntax fixtures for each chosen form; stable unsupported-statement diagnostics; no `UNKNOWN` for a publicly named statement; event/process catalog restart tests before support elevation; provider activation query test; GRANT/namespace abuse tests and threat-model approval before security elevation.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-008 - SQL conformance and pass-through contract

**Exact question.** What SQL dialect/profile does ContextQL accept, and may documentation claim SQL:2016 extension or unchanged pass-through when every query must first parse and is reconstructed before DuckDB execution?

**Evidence IDs.**

- Claim IDs: `CQL-WP-9E08C9D03070`, `CQL-WP-46D55A59C26F`, `CQL-WP-1710EFFE252E`, `CQL-WP-774AECD4912F`
- Finding IDs: `CQL-FND-HR-007`
- Capability IDs: `CQL-CORE-PAR-002`, `CQL-CORE-QRY-001`
- Probe IDs: `P3-SQL-001`

**Competing evidence.** The whitepaper says standard SQL passes unchanged and declares an SQL:2016 extension. SPEC lists a broad SQL surface and general function syntax. The Lark grammar is a subset, requires `FROM` for SELECT, and must accept all input before adapter execution. `P3-SQL-001` shows `SELECT 1;` fails. Supported queries are lowered/reconstructed, so lexical text and some adapter semantics cannot be assumed unchanged. DuckDB support beyond the ContextQL grammar is unreachable through this entry point.

**Current accepted decisions and supersession issue.** There is no accepted decision defining a conformance level, dialect owner, feature matrix, or pass-through escape hatch. `IM-1` chooses Lark/Earley but does not grant SQL conformance. `IM-7` uses DuckDB SQL for REMOTE materialization internally; that is implementation architecture, not proof of public pass-through.

**Options.**

1. Implement a true pass-through detector/path for queries without ContextQL syntax and delegate them unchanged to the configured adapter; define the parser boundary for mixed queries.
2. Define ContextQL SQL as a tested subset/profile with DuckDB-backed execution; remove SQL:2016 conformance and unchanged-pass-through claims.
3. Adopt a full standards grammar and formal SQL conformance programme. This is the strongest claim but a large product commitment.

**Recommendation.** Choose option 2 for the current release. Publish a feature-by-feature ContextQL SQL profile and explicitly say DuckDB-backed rather than DuckDB-equivalent. Consider option 1 later as a separate API or explicit `EXECUTE NATIVE` mechanism so adapter-specific SQL is deliberate and does not bypass ContextQL safety/observability. **Proposed disposition:** whitepaper/SPEC repair and conformance-test expansion; no forced full-SQL implementation.

**Compatibility and migration impact.** This narrows claims rather than behavior. Queries users assumed were portable SQL may already fail; document unsupported forms and adapter differences. A future pass-through path could expose new side effects/security concerns and must be versioned rather than silently enabled.

**Minimum acceptance evidence.** A positive/negative feature matrix covering SELECT constants, joins, CTEs, subqueries, set operations, windows, aggregates, DDL/DML boundaries, quoted identifiers, parameters, comments and literals; adapter-specific behavior table; golden generated SQL where reconstruction matters; conformance CI; public error behavior for unsupported features; removal of unqualified SQL:2016/pass-through wording from every current-status surface.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-009 - Native context parameters and composite contexts

**Exact question.** Are typed named parameters and `COMPOSE` available language features or designed surfaces, and what binding, dependency, membership, scoring, refresh, and persistence semantics must make them executable?

**Evidence IDs.**

- Claim IDs: `CQL-WP-E2801958841C`, `CQL-WP-62B4C921B4F2`, `CQL-WP-283A7DF88DC7`, `CQL-WP-4670FF34EF6A`, `CQL-WP-58811F9DDF28`
- Finding IDs: `CQL-FND-HR-027`
- Capability IDs: `CQL-CORE-CMP-001`, `CQL-CORE-PAR-003`, `CQL-CORE-DDL-002`, `CQL-CORE-MCP-001`
- Probe IDs: `P3-DDL-001`, `P3-DDL-002`; no stable Phase 3 ID was assigned to the focused failed composite-query probe

**Competing evidence.** The whitepaper says required/default typed parameters bind by name at evaluation time and composite contexts derive membership/score from children. OQ-11 and OQ-12 decide named-only binding and type-compatible keys; AD-5 decides weighted scoring. Grammar and lowerer retain parameter declarations, calls, composition items, weights and dependencies. DDL persists composite metadata and validates dependency cycles. Native execution does not validate/substitute invocation arguments, and composite definitions produce neither executable SQL nor composite snapshots; a focused Phase 4 probe failed with unknown context. Named arguments do reach MCP providers, which is a different capability.

**Current accepted decisions and supersession issue.** `OQ-11`: named parameters only. `OQ-12`: composite child keys must be type compatible. `AD-5`: weighted composition defaults to WEIGHTED_MAX. `CS-20`: DDL is the catalog write path. These decisions establish pieces but do not specify parameter type coercion/cache identity, composite refresh/version consistency, temporal-child behavior, or failure atomicity. The whitepaper syntax without `ON` conflicts with grammar/SPEC.

**Options.**

1. Finish both as v1: implement native typed binding/substitution and live/materialized composite evaluation with approved score algebra.
2. Mark both as designed/reserved and reject invocation/query use until full vertical slices exist; retain metadata parsing for tooling.
3. Ship immutable macro expansion only: parameters substitute into definition SQL and composites expand child sets at query time, deferring materialized composite snapshots and independent lifecycle.

**Recommendation.** Choose option 1 as the target but apply option 2 to current release status until acceptance gates pass. Parameter binding must be named-only, validate missing/unknown/duplicate arguments and types, apply defaults, use safe bound values rather than SQL text interpolation, and include effective parameters in cache/trace identity. Composite definitions should require `ON`, validate key compatibility, evaluate children at a consistent version boundary, use the accepted score algebra, materialize atomically when configured, and expose child versions in trace. **Proposed disposition:** immediate relabeling plus implementation closure roadmap; specification completion before coding semantics that remain open.

**Compatibility and migration impact.** Enforcing `ON` invalidates whitepaper examples. Validation may turn previously ignored arguments into errors. Safe binding must avoid string interpolation and audit parameter values under the security policy. Composite versioning can change refresh timing and persisted catalog shape; migrations need rollback and restart evidence.

**Minimum acceptance evidence.** Parameter tests for required/default, every public type/coercion, NULL, unknown/duplicate/missing names, injection-shaped values, cache separation and trace redaction; composite tests for UNION/INTERSECT/WEIGHTED, boolean/scored mixes, incompatible keys, missing/cyclic children, child updates, consistent versions, temporal children, refresh/restart/drop cascade, failure rollback; successful public create-query-refresh-restart probes for both surfaces.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

---

## CQL-P5-LANG-010 - Multi-statement execution contract

**Exact question.** Does the public execution API accept exactly one statement, execute a script, or return multiple results transactionally when parsing yields multiple statements?

**Evidence IDs.**

- Claim IDs: none; Phase 4 identifies this as a code-only capability candidate
- Finding IDs: `CQL-FND-HR-026`
- Capability IDs: `CQL-CORE-QRY-002`
- Probe IDs: `P3-MUL-001`, `P3-MUL-002`

**Competing evidence.** Grammar/parser retain multiple semicolon-delimited statements and the lowerer returns both (`P3-MUL-001`). `ContextQLExecutor.execute_sql` selects `analysis.statements[0]`, and `P3-MUL-002` proves only the first result is returned while the second statement is silently ignored. CLI file mode uses a separate naive split, creating another implicit contract and risks around semicolons inside literals/comments. No whitepaper claim or accepted decision authorizes script execution.

**Current accepted decisions and supersession issue.** No applicable accepted decision exists. This is not a supersession conflict; it is an unspecified public behavior with silent data/control-flow loss.

**Options.**

1. Make `Engine.execute` single-statement: reject input containing more than one semantic statement with a stable error. Add an explicit script API later.
2. Execute all statements sequentially and return a multi-result object; define stop/continue-on-error and transaction boundaries.
3. Execute all transactionally and return multiple results. Strongest atomicity, but provider calls, remote reads, and non-transactional effects make a universal transaction difficult.

**Recommendation.** Choose option 1 now. Silent truncation must end immediately. Define whitespace/comments/empty trailing semicolons as non-statements. If scripts are needed, design `execute_script` separately with parser-derived statement boundaries, a multi-result type, explicit atomicity per statement class, and a policy for external/provider effects. **Proposed disposition:** implementation repair and public API documentation; roadmap item for scripts only with validated demand.

**Compatibility and migration impact.** Inputs that currently execute only the first statement will begin failing, surfacing latent bugs instead of silently omitting work. CLI file behavior must align or be clearly named as script execution. Do not simulate scripts by naive semicolon splitting.

**Minimum acceptance evidence.** Tests for SELECT+SELECT, SELECT+DDL, DDL+SELECT, DDL+failing DDL, comments, empty statements, semicolons in string/quoted identifiers, and trailing semicolons; stable multi-statement error from Engine/CLI/server; proof no statement executes before rejection; if a script API is later accepted, transaction/failure/provider-side-effect and multi-result contract tests.

**Owner:** design authority
**State:** `recommended-awaiting-decision`

## Adjudication recording requirements

For each packet, the design authority should add the selected option (or a precisely written alternative), decision date, accepted version scope, compatibility class, and replacement links to `DECISIONS.md`. The Phase 5 working row should then move from `recommended-awaiting-decision` to the locally defined accepted/rejected/deferred state. Only after acceptance should closure work alter normative specification, whitepaper prose, grammar, implementation, or published compatibility statements.

The companion `language_decisions.csv` is the machine-readable queue. It intentionally carries the same recommendations and evidence IDs so adjudication and traceability can be checked without parsing this narrative document.
