# ContextQL Whitepaper, Specification, and Implementation Reconciliation Methodology

## 1. Purpose

This methodology governs a rigorous reconciliation of four things that have evolved at different speeds:

1. the ContextQL architectural thesis and whitepaper;
2. the normative language specification and accepted design decisions;
3. the implemented `contextql` language/runtime and `contextql-server` control plane;
4. the future product and research roadmap.

The goal is not to make the documents agree with the code automatically. The code may be incomplete, accidental, or wrong; the documents may be stale, aspirational, or internally inconsistent. The goal is to establish what is intended, what exists, what is verified, what conflicts, and what should happen next.

The cycle must produce:

- a traceable capability inventory;
- an evidence-backed gap register;
- explicit decisions for every material conflict;
- an approved closure plan;
- a consolidated, dependency-aware roadmap;
- corrected documentation and implementation;
- durable controls that reduce future drift.

## 2. Why a New Reconciliation Cycle Is Necessary

The repository already contains evidence that a simple documentation refresh would be insufficient:

- `contextql/WHITEPAPER.md` was last changed at core commit `6ad2886` on 2026-04-06.
- More than 13,000 lines were added to the core repository after that whitepaper revision, including executable context DDL, catalog abstractions, versioned membership snapshots, bitmap pushdown, temporal resolution, provider contract extensions, hardening, and benchmark evidence.
- Most of the current `contextql-server` control-plane implementation was added after the whitepaper revision.
- `RoadmapGapClosurePlan.md` describes several server capabilities as future work even though persistent catalog, provider, identity, audit, refresh, snapshot, and history surfaces now exist.
- `contextql-server/README.md` still presents a much smaller HTTP surface than the routes currently implemented.
- The whitepaper's coarse labels—Implemented, Specified, Designed, and Deferred—cannot accurately describe partial vertical slices.
- Existing specialist-agent material is valuable, but the ARCHIVIST mandate treats implementation as ground truth and does not have authority to decide when the implementation should change to match accepted design.

These are seed observations, not a completed audit. They demonstrate why the next cycle needs claim-level traceability, executable evidence, and explicit adjudication.

## 3. Governing Principles

### 3.1 No universal source of truth

Authority depends on the kind of claim being evaluated. A grammar file can prove that syntax is accepted; it cannot prove that the syntax has the intended semantics. A passing unit test can prove one tested behavior; it cannot prove that an architectural claim has been fully realised.

Any disagreement between authoritative sources is recorded as a gap. An agent must not silently choose the source it prefers.

### 3.2 Separate intent, existence, verification, and maturity

Every capability must be assessed along at least four axes:

- **Intended:** Is the behavior accepted as part of the design?
- **Present:** Does an implementation path exist?
- **Verified:** Is the behavior demonstrated by tests or reproducible evidence?
- **Operationally mature:** Is the behavior safe, observable, persistent, bounded, and supportable?

The word “implemented” must not collapse these distinctions.

### 3.3 Claims are the unit of reconciliation

Documents and files are too coarse. Each auditable statement must be decomposed into an atomic claim, such as:

- “`ALTER CONTEXT` invalidates incompatible snapshots.”
- “A provider timeout produces a partial result under the configured policy.”
- “The server persists context lifecycle state across restarts.”
- “`WEIGHTED_MAX` is the default multi-context scoring strategy.”

One paragraph, feature, or endpoint may therefore create several claims.

### 3.4 Positive and negative evidence are both required

Finding a class, route, or grammar production is not enough. Reviewers must also look for:

- unimplemented branches;
- `TODO`, `NotImplementedError`, placeholder, and fallback paths;
- tests limited to a happy path;
- declared but unused type models;
- state that is not persisted or restored;
- API surfaces that do not reach the engine;
- features that parse but do not execute;
- behavior that exists only in a demo or mock.

### 3.5 No silent reconciliation

Each material mismatch must end in one explicit disposition:

- update the whitepaper or supporting documentation;
- update the specification;
- change or complete the implementation;
- add missing verification;
- record an architectural decision;
- relabel maturity accurately;
- deprecate or remove an unintended implementation;
- retain as an approved future capability;
- reject the claim or feature.

### 3.6 One integrator, many reviewers

Specialists should investigate independently and challenge one another, but a single integration owner should apply final cross-document edits. This avoids contradictory terminology, duplicated roadmap items, and agents overwriting one another's work.

## 4. Source-of-Authority Matrix

The following matrix defines the default authority for each claim type. “Authority” does not mean infallibility; it identifies the source whose disagreement requires an explicit design decision.

| Claim type | Primary evidence of intent | Primary evidence of actual behavior | Verification evidence |
|---|---|---|---|
| Strategic purpose and category | Whitepaper principles plus accepted strategic decisions | Product surfaces and end-to-end use cases | Scenario review and design-authority approval |
| Language syntax | `SPEC.md` plus accepted decisions | `grammar/contextql.lark` and parser behavior | Positive and negative parser tests |
| Language semantics | `SPEC.md` plus `DECISIONS.md` | semantic models, lowerer, executor, and DDL runtime | Semantic, integration, and conformance tests |
| Public Python API | Accepted API decisions and published documentation | exported symbols and callable behavior | Contract and compatibility tests |
| Server API | Accepted API decisions and version policy | FastAPI routes, request/response models, OpenAPI output | API integration and persistence tests |
| Persistence and lifecycle | Storage/lifecycle decisions and normative spec | migrations, repositories, services, startup restoration | Restart, migration, transition, and failure tests |
| Federation and identity | Protocol and identity decisions | provider contracts, connectors, broker paths, identity maps | Contract, timeout, mismatch, and cross-system tests |
| Security and governance | Security decisions and threat model | enforcement code and storage boundaries | Abuse cases, authorization tests, audit evidence |
| Performance or scale | Explicitly scoped performance claims | benchmark implementation and runtime configuration | Reproducible benchmark artifact with environment metadata |
| Implementation maturity | Capability matrix status rules | reachable integrated behavior | Tests, probes, demos, and operational evidence |
| Future intent | Approved roadmap decisions | Prototypes may be supporting evidence only | Exit criteria or experiment results |

Additional rules:

- `DECISIONS.md` outranks prose when it contains a current, accepted decision.
- The whitepaper explains the architecture; it does not prove current implementation status.
- The grammar proves accepted syntax only, not executable support.
- Tests prove only their asserted behavior. Their absence creates an evidence gap; their presence does not automatically establish production maturity.
- Git history supplies provenance and change boundaries, not design authority.
- A plan or proposal is not an approved roadmap item unless its decision state is explicit.

## 5. Shared Taxonomy

### 5.1 Reconciliation status

Every capability claim receives one primary status:

| Code | Status | Meaning |
|---|---|---|
| `A` | Aligned | Intent, implementation, verification, and documentation agree at the claimed maturity. |
| `D` | Documentation lag | Verified behavior exists but is missing, stale, or inaccurately described. |
| `I` | Implementation lag | Accepted behavior is not implemented or is materially incomplete. |
| `S` | Specification gap | Behavior exists or is proposed without adequate normative semantics. |
| `V` | Semantic divergence | Code and accepted intent conflict. |
| `T` | Test/evidence gap | A capability is claimed or implemented but lacks adequate proof. |
| `P` | Partial vertical slice | Some layers exist, but the end-to-end capability does not. |
| `O` | Orphan implementation | Code exists without an accepted requirement or documented purpose. |
| `R` | Roadmap orphan | A proposed feature has no clear problem, sponsor, dependency path, or decision state. |
| `C` | Cross-document conflict | Normative or explanatory documents disagree with one another. |
| `U` | Unresolved | Evidence is insufficient or the design authority has not decided. |

Secondary tags should identify the affected dimensions: `syntax`, `semantics`, `runtime`, `storage`, `api`, `security`, `operations`, `performance`, `devx`, or `narrative`.

### 5.2 Capability maturity

Use a graduated maturity scale rather than a binary implemented/not-implemented label:

| Level | Name | Minimum evidence |
|---|---|---|
| `M0` | Idea | Problem or concept recorded, not yet accepted. |
| `M1` | Designed | Semantics and constraints accepted in specification or decision record. |
| `M2` | Surface implemented | Syntax, model, route, or interface exists, but not a complete execution path. |
| `M3` | Executable | A happy-path implementation works locally. |
| `M4` | Integrated | The relevant language, runtime, persistence/API, and restart boundaries work together. |
| `M5` | Verified and hardened | Negative cases, bounds, compatibility, failure behavior, and regression tests exist. |
| `M6` | Operationally proven | Observability, security, migration, performance, and real deployment evidence meet an explicit service target. |

Claims in public status tables must include the maturity level or an equivalent precise label. A feature with grammar and models but no execution is `M2`, not simply “implemented.”

### 5.3 Severity

- **P0 — Integrity or safety:** incorrect semantics, security boundary failure, corrupting persistence, or materially false performance/compliance claim.
- **P1 — Public contract:** public syntax, API, lifecycle, compatibility, or central architectural promise is materially inaccurate.
- **P2 — Product completeness:** a meaningful partial implementation, missing evidence, or roadmap dependency affects usability or credibility.
- **P3 — Editorial or discoverability:** terminology, examples, counts, links, and non-material omissions.

Severity and roadmap priority are related but not identical. A trivial documentation correction can be P1 and cheap; a strategically valuable new capability can be lower urgency but large.

## 6. Required Artifacts

### 6.1 Audit charter

Defines:

- repositories, branches, commits, and local changes in scope;
- release or version being reconciled;
- authoritative documents;
- explicitly excluded experiments and generated artifacts;
- design authority and adjudicators;
- stop date for evidence collection;
- expected outputs and acceptance gates.

### 6.2 Capability register

One row per atomic capability claim, with stable IDs such as `CQL-LANG-CTX-001` or `CQL-SRV-CAT-004`.

Minimum fields:

```text
capability_id
domain
atomic_claim
user_or_system_outcome
source_document_and_lines
normative_or_explanatory
target_version
accepted_decision_ids
implementation_files_and_symbols
public_surface
test_files_and_cases
runtime_probe_or_benchmark
negative_evidence
reconciliation_status
maturity_level
severity
confidence
disposition
owner
dependencies
roadmap_item
```

### 6.3 Evidence ledger

Evidence must be reproducible. Each item records:

- repository and immutable commit SHA;
- file and line or symbol;
- command or probe used;
- expected and observed result;
- environment and dependency versions where relevant;
- reviewer and review date;
- links to test, benchmark, trace, or API output.

### 6.4 Gap register

The gap register is a filtered, decision-oriented view of the capability register. Each material gap contains:

- claim and competing evidence;
- user/architecture impact;
- status, severity, and confidence;
- recommended disposition and alternatives;
- required decision owner;
- closure work and acceptance evidence;
- dependencies and release target.

### 6.5 Decision queue

Ambiguity must become a small decision packet rather than an open-ended discussion. Each packet contains:

1. the exact question;
2. current accepted decisions;
3. whitepaper/spec position;
4. observed implementation;
5. compatibility and security consequences;
6. two or three viable options;
7. specialist recommendation;
8. design-authority decision and date.

Accepted outcomes are added to `DECISIONS.md`; superseded decisions remain visible with their status and replacement.

### 6.6 Consolidated roadmap

The roadmap must contain approved outcomes, not a flat feature wish list. Every item carries:

- problem and beneficiary;
- expected outcome;
- supporting evidence;
- capability/gap IDs;
- maturity start and target;
- dependencies;
- security, operations, migration, and compatibility implications;
- acceptance tests and evidence;
- owner, horizon, and decision state.

## 7. Multi-Agent Review Structure

The existing specialist domains remain useful, but the reconciliation cycle requires narrower evidence contracts and explicit challenge roles.

### 7.1 Roles

- **Design authority:** decides unresolved intent, scope, compatibility, and roadmap commitment. This role is not delegated to an agent.
- **Reconciliation lead:** owns the charter, claim schema, cross-repository baseline, gap register, and final integrated output.
- **FORMALIST review:** syntax, type system, context algebra, temporal semantics, names, and normative language behavior.
- **ENGINE review:** lowering, execution, scoring, pushdown, storage abstractions, snapshots, resource bounds, and performance claims.
- **CONTEXTOPS review:** server catalog, lifecycle, refresh, federation, identity, persistence, scheduling, and distributed-operational claims.
- **PROCESSMINOR review:** event logs, process functions, conformance, process-backed contexts, and their executable maturity.
- **GUARDIAN review:** threat model, privileges, tenant boundaries, classification, audit integrity, credentials, and compliance language.
- **DEVX review:** public SDK, CLI, LSP, notebooks, diagnostics, examples, packaging, compatibility, and onboarding.
- **ARCHIVIST review:** factual document synchronisation after design conflicts have been adjudicated.
- **Evidence/test review:** independently challenges whether tests and benchmarks prove the stated capability.
- **Red-team/murder-board review:** looks for category overreach, hidden coupling, unbounded behavior, demo-only capability, unsafe defaults, and claims stronger than evidence.
- **Roadmap curator:** deduplicates future ideas and builds the dependency graph; does not invent commitments.

### 7.2 Three-pass protocol

**Pass 1 — Independent extraction.** Domain reviewers work from the same frozen baseline and submit structured claim/evidence rows. They do not edit the whitepaper and do not see other reviewers' conclusions until submission.

**Pass 2 — Adversarial cross-review.** Each domain is challenged by a reviewer from an adjacent layer. Examples: FORMALIST challenges ENGINE semantic coverage; ENGINE challenges CONTEXTOPS end-to-end reachability; GUARDIAN challenges federation and identity; DEVX challenges whether public examples actually run.

**Pass 3 — Adjudication.** The reconciliation lead groups duplicates and conflicts. Specialists present evidence; the design authority resolves intent. ARCHIVIST then prepares minimal factual edits, and the roadmap curator schedules approved closure work.

### 7.3 Agent output contract

Agents must return structured rows and decision packets, not only narrative essays. Each finding must include exact evidence, maturity, confidence, impact, and a proposed disposition. Claims without evidence are hypotheses and must be labelled accordingly.

Agents must not:

- mark a capability complete because a symbol exists;
- infer acceptance from a TODO, branch name, or proposal;
- alter another domain's accepted semantics;
- edit shared primary documents concurrently;
- turn every discrepancy into a new feature;
- resolve conflicts without the design authority.

## 8. End-to-End Reconciliation Process

### Phase 0 — Charter and freeze the baseline

1. Name the target release and the questions the cycle must answer.
2. Pin the core and server repositories to explicit SHAs and record branch names.
3. Record local modifications separately; do not allow an uncommitted file to become invisible evidence.
4. Record the supported core/server compatibility pair.
5. Freeze dependency versions and benchmark/runtime environment metadata.
6. Identify documents and proposals that are historical rather than current.
7. Obtain design-authority approval of the charter.

This matters especially here because `contextql` and `contextql-server` are separate Git repositories with independent histories.

### Phase 1 — Build the document claim corpus

Extract atomic claims from:

- `WHITEPAPER.md`;
- `SPEC.md`;
- `DECISIONS.md`;
- both READMEs and `CLAUDE.md` files;
- architecture documents, tooling specs, examples, plans, and proposals;
- root-level historical reviews and roadmaps.

For every claim, record whether it is:

- normative;
- architectural/explanatory;
- a current-status claim;
- a performance or scale claim;
- a future commitment;
- a non-binding idea.

Do not compare against code during initial extraction. This reduces confirmation bias and preserves what the documents actually claim.

### Phase 2 — Build independent implementation inventories

Create separate inventories for the core engine and server.

For the core engine, inspect:

- grammar and parser;
- semantic models and lowerer;
- executor and adapters;
- context DDL and catalog interfaces;
- membership, history, snapshot, temporal, and bitmap paths;
- provider and identity contracts;
- public API, builder, CLI, LSP, and notebook surfaces;
- tests, benchmarks, demos, CI, and packaging.

For the server, inspect:

- routes and OpenAPI models;
- services and dependency wiring;
- database migrations and repositories;
- startup restoration and engine synchronisation;
- provider, identity, audit, explain, refresh, and connector paths;
- configuration and resource boundaries;
- tests, demo integration, CI, and packaging.

Use static inventory plus executable probes. Static existence alone is insufficient.

### Phase 3 — Establish executable evidence

For each public capability, use the cheapest evidence that can falsify the claim:

1. parse positive and negative examples;
2. lower and inspect semantic models;
3. execute representative and boundary cases;
4. restart around persisted state;
5. exercise public SDK and HTTP surfaces rather than internal calls only;
6. inject timeout, provider failure, stale snapshot, incompatible schema, and invalid lifecycle transitions;
7. reproduce performance claims from a clean environment;
8. collect traces and audit events where explainability is claimed.

Record both successes and failures in the evidence ledger. A failing probe is a finding, not a reason to omit the row.

### Phase 4 — Join claims to evidence

Match document claims and implementation capabilities many-to-many:

- one claim can require grammar, semantic, runtime, server, and test evidence;
- one implementation can satisfy several narrower claims;
- unmatched document claims become implementation/specification candidates;
- unmatched implementations become documentation or orphan-implementation candidates.

Automated name matching may propose links, but a specialist must approve semantic equivalence.

### Phase 5 — Classify gaps and assess maturity

Assign each row:

- reconciliation status;
- capability maturity;
- severity;
- confidence;
- affected dimensions;
- proposed disposition.

Review partial vertical slices carefully. For example, grammar plus semantic models without executor behavior is not equivalent to end-to-end support; an API backed only by in-memory state is not persistent control-plane support; benchmark code without reproducible evidence is not a performance result.

### Phase 6 — Run adversarial review and adjudication

1. Pair adjacent domains for cross-review.
2. Have the red team challenge the strongest public and architectural claims first.
3. Consolidate duplicate findings by capability ID.
4. Escalate semantic, compatibility, security, and category conflicts through decision packets.
5. Record accepted decisions before changing code or architectural prose.
6. Maintain a “no-action” list for claims that were questioned and proven correct.

The design authority should decide questions of intent. Agents should decide whether evidence supports a factual statement.

### Phase 7 — Build the closure programme

Group approved work into four closure streams:

1. **Truth repair:** fix false status, API, maturity, and performance statements immediately.
2. **Contract repair:** align specification, decisions, grammar, APIs, and compatibility policy.
3. **Vertical-slice completion:** finish capabilities that are already substantially present but fail at a layer boundary.
4. **Hardening:** add negative tests, resource bounds, security enforcement, migrations, observability, and operational evidence.

Each closure item requires an owner, dependency, acceptance test, target version, and final document updates. Documentation and verification are part of the item, not follow-up chores.

### Phase 8 — Discover and consolidate unrealised features

Create a feature intake pool from:

- whitepaper future sections;
- deferred and open decisions;
- unimplemented normative clauses;
- TODOs, placeholders, test skips, and extension points;
- historical plans and agent drafts;
- gaps exposed by end-to-end scenarios;
- benchmark and operational bottlenecks;
- user, pilot, and integration feedback;
- external research, assessed separately and cited when used.

Convert every idea into a problem statement before comparing solutions. Deduplicate by desired outcome, not feature name.

Each candidate must pass these gates:

- Does it reinforce the SQL-first context-resolution thesis?
- Which user or operator problem does it solve?
- Is the problem evidenced or only hypothesised?
- Does the capability belong in the core engine, control plane, connector, tooling, or documentation?
- What foundation must exist first?
- Does it introduce semantic, compatibility, security, privacy, or operational risk?
- What would falsify its value cheaply?
- What is the minimum coherent vertical slice?

Candidates that fail these gates remain in an incubator with an explicit research question; they do not enter the delivery roadmap.

### Phase 9 — Produce a dependency-aware roadmap

Use four horizons:

- **H0 — Reconciliation and credibility:** false claims, semantic conflicts, missing tests, compatibility, and release blockers.
- **H1 — Complete started vertical slices:** capabilities already at M2–M4 whose completion unlocks the product.
- **H2 — Strategic product capability:** approved differentiators with validated problems and prerequisite architecture.
- **H3 — Research and options:** promising ideas requiring experiments, standards work, or design decisions.

Maintain roadmap lanes for:

- language and semantics;
- execution and storage;
- server/control plane;
- federation and identity;
- process intelligence;
- security and governance;
- developer experience and connectivity;
- documentation, evidence, and adoption.

Sequence by dependency and risk before estimating dates. Do not schedule an advanced distributed feature ahead of the lifecycle, identity, failure, security, and observability foundations it requires.

### Phase 10 — Implement in controlled change sets

Use one coherent change set per capability or tightly coupled group:

1. accepted decision/specification change, if needed;
2. implementation;
3. positive, negative, compatibility, and integration tests;
4. benchmark or operational evidence where claimed;
5. whitepaper/status/API/example updates;
6. capability-register status update.

Avoid a single large “make docs match code” commit. It obscures why a claim changed and makes regression review difficult.

### Phase 11 — Release verification and sign-off

The release candidate receives a clean-room review from the pinned commits, not from the authors' working notes.

Required sign-offs:

- FORMALIST: syntax and semantics;
- ENGINE: execution, storage, and performance;
- CONTEXTOPS: server, persistence, lifecycle, federation, and identity;
- GUARDIAN: security/governance claims and enforcement;
- DEVX: public surfaces and examples;
- Evidence reviewer: tests and benchmarks;
- ARCHIVIST: factual and terminological consistency;
- Design authority: unresolved trade-offs and roadmap.

## 9. Prioritisation Method

Use a rubric rather than an opaque aggregate score. Rate each item Low/Medium/High on:

- user or operator impact;
- semantic/correctness risk;
- security/compliance risk;
- public credibility risk;
- dependency centrality;
- compatibility/migration cost;
- evidence confidence;
- delivery effort and uncertainty.

Apply these rules:

1. P0 integrity and security gaps pre-empt roadmap features.
2. P1 public-contract gaps are resolved or explicitly disclosed before release.
3. High-centrality foundations outrank isolated features with similar value.
4. Low-confidence, high-cost ideas receive an experiment rather than a delivery commitment.
5. Documentation-only fixes may ship quickly, but they must not hide implementation gaps by weakening accepted semantics without a decision.
6. Started work receives no automatic priority; sunk cost is not product value.

## 10. Acceptance Gates

### 10.1 Claim gate

No capability is marked at a maturity level unless the required evidence for that level is linked in the capability register.

### 10.2 Language-feature gate

A public language feature requires:

- accepted syntax and semantics;
- grammar and parser behavior;
- semantic lowering/validation;
- executable behavior or precise non-executable status;
- positive and negative tests;
- errors/diagnostics;
- specification and example updates.

### 10.3 Server-feature gate

A public control-plane feature requires:

- versioned API schema;
- service and repository behavior;
- persistence and restart tests where stateful;
- engine integration where claimed;
- error, lifecycle, and authorization behavior;
- audit/trace behavior where required;
- README/OpenAPI/example updates.

### 10.4 Performance-claim gate

A performance claim requires:

- a committed benchmark;
- fixed data generator and parameters;
- environment and dependency metadata;
- correctness checks on benchmark output;
- raw result artifact;
- claim wording limited to the measured configuration.

### 10.5 Whitepaper gate

Before publishing a reconciled whitepaper:

- every implementation-status claim maps to capability IDs;
- no unresolved P0 or undisclosed P1 conflict remains;
- speculative architecture is clearly labelled;
- implemented, verified, and operational maturity are not conflated;
- examples are executable or explicitly illustrative;
- terms agree with the specification and glossary;
- the paired core/server release baseline is stated.

## 11. Recommended Whitepaper Structure After Reconciliation

Preserve the whitepaper as an architectural paper, but make its relationship to the living product explicit:

1. **Stable thesis and architecture:** the durable problem, design principles, formal model, and architectural direction.
2. **Normative references:** links to the language specification and decision register for exact semantics.
3. **Capability-status appendix:** a concise, versioned view generated from the capability register.
4. **Evidence appendix:** benchmark and conformance references, with measured scope.
5. **Future directions:** clearly separated into approved roadmap horizons and non-committed research.

This prevents frequent implementation changes from forcing broad architectural rewrites while still keeping public status accurate.

## 12. Continuous Drift Prevention

After the first reconciliation, add a machine-readable capability manifest as the common index. It should not replace specifications or tests; it should link them.

Recommended controls:

- a pull-request checkbox for capability IDs affected;
- a required decision entry for new or changed public semantics;
- CI detection of public grammar, exported API, route/OpenAPI, migration, and configuration changes;
- CI validation that referenced files, tests, and examples exist;
- executable documentation examples where practical;
- an OpenAPI compatibility diff for server releases;
- scheduled ARCHIVIST runs at milestone boundaries;
- a release gate that refreshes the generated capability-status appendix;
- a quarterly red-team review of the strongest public, scale, security, and differentiation claims;
- a compatibility matrix linking `contextql` and `contextql-server` versions.

The CI system should flag probable drift; it should not decide semantics automatically.

## 13. Recommended First Cycle for This Repository

### Cycle 0 — Immediate normative decision docket

Resolve these questions before broadly rewriting the whitepaper:

- **Authority and supersession:** reconcile the specification's normative-language claims, the grammar's self-description as an incomplete scaffold, the decision register's canonical status, and architecture documents that refer to agent drafts as authoritative.
- **Temporal semantics:** decide whether `AT` and `BETWEEN` filter an event-time column, resolve membership history/snapshots, or represent distinct operations. Mark OQ-9 and the later CS-16/CS-22 decisions with explicit supersession state.
- **`THEN`:** choose between candidate-scoped staged evaluation, temporal sequence matching, and the runtime's current intersection-like behavior; these must not share one operator accidentally.
- **`CONTEXT WINDOW`:** settle whether an unscored context is legal with a warning or invalid.
- **Scoring and negation:** settle the supported aggregation strategies, score normalization, and whether negation is unscored or computes `1 - score`.
- **DDL syntax:** reconcile composite-context `ON` requirements and the incompatible process-model syntaxes.
- **Lifecycle:** reconcile the nine-state reference model, the four-state implementation, core CREATE behavior, server draft behavior, and whether query execution is lifecycle-gated.
- **Error-code ownership:** remove conflicting meanings for W100/W101 and establish a generated registry as the reference index.
- **SQL compatibility:** replace unqualified SQL:2016/pass-through claims with an explicit conformance profile backed by tests.
- **Explain safety:** decide whether the server's explain endpoint is allowed to execute/mutate or must be a dry plan/trace operation.

Each item should produce an accepted decision, compatibility consequence, conformance fixture, and document disposition.

### Cycle A — Baseline and current truth

- Pin the intended core branch/commit and server commit as a supported pair.
- Resolve whether the two current local modifications are in or out of scope.
- Extract all whitepaper claims into the capability register.
- Inventory the July core and server additions first because they post-date the last whitepaper change.
- Replace stale current-status statements before attempting large architectural edits.

### Cycle B — Highest-risk semantic reconciliation

Review first:

- executable context DDL and lifecycle transitions;
- immutable context identity, versioning, snapshot compatibility, and temporal resolution;
- materialized membership, history, and bitmap pushdown;
- scoring strategies and aggregation behavior;
- MCP/REMOTE contracts, bounded joins, identity maps, and failure policy;
- server persistence, engine synchronisation, refresh, explain, audit, and recovery.

These areas combine recent implementation with central whitepaper claims and therefore have the highest divergence risk.

### Cycle C — Designed-but-unrealised domains

Audit process intelligence, full Context Ops, distributed federation, global identity, security/RBAC/RLS, multi-tenancy, connectivity, and LLM integration. For each, distinguish:

- accepted normative commitment;
- architectural reference design;
- existing foundation;
- missing vertical slice;
- research-only direction.

### Cycle D — Roadmap and publication

- adjudicate intent conflicts;
- approve closure streams and roadmap horizons;
- implement H0 truth/contract repairs;
- publish the reconciled whitepaper status appendix and compatibility baseline;
- retain unresolved research in a separate incubator rather than presenting it as committed delivery.

## 14. Definition of Done

The reconciliation cycle is complete only when:

- all material whitepaper claims have capability IDs;
- every capability has evidence, maturity, and reconciliation status;
- all P0 gaps are closed;
- all P1 gaps are closed or explicitly disclosed with an approved target;
- semantic conflicts have recorded decisions;
- implemented public behavior has appropriate tests and documentation;
- designed but unrealised features are accurately labelled;
- the core/server compatibility pair is recorded;
- the roadmap is deduplicated, dependency-aware, outcome-based, and decision-state explicit;
- a clean-room review reproduces the central behavior and benchmark claims;
- continuous drift controls are assigned and scheduled.

The final measure of success is not that every document says the same thing. It is that every important statement can be traced to intent, implementation, verification, maturity, and an accountable next action.
