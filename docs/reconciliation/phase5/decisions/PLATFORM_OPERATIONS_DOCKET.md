# Phase 5 Platform, Operations, and Security Decision Docket

## Purpose and decision boundary

This docket converts the platform-facing Phase 4 findings into adjudicable packets. It does not change implementation, normative language, or the whitepaper. Evidence is pinned to core commit `a054c8fcc576f3913d98d664ddf71eeea56d9755` and server commit `78c9565c33237a21dbf87f11d92ac6c7f29a846e`.

`factual-truth-repair` means the recommendation only corrects an unsupported statement about the pinned release; it does not retire the architecture as a future design. `recommended-awaiting-decision` means a design, compatibility, security, or roadmap owner must accept an outcome before closure work begins. Acceptance requires a dated decision in `DECISIONS.md` (or an expressly designated successor), named owners, and the evidence listed in the packet.

## Queue

| Packet | Question | Severity | Recommended disposition | Owner | State |
|---|---|---:|---|---|---|
| CQL-P5-PLAT-001 | What security/compliance statements are true of the pinned release? | P0 | Immediate public truth repair | Documentation integration owner + security design authority | factual-truth-repair |
| CQL-P5-PLAT-002 | What lifecycle and freshness states gate create, refresh, and query? | P1 | Four durable governance states for the next supported release; runtime substate remains separate | Core/server architecture owners | recommended-awaiting-decision |
| CQL-P5-PLAT-003 | Is server explain dry or executing? | P1 | Make explain dry; move execution-and-trace to an explicitly named endpoint | API owner + security design authority | recommended-awaiting-decision |
| CQL-P5-PLAT-004 | When does a persisted provider become active and healthy? | P1 | Registry-driven activation with active probes and fail-closed routing | Federation owner + server operations owner | recommended-awaiting-decision |
| CQL-P5-PLAT-005 | Is current identity functionality binding or resolution? | P1 | Name and document it as deterministic identity binding; defer canonical/confidence resolution | Identity/governance owner | recommended-awaiting-decision |
| CQL-P5-PLAT-006 | What versions identify the server API and compatible core? | P1 | One generated release version, `/v1` public routes, explicit pair matrix | Release/API compatibility owner | recommended-awaiting-decision |
| CQL-P5-PLAT-007 | Which context fields are mutable? | P1 | Reject currently ignored fields until atomic semantics are designed | Catalog API owner | recommended-awaiting-decision |
| CQL-P5-PLAT-008 | What storage, isolation, and performance claims are supportable? | P1 | Bound claims to reference implementations and reproducible scenarios | Storage/performance owner | recommended-awaiting-decision |
| CQL-P5-PLAT-009 | Is connector synchronization a generic streaming capability? | P1 | No; supersede v1 generic micro-batch commitment and name connector sync precisely | Operations/connector owner | recommended-awaiting-decision |
| CQL-P5-PLAT-010 | What integrity guarantee does the audit log provide? | P0/P1 | Treat current log as operational event history; design a separately verified integrity profile | Security design authority + audit owner | recommended-awaiting-decision |
| CQL-P5-PLAT-011 | How mature and public is the server control plane? | P1 | Document the implemented single-node reference surface and its explicit limits | Server documentation owner | factual-truth-repair |

## CQL-P5-PLAT-001 — Immediate security and compliance truth repair

**Exact question.** Which security and compliance statements may describe the pinned release before authentication, authorization, tenant isolation, RLS, and tamper-evident audit are implemented and independently verified?

**IDs.** Finding `CQL-FND-HR-020`; claims `CQL-WP-E7665FC969E0`, `CQL-WP-5DEA2F35D7D6`, `CQL-WP-6F7A816BE852`, `CQL-WP-8E079CB7A4CB`; capabilities `CQL-CORE-SEC-001`, `CQL-CORE-AUD-001`, `CQL-SRV-SEC-001`, `CQL-SRV-AUD-001`, `CQL-SRV-AUD-002`; authority `AUTH-008`.

**Current accepted decisions.** GQ-1 through GQ-8 define intended namespace, visibility, storage, and audit-content policy. They do not record an accepted implementation-maturity or compliance-assurance decision.

**Competing evidence.** The whitepaper presents fail-closed privileges, RBAC, RLS, classification propagation, tenant isolation, hash-chained Parquet audit, GDPR/SOX support, and regulated-industry readiness as operating properties (`WHITEPAPER.md:1648-1873,2587`). GQ-1 through GQ-8 record desired controls and audit content. At the pinned release, `GRANT` is parse-only; HTTP routes have no authentication/authorization dependency; namespace and classification are metadata; the audit store is ordinary SQLite without signatures or a hash chain. Passing server tests do not include an auth, tenant-abuse, or audit-integrity suite.

**Options.** (A) Leave current assurances and treat architecture prose as sufficient. (B) Qualify every affected passage as reference architecture/future design and remove readiness or compliance assurance. (C) Remove the security architecture entirely until implementation exists.

**Recommendation.** Choose B immediately. Preserve the design, but add a prominent release-status boundary wherever the current tense could be interpreted as an implemented control. Prohibit “production-ready for regulated industries,” “compliant,” “fail-closed,” and equivalent assurance language until verified enforcement exists.

**Consequences.** This reduces misleading security reliance without deciding the eventual enforcement design. It may narrow positioning in the short term. It also creates a publication gate: security architecture can be discussed, but operational assurance must be evidence-backed.

**Acceptance evidence.** A claim search shows all current-tense security/control/readiness statements either removed or marked design/future; a capability table states authentication, RBAC, RLS, tenant isolation, audit integrity, and independent security review as absent; the security design authority signs the boundary; publication review has no unqualified compliance assurance.

**Owner/state.** Documentation integration owner, accountable security design authority; `factual-truth-repair`.

## CQL-P5-PLAT-002 — Lifecycle, freshness, create semantics, and query gating

**Exact question.** What are the canonical durable lifecycle states, legal transitions, create state, runtime refresh substates, freshness policy, and query visibility rules shared by core and server?

**IDs.** Findings `CQL-FND-HR-016`, `CQL-FND-HR-017`; claims `CQL-WP-BF1F3291584C`, `CQL-WP-66DB2AA5B51F`, `CQL-WP-D71BB7D52680`, `CQL-WP-FFCA3426B13A`, `CQL-WP-1A867374DEE3`, `CQL-WP-3FCC4C832E5F`, `CQL-WP-9B44A1743052`; capabilities `CQL-CORE-LIF-001`, `CQL-CORE-OPT-001`, `CQL-CORE-SNP-001`, `CQL-SRV-CAT-001`, `CQL-SRV-CAT-004`, `CQL-SRV-CAT-005`, `CQL-SRV-SCH-001`; authority `AUTH-006`.

**Current accepted decisions.** CS-7, CS-15, and CS-18 require copy-and-promote, defined snapshot-state reads, and central compatibility enforcement. No current decision reconciles those rules with the paper's nine-state FSM or the divergent core/server create states.

**Competing evidence.** The paper specifies a nine-state FSM, `max_staleness`, multiple refresh policies, and W010/W012/W013. The current specification and CS-15 define `stale_after`, W100/W101, copy-and-promote, and available stale reads. Core accepts four lifecycle strings and creates active contexts; server creates through core then changes the object to draft. Server offers validate/activate/retire, while queries can still read draft or retired contexts. A focused probe confirmed a retired context remained queryable. No W013 emitter or strict-freshness branch is evidenced.

**Options.** (A) Implement the paper’s nine states as one persisted FSM. (B) Standardize four durable governance states (`draft`, `validated`, `active`, `retired`) and represent `materializing`, `refreshing`, `stale`, `failed`, and similar conditions as orthogonal runtime status. (C) Treat all states as informational metadata with no query gate.

**Recommendation.** Choose B. Create must persist `draft` atomically in both core and server. Only `active` is queryable through ordinary query endpoints; preview is the explicit draft/validated evaluation path. Retired contexts remain addressable for audit/history but not ordinary resolution. Missing/invalid snapshots fail E200; stale or last-failed snapshots follow CS-15 (serve with W100/W101); refreshing serves the last good snapshot. Use `stale_after` as the canonical v0.3 vocabulary, and defer strict/blocking freshness until separately designed.

**Consequences.** This is simpler than the paper’s conflated nine-state model and makes safety boundaries testable. It changes current core create behavior and query compatibility for callers relying on draft/retired reads. It requires server/core atomicity so a transient active object is never exposed during create.

**Acceptance evidence.** One generated transition matrix shared by spec/core/server; create is draft after success and after restart; illegal transitions have stable errors; query/preview probes cover every governance state and runtime condition; clock-controlled stale/refresh-failed tests assert W100/W101; concurrent refresh preserves the previous snapshot; no context is observable in an intermediate create state; docs and OpenAPI state the gate.

**Owner/state.** Core semantics owner + server control-plane owner, consulted operations owner; `recommended-awaiting-decision`.

## CQL-P5-PLAT-003 — Explain execution safety

**Exact question.** Does `POST /query/explain` promise a side-effect-free plan, or does it execute the query and return trace/rows?

**IDs.** Finding `CQL-FND-HR-021`; claim `CQL-WP-506FAB66AF2A`; capabilities `CQL-CORE-EXP-001`, `CQL-SRV-EXP-001`, `CQL-SRV-EXP-002`; evidence `P3-API-001`; authorities `AUTH-005`, `AUTH-008`.

**Current accepted decisions.** EQ-8 governs adapter SQL visibility in `EXPLAIN VERBOSE`; it does not say that explain may execute or return query rows.

**Competing evidence.** The paper describes `/v1/query/explain` as returning an execution plan and core `Engine.explain` is dry. The unversioned server endpoint invokes normal execution, can call providers, reads data, and returns actual rows plus trace. EQ-8 limits adapter SQL visibility in verbose explain but does not authorize execution.

**Options.** (A) Keep executing behavior under `explain` and document it. (B) Make `explain` dry and add a distinct authenticated `/query/trace` or `execute?trace=true` operation for execution telemetry. (C) Expose plan and execution in one endpoint selected by a required mode.

**Recommendation.** Choose B. “Explain” should be predictably side-effect-free. The execution trace surface should be explicitly named, authorized like query execution, bounded like a query, and visibly marked as executing.

**Consequences.** Existing callers using explain for rows must migrate. Dry explain may initially provide less runtime detail, but it removes a safety trap and aligns core, paper, and common API expectations.

**Acceptance evidence.** Provider, adapter, refresh, DDL, and audit spies prove dry explain performs no execution; OpenAPI marks side effects and authorization for the trace endpoint; trace retains query row/intermediate/response bounds; compatibility/deprecation tests cover the old route; verbose-plan authorization is tested separately.

**Owner/state.** Server API owner + security design authority; `recommended-awaiting-decision`.

## CQL-P5-PLAT-004 — Provider registry activation and health

**Exact question.** What lifecycle turns persisted provider metadata into a routable core provider, and does “healthy” mean stored status or a successful live probe?

**IDs.** Finding `CQL-FND-HR-022`; claims `CQL-WP-CFB95979A8CF`, `CQL-WP-7DBCE3652446`; capabilities `CQL-SRV-PRO-001`, `CQL-SRV-PRO-002`, `CQL-SRV-PRO-003`, `CQL-CORE-MCP-001`, `CQL-CORE-REM-001`; authority `AUTH-007`.

**Current accepted decisions.** IM-4 defines the core provider protocol, CS-10 keeps credentials as references, and CS-12 defines MCP/REMOTE roles. No accepted decision defines server registry hydration, provider factories, active health, or desired-versus-observed state.

**Competing evidence.** Provider CRUD persists role, lifecycle, credential reference, and stored health. Built-in/mock providers are registered through separate startup wiring. There is no persisted-registry hydration/factory path analogous to identity synchronization, and the health route returns stored state rather than probing the configured endpoint. Thus an enabled/healthy record can be unavailable to queries.

**Options.** (A) Keep the registry informational and rename status fields accordingly. (B) Make it authoritative: resolve factory and credentials, validate configuration, activate on startup/enable, actively probe health, and unregister on disable. (C) Split desired-state registry from a separate runtime-instance registry.

**Recommendation.** Choose B for the reference server, while storing desired state and observed runtime state separately. Only successfully activated providers may be routable; active health must carry probe time and diagnostic reason. Startup failure for one provider should degrade that provider, not silently misreport health or necessarily stop unrelated local queries.

**Consequences.** Adds plugin/factory allowlisting, credential and SSRF boundaries, restart behavior, and failure policy. It turns currently decorative operational metadata into a contract and may expose previously hidden configuration failures.

**Acceptance evidence.** External provider configured solely through the API survives restart and is queryable; bad credential, unknown factory, forbidden endpoint, timeout, schema mismatch, disabled, and recovery cases are tested; health distinguishes desired state, activation state, last probe, and stored history; secrets never enter catalog/trace/audit payloads; concurrent activation is idempotent.

**Owner/state.** Federation owner + server operations owner, consulted security owner; `recommended-awaiting-decision`.

## CQL-P5-PLAT-005 — Identity scope and naming

**Exact question.** Is the current feature a deterministic path/column binding registry, or the paper’s global canonical, confidence-based identity-resolution system?

**IDs.** Finding `CQL-FND-HR-023`; claim `CQL-WP-5592BF0BAA46`; capabilities `CQL-CORE-IDN-001`, `CQL-SRV-ID-001`, `CQL-SRV-ID-002`; authority `AUTH-007`; decisions OPS-5, OPS-6, AD-2, AD-6, GQ-4.

**Current accepted decisions.** OPS-5/OPS-6 and AD-2/AD-6 place many-to-many, composite, canonical, confidence-based identity in v1; GQ-4 excludes cross-tenant resolution. These decisions have not been superseded despite the narrower implementation.

**Competing evidence.** AD-2/AD-6 and the paper define local, system-qualified, and global canonical identities plus a federated mode and confidence. The server persists matching mode/confidence, but startup passes exact `table.column` path pairs to core. There are no canonical entity records, resolver, ambiguous-match semantics, provenance lifecycle, or probabilistic execution. Cross-tenant resolution is already deferred by GQ-4.

**Options.** (A) Preserve “identity resolution” naming and implement the full v1 design now. (B) Rename/reframe the shipped capability as deterministic identity binding and move canonical/confidence resolution to an approved roadmap item. (C) Remove the server registry until the full model exists.

**Recommendation.** Choose B. Keep the useful exact-binding path, do not expose inert `confidence` or matching-mode inputs as effective runtime semantics, and explicitly supersede the v1 timing of OPS-5/AD-2/AD-6 while retaining the architecture as a future candidate.

**Consequences.** This is an API/model migration if public names or fields change, but it prevents governance assumptions based on metadata that execution ignores. The future resolver needs privacy, authorization, provenance, conflict, deletion, and cross-system key-type design before scheduling.

**Acceptance evidence.** Current API and docs use one unambiguous term; every accepted field changes runtime or is rejected; deterministic cross-system key-type/restart tests pass; future canonical resolution has a separate model, decision, threat analysis, ambiguity/confidence fixtures, and authorization/privacy acceptance criteria.

**Owner/state.** Identity/governance owner, consulted federation and security owners; `recommended-awaiting-decision`.

## CQL-P5-PLAT-006 — API, release, and core/server compatibility identity

**Exact question.** Which value is the authoritative server release version, how are public HTTP versions expressed, and what core/server combinations are supported?

**IDs.** Findings `CQL-FND-HR-024`, `CQL-FND-HR-032`; capability `CQL-SRV-VER-001` plus `CQL-SRV-API-001`, `CQL-SRV-QRY-001`; evidence `P3-API-001`; authority `AUTH-005`.

**Current accepted decisions.** No accepted decision defines server release identity, HTTP versioning, or the supported core/server matrix. The Phase 0 frozen pair is an audit pin explicitly not a public compatibility guarantee.

**Competing evidence.** Server package metadata and README report 0.1.0; FastAPI/OpenAPI and startup audit report 0.3.0; core reports 0.2.0. The dependency only states `contextql>=0.2`. All 27 observed routes are unversioned while the paper uses `/v1` and says HTTP and language versions are independent. The frozen pair is audit evidence, not a compatibility promise.

**Options.** (A) Keep unversioned routes and Semantic Versioning only at the package level. (B) adopt `/v1`, generate application/package/audit version from one source, and publish a tested core/server matrix. (C) declare the current API experimental and postpone route versioning until stabilization.

**Recommendation.** Choose B, with a time-bounded compatibility alias or explicit breaking release for current unversioned routes. Distinguish server package version, HTTP major version, core package version, and language/spec version. Pin and test the lower and upper bounds of supported core versions.

**Consequences.** Route migration and client changes are required. In return, diagnostics and audit records identify the executable pair, and OpenAPI changes can be governed instead of inferred from a floating dependency range.

**Acceptance evidence.** Package metadata, `--version`, FastAPI/OpenAPI, health/startup audit, README, release tag, and artifact metadata share one generated server release value; public routes are versioned or explicitly experimental; a committed compatibility matrix and CI jobs test every supported pair; OpenAPI compatibility diff and deprecation policy gate releases.

**Owner/state.** Release/API compatibility owner + core/server maintainers; `recommended-awaiting-decision`.

## CQL-P5-PLAT-007 — Ignored context-update fields

**Exact question.** Are `entity_key`, `has_score`, and `classification` mutable through context update, immutable after creation, or unsupported?

**IDs.** Finding `CQL-FND-HR-025`; capability `CQL-SRV-CAT-003`; authorities `AUTH-005`, `AUTH-006`.

**Current accepted decisions.** CS-13 rejects silent option ignoring, CS-17 makes context identity immutable, and CS-18 invalidates incompatible snapshots. No accepted decision defines mutability for these request fields.

**Competing evidence.** `ContextUpdate` accepts the three fields, so clients receive a successful schema-level contract, while `CatalogService.update` applies definition, description, tags, and score-column changes only. CS-13 prohibits silent option ignoring, and immutable identity/snapshot compatibility decisions imply that some changes may require invalidation or recreation.

**Options.** (A) Implement all fields as mutable immediately. (B) Remove/reject all three until per-field version, invalidation, authorization, and migration semantics are accepted. (C) accept only classification as metadata and require recreate/migration for key/score shape.

**Recommendation.** Choose B now; then decide fields independently. Unsupported supplied fields must fail with a stable client error, never succeed silently. The likely target is C, but classification itself requires authorization/audit policy before it becomes a security-relevant mutation.

**Consequences.** Strict rejection can break clients whose requests currently “succeed,” but their intended changes are already absent. Future key or score-shape changes must define snapshot invalidation, history compatibility, dependent contexts, and rollback.

**Acceptance evidence.** One contract test per request field asserts persisted, engine-observed, version, snapshot, audit, and restart behavior or a stable rejection; omitted and explicit-null semantics are distinct; generated OpenAPI exposes only effective fields; no successful response accompanies an ignored value.

**Owner/state.** Catalog API owner + core storage owner for key/score changes, security owner for classification; `recommended-awaiting-decision`.

## CQL-P5-PLAT-008 — Storage, MVCC, isolation, and performance claim scope

**Exact question.** Which storage tiers, MVCC/isolation guarantees, asymptotic statements, and latency numbers describe the pinned reference implementations rather than target architecture?

**IDs.** Finding `CQL-FND-HR-001`; claims `CQL-WP-55617C0F6BB4`, `CQL-WP-B537AEBC3005`, `CQL-WP-5300CA5B3607`, `CQL-WP-82FD38E9BF97`; capabilities `CQL-CORE-BMP-001`, `CQL-CORE-BEN-001`, `CQL-CORE-SNP-001`, `CQL-SRV-PER-002`, `CQL-SRV-PER-004`; authority `AUTH-009`; decisions EQ-5, EQ-7, CS-3, CS-4, CS-7.

**Current accepted decisions.** CS-3/CS-4/CS-7 define versioned bitmap membership, explicit snapshot metadata, and copy-and-promote. EQ-5/EQ-7 prescribe adaptive MVCC GC and warm storage above a threshold, but those prescriptions lack implementation evidence at the pinned pair.

**Competing evidence.** The paper claims hot Roaring, warm Arrow, cold Parquet, automatic promotion/demotion, broad millisecond/sub-microsecond performance, and MVCC snapshot isolation. Core implements set/Roaring in-memory snapshots and carries one committed 10M-row benchmark scenario that Phase 3 did not regenerate. Arrow/Polars are result conversions, no warm/cold tier manager exists, and server evidence proves transactional copy-and-promote in SQLite—not general query-start snapshot isolation or adaptive MVCC GC.

**Options.** (A) Keep broad claims as architectural shorthand. (B) split every passage into implemented reference behavior, measured scenario, and target architecture; reserve “MVCC/snapshot isolation” for a defined and tested concurrency contract. (C) prioritize implementation of the complete three-tier design before revising prose.

**Recommendation.** Choose B. Describe SQLite atomic publication and last-good-snapshot behavior precisely. Use “versioned immutable snapshots/copy-and-promote” instead of MVCC unless query-lifetime pinning and reclamation are demonstrated. Report performance only with dataset, hardware, software versions, command, statistic, and scope; do not turn bitmap complexity into end-to-end O(1) or universal latency.

**Consequences.** Marketing claims become narrower, while the architecture remains a roadmap candidate. EQ-5/EQ-7 should be marked target/superseded for the pinned release unless implementation and evidence are scheduled.

**Acceptance evidence.** Clean-environment benchmark reproduction with raw artifacts; concurrent reader/refresh tests prove the accepted isolation level and lifetime; storage inventory proves each claimed tier and movement policy; benchmark wording passes an evidence-link check; no unsupported asymptotic or platform-wide latency statement remains.

**Owner/state.** Storage/performance owner + server persistence owner; `recommended-awaiting-decision`.

## CQL-P5-PLAT-009 — Generic streaming versus connector synchronization

**Exact question.** Does v1 provide an embedded generic micro-batch/streaming engine, or only connector-specific ordered change-feed synchronization?

**IDs.** Finding `CQL-FND-HR-018`; claims `CQL-WP-3D6733377440`, `CQL-WP-4633954A88FF`, `CQL-WP-7ADB781864CA`, `CQL-WP-9FE16BD53DD7`, `CQL-WP-A608F1F3F3E1`, `CQL-WP-DF1416B35A07`; capabilities `CQL-CORE-OPT-001`, `CQL-SRV-DS-004`; authority `AUTH-006`; decisions OPS-3, CS-19.

**Current accepted decisions.** OPS-3 selects an embedded micro-batch processor for v1, while CS-19 limits incremental refresh to connector-managed ordered change feeds. These cannot both describe the pinned general platform without an explicit scope or supersession decision.

**Competing evidence.** OPS-3 and several paper passages commit v1 to an embedded one-second micro-batch processor, while later status/future passages defer streaming. CS-19 reserves incremental refresh for connector-managed ordered/idempotent change feeds. The DeepSee synchronizer implements bootstrap, folding, watermark durability, idempotency, and failure-safe publication, but it is connector-specific and not a generic streaming execution contract.

**Options.** (A) retain OPS-3 and complete a generic processor in the current roadmap. (B) supersede OPS-3’s v1 timing, document connector synchronization as the shipped bounded capability, and design generic streaming later. (C) generalize the DeepSee synchronizer immediately into a connector SDK while continuing to avoid query-stream semantics.

**Recommendation.** Choose B, with C as a roadmap candidate only after extracting a second connector. Do not call polling, watch-mode re-execution, scheduler refresh, or one connector’s change-feed fold “streaming.”

**Consequences.** Removes a false v1 differentiator and clarifies the actual strong capability. A future generic contract must specify ordering, deduplication, watermark/replay, deletes, schema evolution, backpressure, poison records, recovery, and delivery semantics.

**Acceptance evidence.** Current docs use “DeepSee incremental synchronization” or equivalent scoped term; OPS-3 is explicitly superseded/deferred; connector tests retain bootstrap/restart/idempotency/failure guarantees; a future generic capability has a protocol, second implementation, load/failure experiments, and acceptance SLOs before being called platform streaming.

**Owner/state.** Operations/connector owner + roadmap owner; `recommended-awaiting-decision`.

## CQL-P5-PLAT-010 — Audit integrity and evidentiary scope

**Exact question.** Is the server audit store operational event history, an append-only control, or tamper-evident compliance evidence, and which actor is the integrity boundary?

**IDs.** Finding `CQL-FND-HR-020`; claims `CQL-WP-E7665FC969E0`, `CQL-WP-5DEA2F35D7D6`, `CQL-WP-6F7A816BE852`, `CQL-WP-8E079CB7A4CB`; capabilities `CQL-CORE-AUD-001`, `CQL-SRV-AUD-001`, `CQL-SRV-AUD-002`; authority `AUTH-008`; decisions GQ-7, GQ-8.

**Current accepted decisions.** GQ-7 decides parameterized query storage and GQ-8 requires the served snapshot version. Neither defines an integrity boundary, verification protocol, retention enforcement, or protected trust anchor.

**Competing evidence.** The paper promises append-only Parquet, per-record previous hashes, retention, query parameter separation, snapshot versions, and compliance investigation value. The server service appends filterable SQLite rows by convention; database-level mutation remains possible, no chain/signature/checkpoint verification exists, routes are unauthenticated, and the evidence does not establish complete event coverage or protected retention. GQ-7/GQ-8 decide content but not integrity architecture.

**Options.** (A) Keep SQLite as an operational log and make no compliance-evidence claim. (B) add a local hash chain to SQLite. (C) define a tamper-evident profile with canonical serialization, authenticated append, protected keys or external anchoring, verification/checkpoint/export, retention, and monitored failure handling.

**Recommendation.** Choose A as the immediate released scope and C as the required design before compliance-evidence language returns. A local hash chain alone is insufficient when the same administrator can rewrite data and recompute the chain; the threat model must state the protected actor and trust anchor.

**Consequences.** Current audit remains useful for observability and debugging but not independent proof. The stronger profile affects schema, keys, operations, migration, erasure/retention tension, availability, and external review.

**Acceptance evidence.** Immediate docs say operational event log and enumerate known coverage/limitations. For the integrity profile: approved threat model; canonical event schema; authenticated writers/readers; immutable or externally anchored checkpoints; mutation/deletion/reorder/truncation/replay tests; restart/key-rotation/export verification; retention and privacy review; independent security assessment.

**Owner/state.** Security design authority + audit/compliance owner; `recommended-awaiting-decision`.

## CQL-P5-PLAT-011 — Server README and control-plane maturity

**Exact question.** What implemented server surface should the README describe, and what maturity boundary prevents a single-node reference control plane from being mistaken for a production distributed service?

**IDs.** Finding `CQL-FND-HR-032`; claims `CQL-WP-D8998113046B`, `CQL-WP-506FAB66AF2A`, `CQL-WP-BEEC4169BEDA`, `CQL-WP-24B6558AE9C0`, `CQL-WP-8105C519874A`, `CQL-WP-7DBCE3652446`, `CQL-WP-29E1F1769037`, `CQL-WP-A57576D122D9`; capabilities `CQL-SRV-API-001`, `CQL-SRV-QRY-001`, `CQL-SRV-CAT-001`, `CQL-SRV-PRO-002`, `CQL-SRV-ID-001`, `CQL-SRV-AUD-001`, `CQL-SRV-VER-001`; evidence `P3-API-001`; authority `AUTH-005`.

**Current accepted decisions.** No accepted decision declares the observed server routes stable or production-ready. Phase 0 limits the audit to repository evidence, and M6 requires deployment, security, migration, and service-level evidence not present here.

**Competing evidence.** Generated OpenAPI exposes 27 operations across health, query, explain, context catalog/lifecycle/versions/preview/refresh/snapshots/history, providers, identity maps, and audit. The README documents mainly health/query. Persistence, limits, scheduler, recovery, and DeepSee hardening are stronger than the README suggests, while authentication, API versioning, active provider activation/health, multi-node deployment, migrations, and external operational evidence remain incomplete. Only JSON output exists; paper claims NDJSON/CSV/Arrow, gRPC/Flight, and JDBC/ODBC.

**Options.** (A) Keep README minimal until the API is stable. (B) document the observed surface as a single-node reference control plane, label endpoint stability, and list explicit non-capabilities. (C) document every route as stable public API now.

**Recommendation.** Choose B. Generate the route inventory from OpenAPI, but hand-author semantics, failure behavior, persistence boundary, and maturity. Document `/health` as implemented without implying readiness/liveness or dependency probes beyond its fields.

**Consequences.** Users can discover implemented value without confusing M4/M5 repository evidence with M6 production maturity. This may expose unstable endpoints; each must be marked experimental or governed by the API decision packet.

**Acceptance evidence.** README route inventory matches generated OpenAPI; examples cover supported operations and failure cases; deployment section states single-node SQLite/reference scope, durability boundary, limits, scheduler behavior, backup/migration gaps, auth absence, and provider/identity limitations; claimed formats/transports match implementation; CI detects route/documentation drift.

**Owner/state.** Server documentation owner + API owner; `factual-truth-repair`.

## Cross-packet ordering and closure gate

1. Apply CQL-P5-PLAT-001 and CQL-P5-PLAT-011 truth repairs before republishing the whitepaper or server maturity claims.
2. Decide lifecycle and explain safety before treating the server catalog/query surfaces as stable.
3. Decide API/version identity before publishing compatibility or deprecation commitments.
4. Decide provider and identity scope before describing the server as a federation broker.
5. Decide audit integrity before making any compliance-evidence claim.
6. Decide storage/performance and streaming scope before consolidating the roadmap.

No affected traceability row moves to `matched` merely because a recommendation is accepted. It moves only after the relevant prose/code/spec disposition is complete and the packet’s acceptance evidence is attached.
