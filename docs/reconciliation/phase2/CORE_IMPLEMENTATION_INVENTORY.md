# Phase 2 — Core `contextql` Implementation Inventory

## Baseline and method

This inventory examines core commit `a054c8fcc576f3913d98d664ddf71eeea56d9755` on reconciliation branch `agent/whitepaper-reconciliation-phase-4`. The machine-readable inventory is [`core_capabilities.csv`](core_capabilities.csv).

The inventory was built in both directions:

1. forward from grammar to parser, semantic lowering, validation, execution, storage/repository boundaries, public APIs, and tests;
2. backward from exported Python symbols, CLI entry points, notebook magics, LSP handlers, error codes, engine options, provider protocols, snapshot/history APIs, benchmarks, and examples.

Static presence was not treated as execution evidence. Each row records whether a capability is only a surface (`M2`), locally executable (`M3`), integrated across relevant boundaries (`M4`), or verified and hardened with meaningful negative cases (`M5`). No core capability is assessed as `M6`: the repository contains no production service-level, deployment, migration, security, or sustained operational evidence sufficient for that designation.

## Capability profile

The register contains 43 atomic implementation capabilities:

| Maturity | Count | Interpretation in this inventory |
|---|---:|---|
| M2 | 8 | Syntax/model/interface exists, but the public execution path stops before semantics are realised. |
| M3 | 6 | A local happy path executes, with a material semantic, verification, or operational limitation. |
| M4 | 19 | The reference-engine path is integrated across its relevant in-process boundaries. |
| M5 | 10 | The capability has strong positive and negative regression evidence for the reference implementation. |
| M6 | 0 | No operationally proven core capability was identified. |

The strongest core vertical slices are:

- executable context DDL with dependency and option validation;
- immutable membership snapshots, atomic promotion, history, retention, and compatibility checks;
- set and Roaring64 storage with snapshot/delta algebra and serialization;
- bitmap membership pushdown and bounded failure before large DataFrame materialization;
- MCP/REMOTE provider contracts, timeouts, entity filtering, and reference integration;
- exact-path identity-map bridging;
- the synchronous embedded Python API and query builder;
- a deterministic, provenance-checked 10M post-trade benchmark scenario.

## End-to-end stage inventory

| Stage | Reachable implementation | Important boundary |
|---|---|---|
| Grammar | Broad SQL-like and ContextQL grammar in `grammar/contextql.lark` | The file calls itself a non-normative-complete scaffold; accepting syntax is not evidence of execution. |
| Parser | Lark Earley parser with positions and normalized E001 failures | E002–E004 are registered but not emitted distinctly by the parser. |
| Semantic lowering | Explicit models for SELECT, context DDL, event-log create, and process-model create | Several accepted grammar statement types lower to `UNKNOWN`. |
| Validation | Tree linter plus a smaller structured analyzer | Neither layer is a complete implementation of the error registry or type system. |
| Query execution | Hybrid DuckDB base SQL plus context filtering/scoring in pandas, with bitmap pushdown where eligible | This is not the whitepaper's full logical/physical planner, cost optimizer, or Volcano pipeline. |
| DDL execution | CREATE/ALTER/DROP/SHOW/DESCRIBE/VALIDATE/REFRESH CONTEXT | Event-log, process-model, provider-registration, namespace, grant, and settings DDL are not executable. |
| Persistence boundary | Injectable `ContextCatalogRepository` protocol | The embedded default repository is deliberately no-op; durable behavior is supplied by the server. |
| Storage | Versioned set/Roaring snapshots, scores, deltas, history, serialization | No Arrow warm tier, Parquet cold tier, or automatic tier movement exists in core. |
| Public API | `Engine`/`ContextQL`, `Result`, builder, decorator, providers, parser/linter | No `contextql.aio`, stable structured exception hierarchy, or generic service client exists. |
| Tooling | Narrow CLI, notebook magics, offline LSP, VS Code client | CLI and LSP are behind the feature sets documented in the whitepaper/tooling specifications. |

## Reverse inventory of public and extension surfaces

### Python SDK

The top-level package exposes a credible synchronous embedded SDK:

- `Engine` and its `ContextQL` alias;
- table, live-context, snapshot-context, MCP, REMOTE, and identity-map registration;
- `execute`, dry `explain`, DDL audit callbacks, fluent query construction, and context decorators;
- `Result` conversion to pandas and optional Arrow/Polars, plus diagnostics and trace metadata;
- provider dataclasses and runtime-checkable protocols.

The principal reverse-inventory gaps are the documented async API, structured `ContextQLError` hierarchy, persistent embedded catalog, and a public API for catalog metadata richer than lists of names.

### CLI and notebook integration

`cql` supports a bare or demo REPL, `.cql` file execution, explain, and table/JSON/CSV output. It does not implement the whitepaper's watch mode, native test runner, parse/validate commands, provider health commands, or the richer documented REPL command set. File execution uses string splitting on semicolons, so semicolons inside valid literals or more complex scripts are not handled through the parser.

The IPython extension registers `%%cql`, `%cql_setup`, and `%cql_contexts`. It is reachable but has no dedicated automated test suite, prints errors instead of surfacing structured notebook diagnostics, and contains an unused empty line-magic helper.

### Language server

The pygls server has implemented diagnostics, keyword completion, in-memory catalog completion, keyword/catalog hover, and regex-based document symbols. The test suite directly verifies these helpers. It does not implement go-to-definition, live database discovery, cached metadata fallback, provider metadata, or the response-time evidence required by `docs/LANGUAGE_SERVER_SPEC.md`.

### Errors and diagnostics

The central error registry is useful and substantially exercised, especially for context option, snapshot, history, and bounded-federation failures. It is not yet the single effective error authority: parser errors collapse to E001, some runtime paths construct bracketed strings directly, several registered codes have no reachable emitter, and ordinary exceptions remain `ValueError`/`RuntimeError`. W100/W101 and the absent whitepaper E300/E400 meanings require the decision-docket reconciliation already identified in the methodology.

### Configuration and operational options

Public `Engine` options currently cover:

- DuckDB database location;
- MCP and REMOTE timeouts;
- MCP timeout behavior (`warn` or `error` in practice);
- default namespace;
- injected catalog repository, membership store, and history store;
- maximum intermediate rows.

Context DDL options cover materialization/storage, refresh mode/interval, staleness, history/retention, and a source watermark. Options for scheduled operation are metadata in embedded core; scheduling is a server responsibility. Native incremental refresh is explicitly rejected. There are no core limits for result bytes, context count, provider fan-out, concurrency, or total memory.

### Provider and identity contracts

MCP and REMOTE are deliberately separate runtime contracts. MCP returns membership and optional scores; REMOTE returns relational data. The implementation now additionally supports Roaring64 provider membership payloads, bounded `EntityFilter` pushdown, freshness/watermark/cursor metadata, and execution trace records.

The reverse inventory also limits the identity claim precisely: `register_identity_map` supplies exact pairs of `table.column` paths used to select an existing result column. It is not a canonical global entity map, does not evaluate confidence, does not enforce key types, and has no temporal or probabilistic matching.

### Snapshots, history, and temporal behavior

Materialized native contexts stage a complete new membership version, call the repository's atomic promotion boundary, then publish it in memory. A failed build or promotion leaves the previous version current. Definition replacement invalidates the current pointer; rename preserves identity; drop/recreate assigns a new identity. Optional history stores added, removed, and score-changed events, with pruning anchored by retained snapshots.

The executable temporal meaning is therefore membership-history resolution:

- `AT VERSION n` selects a stored membership version;
- `AT timestamp` replays membership history to a point;
- `BETWEEN start AND end` returns entities present at any time in the interval and their maximum observed score.

This is a material divergence from the earlier OQ-9 event-time-column interpretation and must be adjudicated rather than silently documented as settled.

## High-priority partial and divergent slices

### `THEN` is intersection, not staged evaluation

The parser and semantic model retain `sequence_mode`, but executor membership combines every reference mask with boolean intersection. The executor itself records that the later context is not evaluated only over the candidates produced by the earlier stage. The surface is `M3`, not aligned with the whitepaper's staged, left-associative semantics.

### Score strategies are advertised beyond implementation

The grammar accepts `MAX`, `MIN`, `AVG`, `SUM`, `COUNT`, `WEIGHTED_MAX`, and `WEIGHTED_SUM`, while scoring uses fixed accumulation/intersection behavior and contains an explicit incomplete-strategy TODO. Strategy-specific end-to-end conformance tests are absent. Membership filtering is mature; score algebra is not.

### Composite contexts stop before resolution

`COMPOSE` definitions have models, hashes, dependency validation, and catalog entries. They are not registered as live adapter contexts, do not build composite snapshots during refresh, and cannot be queried successfully. This is a clear `M2` partial vertical slice.

### Native context parameters stop at models

Typed declarations and named invocation bindings parse and lower. Invocation parameters flow to MCP provider calls, but native context arguments are not validated against declarations or substituted into definition execution. Documentation must distinguish provider parameters from native parameterized contexts.

### Lifecycle is metadata only

Core implements four accepted strings, and CREATE DDL immediately marks new contexts active. There is no transition graph and no query visibility gate. A direct public probe confirmed that a context changed to `retired` remained queryable. The whitepaper's nine-state governed lifecycle is not implemented in core.

### Process and security surfaces are scaffolds

Event-log and process-model create statements reach semantic models and validation but are rejected by the executor. Their companion DDL is mostly parse-only. Provider registration, namespaces, grants, and settings are grammar-only. There is no core authorization, RLS, tenant isolation, security package, process catalog, or process execution engine.

### SQL conformance is narrower than claimed

ContextQL cannot pass arbitrary standard SQL through unchanged because the Lark grammar and semantic lowerer sit in front of DuckDB. The grammar is a practical subset and the executor rebuilds supported SQL shapes. The whitepaper's SQL:2016 declaration needs a bounded conformance profile and fixtures.

## Executable evidence collected during inventory

The installed reconciliation environment used Python 3.12 with the repository's `dev`, `executor`, `arrow`, `roaring`, `polars`, and `lsp` extras.

The suite was run in bounded groups because one aggregate invocation was interrupted by the desktop runner after collecting all 556 tests. The grouped passes provide complete file-level coverage without relying on that interrupted aggregate run:

- parser, semantic models, executor, context DDL, and bitmap pushdown: **221 passed**;
- hardening, snapshot lifecycle, membership stores, MCP/REMOTE, and provider contract: **124 passed, 1 skipped**;
- builder, result, linter, diagnostics, LSP, grammar extensions, identity, federation integration, provider package, benchmark evidence, and post-trade dataset/definition: **209 passed, 1 failed**. The sole failure was `TestOptionalOutputFormats.test_to_polars_raises_without_polars`: with Polars installed but not yet imported, the test expects `ImportError` by inspecting `sys.modules`, while `Result.to_polars()` correctly imports the installed package. This is an environment/order-fragile test, and is retained as a test-evidence gap.

A focused public-API probe additionally observed:

- a retired context still returned one matching row;
- `CREATE EVENT LOG` was rejected as unsupported by the executor;
- `CREATE NAMESPACE` was rejected as unsupported by the executor;
- `SHOW CONTEXTS; DROP CONTEXT a;` executed only the first statement and left `a` registered;
- querying a catalogued `COMPOSE` context failed with `Unknown context`.

These failures are evidence of precise reachability boundaries, not test-suite defects.

## Maturity conclusions

- Parser, semantic lowering, reference SELECT execution, Python API, DuckDB adapter, identity bridging, trace, and dry explain: generally **M4**.
- Context option validation, dependency safety, snapshot/history mechanics, membership stores, bitmap pushdown, bounded execution failures, and scoped benchmark evidence: generally **M5** for the single-process reference implementation.
- `THEN`, scoring strategies, lifecycle enforcement, CLI, and notebook UX: **M3** because important semantics or verification are missing.
- Composite contexts, native parameters, event logs, process models, security/namespace/provider-registration DDL, and the duplicate type model: **M2**.
- Three-tier storage, full optimizer/planner architecture, async SDK, general SQL:2016 conformance, process execution, security enforcement, multi-tenancy, and distributed operation are not implemented as core vertical slices.

The central Phase 4 joins should therefore avoid a binary “implemented” label. The core code has evolved significantly beyond the whitepaper in snapshots, history, bitmap pushdown, bounded REMOTE federation, provider payload contracts, and provenance. At the same time, several of the whitepaper's most prominent language and operational claims remain partial or divergent.
