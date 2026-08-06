# Phase 2 — `contextql-server` Implementation Inventory

## Baseline

This inventory examines `contextql-server` commit `78c9565c33237a21dbf87f11d92ac6c7f29a846e` on `main`. It is a static implementation inventory; Phase 3 supplies freshly executed evidence.

The machine-readable inventory is in `server_capabilities.csv`.

## Capability profile

The server is substantially beyond the thin HTTP wrapper described by its README. It now contains a credible reference control-plane vertical slice:

- durable SQLite context definitions and versions;
- versioned snapshot payloads, checksums, history, and restart hydration;
- context lifecycle, validation, preview, refresh, snapshot, and history APIs;
- persistent provider and identity registries;
- identity-map synchronization into the core engine;
- query tracing and an explain response surface;
- audit recording;
- a background refresh scheduler;
- explicit result, intermediate-row, and response-byte limits;
- a hardened DeepSee reference connector with MCP, REMOTE, snapshot, and change-feed roles.

The strongest evidence is in persistence and failure handling. The test suite contains restart, corruption, transaction-failure, atomic-publication, simultaneous-refresh, and synchronizer-idempotency cases.

## Public route inventory

The application exposes unversioned JSON routes for:

- `GET /health`;
- `POST /query`;
- `POST /query/explain`;
- context create/list/describe/update/delete;
- context validate/activate/retire;
- context versions/preview/refresh/snapshots/history;
- provider create/list/describe/update/enable/disable/health;
- identity-map create/list/describe;
- audit-log query.

This is ahead of `contextql-server/README.md`, which describes mainly query and health, but behind the whitepaper's versioned REST, multi-format output, gRPC/Flight, and JDBC/ODBC vision.

## Important partial or divergent capabilities

### Provider registry is not yet a runtime broker

Provider records persist metadata and stored health, but startup registers only built-in/mock providers through separate code. There is no provider-registry hydration path corresponding to identity-map synchronization. Consequently, “persistent provider registry” is accurate; “server-owned federation broker routing through registered providers” is not yet established end to end.

### Identity is metadata plus exact path mapping

The registry records matching mode and confidence, but engine synchronization passes path pairs into the core identity map. This is not the whitepaper's global entity namespace or confidence-based identity resolution.

### Explain currently executes

`POST /query/explain` invokes normal query execution and returns actual rows plus trace data. It is therefore an execution-and-trace endpoint, not a dry plan. The behavior requires an explicit safety and semantic decision.

### Lifecycle behavior is inconsistent across layers

The server exposes the implemented four-state model. Service-level create uses core DDL and immediately changes the new context to draft, while core DDL initially creates an active context. The current engine does not enforce the whitepaper's nine-state lifecycle as a query-visibility gate.

### Security remains architectural

Namespaces, classification, confidence, trust tier, and credential references are stored metadata. No authentication dependency, RBAC, row-level security, tenant enforcement, or audit integrity chain is wired into HTTP execution.

### Some request fields are not effective

`ContextUpdate` accepts `entity_key`, `has_score`, and `classification`, but `CatalogService.update` applies definition, description, tags, and score-column changes only. Public model presence should not be counted as effective update support.

## Maturity assessment

- Query, catalog, persistence, refresh, history, and DeepSee reference integration: generally **M4–M5** for the reference single-node implementation.
- Provider registry as a generic operational broker: **M2**.
- Identity registry as global/probabilistic resolution: **M2**.
- Security, authorization, RLS, and tamper-evident audit: **M1** architectural/design state.
- Multi-node federation, distributed scheduling, gRPC/Flight, and JDBC/ODBC: absent from the server implementation.

No claim in this inventory should be elevated to M6 without deployment, security, migration, and service-level evidence beyond the repository test suite.
