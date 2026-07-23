# Post-Trade Correctness Hardening

**Branch:** `post-trade-correctness-hardening`

**Repositories:** `contextql`, `contextql-server`

**Status:** Approved implementation plan; no runtime changes in this plan

**Depends on:**

- `contextql` `main` at or after `e4141ab`
- `contextql-server` `main` at or after `ae8e833`
- `docs/plans/post-trade-roaring-contexts.md`

**Last reviewed:** 2026-07-23

## 1. Objective

Close every P1 and P2 gap found after the first post-trade implementation,
without requiring the implementation agent to redesign lifecycle, persistence,
federation, storage, temporal, or benchmark behavior.

The completed work must make these claims true:

1. A query can never use a snapshot produced by an incompatible definition.
2. Context DDL, current snapshot pointers, snapshot payloads, history, and
   connector synchronization state survive a server restart.
3. REMOTE evidence for a context-filtered query is fetched only for entities
   surviving context algebra.
4. `storage`, `refresh_mode`, `source_watermark`, `history_retention`,
   namespaces, and temporal qualifiers have executable, tested semantics.
5. Roaring refresh and delta operations do not expand an entire bitmap into a
   Python `set`, `list`, Pandas object, or single NumPy array.
6. Snapshot publication is atomic for concurrent readers.
7. Every result reported by the 10-million-row proof is checked against
   reference SQL.

This is a correctness and hardening branch. Do not add a real DeepSee
transport, dashboard, non-integer bitmap dictionary, or unrelated language
feature.

## 2. Review findings mapped to work packages

| Priority | Finding | Work package |
|---|---|---|
| P1 | Definition replacement, alteration, drop/recreate, and rename can expose stale or unreachable snapshots | WP1 |
| P1 | Server persistence is disconnected from executable DDL and membership state | WP2 |
| P1 | REMOTE evidence is materialized before bitmap narrowing | WP3 |
| P2 | Per-context storage and refresh options are not honored | WP4, WP5 |
| P2 | Roaring deltas and full refreshes expand complete memberships into Python collections | WP4 |
| P2 | Namespaces are parsed but not used as catalog identity | WP1 |
| P2 | Temporal qualifiers are parsed but not executed | WP6 |
| P2 | Snapshot promotion is not atomic under concurrent access | WP4 |
| P2 | Benchmark cross-checks and continuous 10M validation are incomplete | WP7 |

No finding may be closed merely by changing documentation. Each work package
has required executable tests and acceptance criteria.

## 3. Normative decisions

These decisions remove implementation ambiguity and supersede any conflicting
behavior in the first implementation.

### 3.1 Context identity and names

1. `context_id` is an immutable UUID generated at first creation.
2. Membership, scores, snapshots, history, and synchronizer state are keyed by
   `context_id`, never by display name.
3. The catalog key is `(normalized_namespace, normalized_name)`.
4. Unqualified names use the engine's `default_namespace`, whose default is
   `default`.
5. Name comparison is case-insensitive. Original spelling is retained for
   display.
6. Rename changes only namespace/name identity and catalog version. It retains
   `context_id`, snapshots, history, and current snapshot pointer.
7. Drop tombstones the catalog object and removes it from name resolution. It
   does not delete immutable snapshots or history during the DDL transaction.
8. Recreating a dropped name creates a new `context_id`; it cannot inherit the
   dropped object's current snapshot.

Add a single canonical helper, used everywhere:

```python
@dataclass(frozen=True)
class QualifiedContextName:
    namespace: str
    name: str

    @property
    def key(self) -> tuple[str, str]: ...

def qualify_context_name(
    value: str,
    *,
    default_namespace: str = "default",
) -> QualifiedContextName: ...
```

Do not duplicate dotted-name parsing in the semantic layer, DDL executor,
query executor, server service, or migration repository.

### 3.2 Definition versions and snapshot eligibility

`ContextCatalogEntry.version` is the definition/catalog version.
`MembershipSnapshot.version` is the snapshot version. They are independent.

An unqualified query may use a materialized snapshot only when all conditions
below hold:

```text
entry.current_snapshot_version is not null
snapshot.context_id == entry.context_id
snapshot.version == entry.current_snapshot_version
snapshot.definition_hash == entry.definition_hash
snapshot.state == "current"
snapshot payload exists and passes its checksum
```

Use one resolver for every execution path:

```python
resolve_snapshot(
    entry,
    temporal_qualifier=None,
) -> ResolvedSnapshot
```

The snapshot-state gate, bitmap pushdown, score lookup, trace generation, and
legacy compatibility path must all call this resolver. No caller may obtain a
current snapshot directly by context name.

`CREATE OR REPLACE CONTEXT` and `ALTER CONTEXT ... SET DEFINITION`:

1. Retain `context_id`.
2. Increment the definition version.
3. calculate and persist the new definition hash.
4. Set `current_snapshot_version = NULL`.
5. Leave previous immutable snapshots available only to explicit temporal or
   snapshot-version queries.
6. Make ordinary queries fail with E200 until a successful refresh promotes a
   compatible snapshot.

Context creation and definition replacement do not run an implicit refresh.
`REFRESH CONTEXT` is the only synchronous language operation that builds a
native snapshot. The server scheduler and connector synchronizer may invoke
the same refresh/promotion service.

### 3.3 Single write path

Executable ContextQL DDL is the only authoritative catalog mutation path.

- The engine owns validation, dependency checking, lifecycle rules, definition
  hashing, versioning, and audit event creation.
- An injected repository owns durable storage.
- `CatalogService` is a REST facade over engine commands and repository reads.
- `CatalogService` must not insert/update/delete `contexts` directly.
- `activate()` and startup must not call legacy `register_context()`.
- `register_context()` remains a documented in-memory compatibility API for
  embedded applications; the server must not use it.

The server must construct the database and repositories before constructing
the engine, inject them into `Engine`, then load persisted catalog and snapshot
state. Startup must not reconstruct definitions through a second semantic
path.

### 3.4 Storage selection

Storage is selected per snapshot:

| Requested value | Required behavior |
|---|---|
| `set` | Always use the set representation |
| `roaring` | Require a non-negative integer key and installed Roaring backend; otherwise fail, never fall back |
| `auto` | Use Roaring for supported integer keys when installed; otherwise use set |

Persist the resolved `storage_kind` on every snapshot. Existing snapshots are
read using their recorded kind, even if the context option later changes.

`contextql-server` production dependencies must install
`contextql[executor,roaring]`. Roaring remains optional for parser/LSP-only
`contextql` installations.

### 3.5 Refresh modes

The supported behavior for this branch is:

| Mode | Native SQL context | Connector-managed context |
|---|---|---|
| `manual` | `REFRESH CONTEXT` performs a full refresh | Explicit bootstrap/full resync |
| `scheduled` | Server scheduler performs the same full refresh at `refresh_interval` | Server scheduler calls connector full resync |
| `incremental` | Reject with E161; arbitrary SQL cannot infer removals safely | Apply ordered provider change events |

Do not pretend that filtering a native SELECT by a watermark provides correct
incremental semantics: rows that stop satisfying the predicate would be
missed. Native incremental refresh requires a future explicit change-feed
contract and is outside this branch.

Change the post-trade native context definition to
`refresh_mode = 'scheduled'`. Keep the DeepSee connector-managed context
incremental.

### 3.6 Source watermark

`source_watermark` in DDL names a projected definition column. During native
full refresh:

1. Validate that the result contains the named column.
2. Track the maximum non-null value while reading result batches.
3. Persist that observed value on the snapshot.
4. On an empty result, retain the preceding watermark, or `NULL` for the first
   snapshot.

Never store the configured column name as the observed watermark.

Connector snapshots use the provider's committed cursor/watermark. Commit it
in the same promotion transaction as the snapshot pointer.

### 3.7 History retention

`history = FALSE` means no membership-change derivation and no read of the
previous complete membership merely for history.

When `history = TRUE`, each successful promotion appends additions, removals,
and score changes. When `history_retention` is set:

1. Compute `cutoff = now - retention`.
2. Retain the newest complete snapshot whose `data_as_of <= cutoff` as an
   anchor.
3. Retain all later snapshots and history events.
4. Delete only snapshots/events older than the anchor.
5. Set `history_available_from` to the anchor's `data_as_of`.
6. Reject an earlier temporal query with E202.

Retention runs after successful promotion, outside the promotion transaction.
A retention failure is audited and retried; it must not roll back the new
current snapshot.

### 3.8 Temporal semantics

Add language support for explicit snapshot versions:

```sql
context_name AT VERSION 3
```

Existing qualifiers retain these semantics:

- `AT <timestamp>` returns membership effective at that instant.
- `BETWEEN <start> AND <end>` returns entities that were members at any instant
  in the inclusive interval.
- For `CONTEXT_SCORE()` with `AT`, use the score effective at that instant.
- For `CONTEXT_SCORE()` with `BETWEEN`, use the maximum score observed while
  the entity was a member during the interval.
- Reject `start > end` with E203.
- Reject temporal qualifiers on non-temporal contexts with E109.
- Reject timestamps older than `history_available_from` with E202.

Temporal reconstruction starts from the nearest retained anchor snapshot and
replays ordered changes by:

```text
effective_at, recorded_at, history_event_id
```

For full native refresh:

- Addition and score-change `effective_at` use the row's declared TEMPORAL
  column.
- Removal `effective_at` uses the refresh `data_as_of`, because the removed
  row no longer appears in the definition result.
- `recorded_at` is the promotion time.

Round comparison timestamps to the declared granularity in UTC. Reject naive
or invalid timestamp values rather than applying the server's local timezone.

### 3.9 REMOTE narrowing

For a query containing both a local/native context predicate and a REMOTE
equi-join:

1. Resolve snapshots.
2. Compose context membership.
3. Build the member relation used for local DuckDB filtering.
4. Derive the remote join key from the equality join.
5. Pass the surviving membership to the REMOTE provider as an entity filter.
6. Fetch REMOTE rows.
7. Register the bounded result and execute the final join/order/limit.

Extend the provider contract compatibly:

```python
@dataclass(frozen=True)
class EntityFilter:
    column: str
    entity_ids: Sequence[int] | None = None
    membership_bitmap: bytes | None = None
    bitmap_encoding: str | None = None

class RemoteProvider(Protocol):
    def query(
        self,
        resource: str,
        filters: dict,
        columns: list[str],
        limit: int | None = None,
        *,
        entity_filter: EntityFilter | None = None,
    ) -> RemoteResult: ...
```

Exactly one membership representation is present. Use IDs only below a
configurable threshold, default 10,000; otherwise use portable `roaring64`.

Rules:

- Push only columns required by projection, join, filtering, ordering, and
  tracing.
- Do not pass the final query `LIMIT` as a remote limit when it could change
  join correctness.
- If a context-filtered REMOTE join cannot be recognized as an entity-key
  equality join, fail with E301 instead of doing an unbounded fetch.
- If the provider does not accept entity filters, fail with E302.
- A standalone REMOTE query without a context predicate retains existing
  behavior, subject to provider limits.

The DeepSee mock must record requested entity IDs and return evidence only for
those IDs. Tests must assert the provider call cardinality, not merely final
row count.

### 3.10 Atomicity and concurrency

In-memory stores use an `RLock` and immutable snapshot-state replacement.
Readers capture one `ResolvedSnapshot` handle and use it for the whole query.

Persistent promotion uses one SQLite `BEGIN IMMEDIATE` transaction:

1. Insert payload and checksum.
2. Insert snapshot metadata in `building`.
3. Validate payload cardinality/checksum.
4. Mark the preceding current snapshot `superseded` and set `valid_to`.
5. Mark the new snapshot `current`.
6. Update `contexts.current_snapshot_version`, `data_as_of`,
   `last_refreshed_at`, and committed watermark.
7. Append history and synchronizer idempotency rows.
8. Commit.

Definition evaluation and bitmap building happen before this transaction.
Failed evaluation records a refresh failure without changing the current
pointer. Readers either observe the complete old state or complete new state;
they must never observe metadata without payload.

## 4. Core API and data-model changes (`contextql`)

### 4.1 Files to add

```text
contextql/context_names.py
contextql/catalog_repository.py
contextql/snapshot_resolution.py
contextql/snapshot_codec.py
tests/test_context_identity.py
tests/test_snapshot_compatibility.py
tests/test_temporal_execution.py
tests/test_remote_narrowing.py
tests/test_snapshot_concurrency.py
```

Names may differ only if an existing module is the clearly correct owner.
Do not place SQLite code in `contextql`.

### 4.2 Files to change

#### `contextql/semantic.py`

- Use `QualifiedContextName` for every context DDL target and reference.
- Make catalog lookup namespace-aware.
- Give `ContextCatalogEntry.namespace` the non-null default `default`.
- Add `source_kind`, `history_available_from`, and `last_refresh_error`.
- Preserve separate definition version and snapshot version fields.
- Lower `AT VERSION <integer>`.

#### `contextql/context_ddl.py`

- Replace direct `catalog.contexts[...]` mutations with repository-backed
  catalog commands.
- Key membership operations by `context_id`.
- Apply the invalidation, rename, drop, and recreation rules in section 3.
- Route refresh through one snapshot builder and promotion service.
- Remove unconditional `membership.members()` calls.
- Read definitions in batches and track observed watermark.
- Emit audit events containing namespace, context ID, definition version,
  definition hash, snapshot version, and outcome.

#### `contextql/membership.py`

- Expand the protocol with resolved storage kind, snapshot handles,
  invalidation, version lookup, native algebra, and batched ID iteration.
- Make the set store thread-safe.
- Keep immutable prior snapshots.
- Never expose a mutable internal score map.
- Add checksum-aware serialization helpers.

Required operations:

```python
get_snapshot(context_id, version=None)
get_snapshot_at(context_id, timestamp)
put_snapshot(..., storage_kind)
apply_delta(...)
compose(...)
iter_member_batches(context_id, version, batch_size=65_536)
scores_for(context_id, version, entity_ids=None)
invalidate_current(context_id)
```

#### `contextql/membership_roaring.py`

- Override `apply_delta()` using `BitMap64` copy/update/difference operations.
- Override full-refresh comparison using bitmap algebra.
- Implement batched iteration with at most 65,536 IDs resident per batch.
- Do not call `_to_set()` in refresh, delta, compose, pushdown, or history
  derivation.
- Retain `_to_set()` only for explicit compatibility/testing APIs.

#### `contextql/history.py`

- Add anchor-based temporal reconstruction.
- Accept bitmap/native membership inputs without converting both complete
  states to sets.
- Add `events_between`, `membership_at`, `membership_between`, `score_at`,
  `max_score_between`, and `prune`.
- Include definition version/hash and stable event ID in history records.

#### `contextql/executor.py`

- Resolve snapshots once and retain handles for the query.
- Reorder execution so local membership planning precedes REMOTE calls.
- Replace Pandas membership tables with a DuckDB-readable Arrow
  `RecordBatchReader` or equivalent bounded batch relation.
- Apply temporal membership before algebra.
- Add remote entity-filter planning.
- Add trace fields for qualified name, context ID, definition hash, snapshot
  version, temporal qualifier, storage kind, provider request cardinality, and
  observed `data_as_of`.
- Delete or constrain any path that fetches a full base result and then calls
  `Series.isin()`.

#### `contextql/adapters/duckdb_adapter.py`

- Add `execute_batches(sql, batch_size=65_536)`.
- Add registration of a bounded/lazy member-batch relation.
- Ensure temporary relations are unregistered in `finally`.

#### `contextql/context_options.py`

- Add E161 validation for incremental native SQL definitions.
- Validate projected watermark and temporal columns during `VALIDATE` or
  refresh, when the result schema is available.
- Preserve existing strict option validation.

#### `contextql/providers/base.py`

- Add `EntityFilter`.
- Extend `RemoteProvider.query` with keyword-only `entity_filter`.
- Add a capability check/helper so unsupported legacy providers fail clearly
  only when a bounded context join requires the new argument.

#### `contextql/__init__.py`

Extend `Engine.__init__` compatibly:

```python
Engine(
    ...,
    default_namespace="default",
    catalog_repository=None,
    membership_store=None,
    history_store=None,
)
```

Default to in-memory implementations. Hydrate injected state before accepting
queries. Expose no server-specific types.

#### Normative documentation

Update:

- `SPEC.md`
- `DECISIONS.md`
- `docs/architecture/BITMAP_CONTEXT_STORAGE.md`
- `docs/architecture/DEEPSEE_CONNECTOR.md`
- `grammar/contextql.lark` for `AT VERSION`
- `contextql/errors.py`

Add codes:

| Code | Meaning |
|---|---|
| E161 | Incremental refresh is unsupported for native SQL definitions |
| E162 | Explicit Roaring storage requested but backend unavailable |
| E201 | Snapshot payload is corrupt or incompatible |
| E202 | Requested temporal point predates retained history |
| E203 | Invalid temporal range |
| E301 | REMOTE context join cannot be safely narrowed |
| E302 | REMOTE provider does not support required entity filtering |

Continue using E200 for missing or definition-invalidated current snapshots.

## 5. Persistent implementation (`contextql-server`)

### 5.1 Files to add

```text
app/repositories/context_catalog.py
app/repositories/context_snapshots.py
app/repositories/membership_history.py
app/repositories/synchronizer_state.py
app/repositories/__init__.py
app/services/refresh_scheduler.py
app/db/migrations/003_context_runtime_state.sql
tests/test_context_persistence.py
tests/test_snapshot_persistence.py
tests/test_refresh_scheduler.py
tests/test_synchronizer_restart.py
tests/test_remote_data_minimization.py
```

### 5.2 Migration 003

Migration 003 must be additive and work on databases already at schema version
2.

Add to `contexts`:

```text
context_id TEXT
definition_hash TEXT
raw_ddl TEXT
materialization_json TEXT
current_snapshot_version INTEGER
last_refreshed_at TEXT
data_as_of TEXT
last_refresh_error TEXT
history_available_from TEXT
dropped_at TEXT
```

Backfill:

- Generate one UUID per existing `(namespace, name)` lineage and apply it to
  all its versions.
- Calculate definition hashes through application migration code if SQLite
  cannot reproduce the canonical hash. Migration startup may run a
  post-migration backfill before serving requests.
- Normalize null namespaces to `default`.

Add unique/index constraints:

```text
UNIQUE(context_id, version)
INDEX(namespace, name, dropped_at, version)
```

Extend `context_snapshots`:

```text
definition_version INTEGER
membership_sha256 TEXT
score_sha256 TEXT
```

Add:

```text
context_snapshot_payloads
  context_id TEXT
  version INTEGER
  membership_blob BLOB NOT NULL
  score_blob BLOB
  created_at TEXT NOT NULL
  PRIMARY KEY (context_id, version)

context_sync_state
  context_id TEXT PRIMARY KEY
  committed_watermark TEXT
  ordering_boundary TEXT
  updated_at TEXT NOT NULL

context_sync_events
  context_id TEXT
  event_id TEXT
  watermark TEXT NOT NULL
  applied_at TEXT NOT NULL
  PRIMARY KEY (context_id, event_id)
```

Add a partial unique index:

```sql
CREATE UNIQUE INDEX ... ON context_snapshots(context_id)
WHERE state = 'current';
```

Add `definition_version`, `definition_hash`, and stable `event_id` columns to
`context_membership_history`. Existing `context_version` is interpreted as
snapshot version; do not silently repurpose it.

Membership blobs use the existing `CQLM` versioned serialization. Encode scores
in a new versioned `CQLS` binary format:

```text
magic "CQLS"
format version uint8
entry count uint64
sorted repeated (entity_id uint64, score float64)
```

Hash the complete stored blob with SHA-256.

### 5.3 Repository behavior

Repositories use parameterized SQL only. Context and snapshot writes use
explicit transactions. Configure WAL, foreign keys, and a non-zero
`busy_timeout`.

The snapshot repository must:

- Load metadata without decoding all payloads.
- Decode only resolved snapshots.
- Validate hash and cardinality before returning a handle.
- Perform the promotion transaction in section 3.10.
- Retain superseded snapshots until retention removes them.

The catalog repository must:

- Append definition versions rather than overwrite history.
- Resolve only the latest non-dropped lineage by qualified name.
- Return old versions by `context_id`.
- Persist invalidation and rename atomically.

### 5.4 Startup and services

Change startup order in `app/main.py`:

```text
initialize database and run migrations
construct repositories
construct Engine with injected repositories/stores
hydrate catalog and current snapshot metadata
register configured providers
start refresh scheduler
wire API dependencies
accept traffic
```

Shutdown order:

```text
stop scheduler and await active refresh completion
flush audit work
close engine
close database resources
```

Change `CatalogService`:

- Reads use `ContextCatalogRepository`.
- Creates/updates/renames/state changes/deletes/refreshes invoke engine DDL.
- Remove direct context table writes.
- Remove `sync_active_to_engine()`.
- Remove server calls to `register_context()`.

Keep the REST API backward compatible:

- Legacy `ContextCreate`/`ContextUpdate` bodies are converted by one
  `ContextDDLBuilder` into canonical ContextQL DDL.
- Add optional `ddl` input for clients already supplying a complete statement.
- Reject a body containing both `ddl` and conflicting structured fields.
- Return `context_id`, definition hash, materialization options, current
  snapshot version, `data_as_of`, and last refresh error.

Add:

```text
POST /contexts/{name}/refresh?namespace=...
GET  /contexts/{name}/snapshots?namespace=...
GET  /contexts/{name}/history?namespace=...&from=...&to=...
```

Pagination is mandatory for history. Do not expose membership blobs in list
responses.

### 5.5 Scheduler

`RefreshScheduler`:

- Uses a monotonic clock for due-time calculation and UTC for persisted times.
- Polls at a configurable interval, default 5 seconds.
- Uses one non-blocking lock per `context_id`; never overlaps refreshes for the
  same context.
- Runs blocking refresh work outside the event loop.
- Applies jitter of at most 10% to avoid synchronized refreshes.
- Records success/failure audit events.
- Leaves the last good snapshot current on failure and records W101 metadata.
- Is disabled explicitly in unit tests unless the test enables a fake clock.

### 5.6 Synchronizer durability

Change `DeepSeeSynchronizer`:

- Initialize committed watermark and seen event IDs from
  `SynchronizerStateRepository`.
- Persist applied event IDs and new watermark in the same transaction as
  snapshot promotion.
- Do not update in-memory state until commit succeeds.
- After restart, duplicate deliveries remain duplicates and the next fetch
  begins after the committed watermark.
- Prune old idempotency rows only when the upstream replay window and ordering
  contract make it safe; the mock keeps all rows.

### 5.7 DeepSee REMOTE provider

Change `DeepSeeRemoteProvider.query()` and mock transport:

- Require `entity_filter` for context-filtered settlement evidence calls.
- Send explicit IDs below 10,000 and portable roaring payloads above it.
- Reject a response containing an entity ID outside the requested filter.
- Follow pagination without widening the filter.
- Record requested cardinality, returned cardinality, and correlation ID in
  trace/audit metadata.
- Never log evidence payloads or membership IDs at info level.

## 6. Work packages and required tests

### WP1 — Canonical identity and snapshot compatibility

**Repository:** `contextql`

Implementation:

1. Add qualified-name helper.
2. Change all catalog and DDL lookups to namespace/name keys.
3. Change all membership references to `context_id`.
4. Add the central compatibility resolver.
5. Implement replacement, alteration, rename, drop, and recreation rules.
6. Update traces and diagnostics to show qualified names.

Required tests:

- Create `ops.risk` and `finance.risk`; both coexist.
- Unqualified `risk` resolves only in the configured default namespace.
- Materialize v1, replace definition, query fails E200 before refresh.
- Refresh after replacement promotes a snapshot with the new definition hash.
- Alter definition has the same invalidation behavior.
- Drop and recreate the same qualified name produces a different context ID.
- Recreated context cannot see old membership.
- Rename retains context ID and current membership.
- Rename collision is rejected.
- Cascade drop tombstones all dependents without deleting immutable history.

Exit criterion: no executor or store lookup uses a context display name as its
membership key.

### WP2 — Persistent server runtime state

**Repositories:** `contextql`, `contextql-server`

Implementation:

1. Add injectable core repository/store contracts.
2. Add migration 003 and SQLite repositories.
3. Reorder server startup and remove duplicate catalog mutation paths.
4. Persist payloads, scores, pointers, history, audit details, and sync state.
5. Add compatible REST facade behavior.

Required restart test:

```text
start app with temporary SQLite and DuckDB files
execute CREATE CONTEXT through /query
refresh and record context ID, definition hash, snapshot version and rows
stop the app completely
create a new app/engine process against the same files
run the same context query
assert identical context ID, snapshot version, membership, scores and trace
```

Additional tests:

- REST-created context appears in `SHOW CONTEXTS` and `/query`.
- DDL-created context appears in REST list/get.
- Replace through either surface creates one catalog version, not two.
- Failed promotion leaves database pointer and payload unchanged.
- Corrupt payload returns E201 and never falls back to a different snapshot.
- Connector watermark and idempotency survive restart.
- Migration from a populated schema-v2 fixture backfills stable context IDs.

Exit criterion: `rg` finds no direct context mutation SQL outside the context
repository and migrations.

### WP3 — REMOTE evidence after narrowing

**Repositories:** `contextql`, `contextql-server`

Implementation:

1. Add `EntityFilter` provider contract.
2. Split query planning from remote materialization.
3. Compose membership before any bounded REMOTE call.
4. Extract equality join key and projected columns.
5. Update DeepSee provider/mock and trace.

Required tests:

- With 20,000 base rows and a 10-member context, provider receives exactly
  those 10 IDs and returns no other evidence.
- Query result equals an equivalent all-local SQL join.
- The provider call happens after snapshot resolution/algebra in trace order.
- Union/intersection/difference pass the composed IDs, not each source bitmap.
- More than 10,000 IDs uses roaring payload and not a Python list.
- Unrecognizable context-filtered remote join fails E301 before provider call.
- Provider lacking entity-filter support fails E302 before provider call.
- Standalone REMOTE query remains compatible.
- Requested column set contains join/projection fields and excludes unused
  evidence columns.

Exit criterion: no context-filtered demonstration call uses `filters={}`,
`columns=[]`, and `limit=None` as an unbounded request.

### WP4 — Per-context storage, native Roaring operations, and atomic promotion

**Repository:** `contextql`; persistent transaction tests also in
`contextql-server`

Implementation:

1. Resolve storage per snapshot.
2. Add explicit-backend errors and server production dependency.
3. Implement native Roaring delta/diff/batched iteration.
4. Stream definition results in bounded batches.
5. Implement in-memory lock/snapshot handles and SQLite atomic promotion.
6. Remove unconditional old-membership expansion.

Required tests:

- `storage='set'` remains set when PyRoaring is installed.
- `storage='roaring'` fails E162 when PyRoaring is unavailable.
- `storage='auto'` resolves according to section 3.4.
- Set and Roaring results are equivalent across randomized deltas.
- Roaring delta test monkeypatches `_to_set()` to raise; delta still succeeds.
- Full refresh with `history=FALSE` never calls `members()` on the old snapshot.
- Batch builder never receives more than 65,536 definition rows at once.
- Member relation is supplied to DuckDB in bounded batches.
- A barrier-controlled concurrent reader sees the complete old snapshot while
  a new snapshot is building, and the complete new snapshot after commit.
- Forced failure at every promotion step leaves the old snapshot queryable.
- Two simultaneous refresh attempts serialize per context.

Exit criterion: the 10M path has no complete-membership conversion to Python
`set`, `list`, Pandas, or a single NumPy array.

### WP5 — Refresh modes, watermark, retention, and scheduler

**Repositories:** `contextql`, `contextql-server`

Implementation:

1. Enforce refresh-mode matrix.
2. Track observed native watermark.
3. Add scheduler.
4. Add anchor-based retention.
5. Persist failure and freshness metadata.

Required tests:

- Native incremental DDL fails E161 with an actionable message.
- Scheduled native refresh executes only when due under a fake clock.
- Failed scheduled refresh retains the prior current snapshot and emits W101.
- Watermark equals the maximum projected source value, not the column name.
- Empty refresh retains preceding watermark.
- `history=FALSE` writes no history.
- Retention preserves one anchor and all later events/snapshots.
- Temporal query before retained history fails E202.
- Scheduler shutdown waits for or cleanly cancels active work without partial
  promotion.

Exit criterion: every accepted refresh option changes runtime behavior or
produces a documented validation error.

### WP6 — Temporal execution

**Repository:** `contextql`; persistence coverage in `contextql-server`

Implementation:

1. Add `AT VERSION` grammar/semantic model.
2. Implement anchor plus event replay.
3. Apply temporal membership before algebra and score calculation.
4. Persist/reload temporal metadata and history.

Use this deterministic test sequence:

```text
t0: add 1 score .2; add 2 score .4
t1: change 1 score .8
t2: remove 2
t3: add 3 score .6
```

Assert:

- `AT t0` -> `{1, 2}`, scores `.2`, `.4`.
- `AT t1` -> `{1, 2}`, scores `.8`, `.4`.
- `AT t2` -> `{1}`.
- `BETWEEN t1 AND t3` -> `{1, 2, 3}`.
- BETWEEN scores are `{1: .8, 2: .4, 3: .6}`.
- `AT VERSION n` returns the exact immutable snapshot.
- Temporal algebra between two contexts matches a reference event replay.
- Restart produces identical temporal results.
- Non-temporal qualifier fails E109.
- Reversed interval fails E203.
- UTC normalization and granularity rounding are deterministic.

Exit criterion: the combined demo queries prior state through ContextQL text,
not direct store access.

### WP7 — Benchmark and CI correctness

**Repositories:** `contextql`, `contextql-server`

Change `benchmarks/post_trade_benchmark.py`:

- SQL-cross-check every individual context.
- SQL-cross-check union, intersection, and difference.
- Cross-check exact ordered top-20 transaction IDs and scores.
- Cross-check native plus connector-composed membership.
- Assert REMOTE request cardinality equals narrowed membership cardinality.
- Record resolved storage kinds and serialized sizes.
- Record peak RSS and maximum refresh/member batch size.
- Record git SHA, row count, seed, Python, DuckDB, PyRoaring, OS, CPU, and
  available memory.
- Exit non-zero on any mismatch.

Change server demonstration tests:

- Keep a 20,000-row fast fixture.
- Assert exact IDs/order/scores against local reference SQL.
- Assert evidence request IDs/cardinality.
- Query prior state through language syntax.

CI:

1. PR CI runs the complete test suite and a 100,000-row correctness benchmark.
2. Add scheduled/manual `benchmark-10m.yml`.
3. The 10M job installs production extras, runs the deterministic benchmark,
   uploads JSON and logs, and fails on any correctness mismatch.
4. Do not make machine-specific latency a pass/fail gate.
5. The committed benchmark report must be regenerated after all work packages
   and include its source git SHA.

Exit criterion: every JSON result object representing membership or rows has a
reference count or exact reference-result assertion.

## 7. Pull-request sequence

Use the same branch name in both repositories:
`post-trade-correctness-hardening`.

Each numbered step must be green before the next begins.

1. **Normative specification and red characterization tests — `contextql`**
   - Update SPEC/DECISIONS/architecture/error registry.
   - Add failing tests for stale replacement, namespaces, temporal execution,
     storage selection, bounded remote calls, and concurrency.
2. **Canonical identity and snapshot compatibility — `contextql`**
   - Complete WP1.
   - Keep in-memory embedded behavior green.
3. **Injectable repositories and persistent runtime — both**
   - Add core protocols/constructor injection.
   - Add server migration/repositories/startup wiring.
   - Complete WP2 restart tests.
4. **REMOTE narrowing contract — both**
   - Land provider-contract changes in `contextql` first.
   - Update server dependency to that commit/version.
   - Complete WP3 and data-minimization tests.
5. **Storage and atomicity — both**
   - Complete WP4.
   - Update server production dependency to include Roaring.
6. **Refresh modes, scheduler, watermark, and retention — both**
   - Complete WP5.
   - Change native post-trade definition from incremental to scheduled.
7. **Temporal execution — both**
   - Complete WP6, including persisted restart tests and language-based demo.
8. **Benchmark, CI, and documentation — both**
   - Complete WP7.
   - Regenerate the 10M report.
9. **Final cross-repository release gate**
   - Test server against the exact intended `contextql` commit/version.
   - Review migrations against fresh and schema-v2 databases.
   - Verify no secrets, evidence rows, or membership ID lists appear in logs.

Do not combine all steps into one unreviewable commit. Protocol changes must
land in `contextql` before server code depends on them.

## 8. Implementation guardrails

- Do not delete old snapshots to solve invalidation.
- Do not key snapshots by qualified name.
- Do not make the server replay DDL by calling `register_context()`.
- Do not silently downgrade explicit Roaring storage.
- Do not claim native incremental refresh without correct removal semantics.
- Do not implement temporal queries by selecting only `computed_at`; use
  event-time history and retained anchors.
- Do not fetch all REMOTE evidence and filter it locally.
- Do not make final `LIMIT` a remote correctness-changing limit.
- Do not hold a SQLite write transaction while evaluating a context SQL query
  or making a provider network call.
- Do not materialize 10M rows or a complete dense bitmap into Pandas.
- Do not weaken the fast embedded/in-memory use case.
- Do not add DeepSee-specific grammar.

## 9. Verification commands

Run from each repository with the supported Python executable.

`contextql`:

```powershell
python -m pip install -e ".[dev,executor,lsp,roaring]"
python -m pytest
python benchmarks/post_trade_benchmark.py --rows 100000 --output benchmark-100k.json
python benchmarks/post_trade_benchmark.py --rows 10000000 --output benchmark-10m.json
```

`contextql-server`:

```powershell
python -m pip install -e ".[dev]"
python -m pytest
```

Cross-repository restart test:

```powershell
python -m pytest tests/test_context_persistence.py tests/test_snapshot_persistence.py tests/test_synchronizer_restart.py
```

Before claiming completion:

```powershell
git status --short
git diff --check
```

Record actual command output, test counts, elapsed time, benchmark report path,
and both git SHAs in the implementation handoff.

## 10. Definition of done

The branch is complete only when all statements below are true:

- [ ] Both repositories are on `post-trade-correctness-hardening`.
- [ ] Every WP1–WP7 required test is present and green.
- [ ] Replacement/alteration cannot use an incompatible old snapshot.
- [ ] Drop/recreate cannot inherit old membership.
- [ ] Rename and namespaces work through DDL, query execution, persistence, and
      REST.
- [ ] Server restart preserves language-defined contexts, snapshots, scores,
      history, current pointers, watermarks, and idempotency.
- [ ] REST and ContextQL DDL share one catalog write path.
- [ ] Context-filtered REMOTE joins fetch evidence only for surviving IDs.
- [ ] Explicit `set`, `roaring`, and `auto` behavior matches section 3.4.
- [ ] Native incremental refresh is rejected until a correct change-feed
      contract exists.
- [ ] Scheduled refresh, observed watermarks, and history retention work.
- [ ] Roaring delta/full refresh paths do not expand complete memberships.
- [ ] Concurrent readers never observe partial snapshot promotion.
- [ ] `AT`, `BETWEEN`, and `AT VERSION` execute through ContextQL language.
- [ ] 100K correctness benchmark runs in PR CI.
- [ ] Scheduled/manual 10M workflow runs and uploads its report.
- [ ] Every benchmark result is SQL- or exact-result-cross-checked.
- [ ] Production server dependencies include Roaring.
- [ ] Fresh and schema-v2 database migrations both pass.
- [ ] Normative documentation matches executable behavior.
- [ ] Both worktrees are clean and the final handoff records exact commits and
      verification evidence.

