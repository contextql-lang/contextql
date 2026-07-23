# Bitmap Context Storage

**Membership storage architecture for materialized contexts (v0.3)**

Copyright (c) 2026 Anton du Plessis

Normative decisions: DECISIONS.md CS-1 through CS-16. Option and
snapshot-state semantics: SPEC.md section 6.

---

## 1. Storage Model

A materialized context's current membership is a **versioned, immutable
snapshot**. For non-negative integer entity keys the snapshot representation
is a Roaring Bitmap; a plain set representation exists as the
compatibility/reference implementation and for non-integer keys.

Separation of concerns:

| Concern | Where it lives |
|---|---|
| Facts | Source tables (never context flags — CS-1) |
| Current membership (entity IDs only) | Versioned bitmap snapshot (CS-3) |
| Scores | Separate score store, joined by entity ID (CS-6) |
| Membership changes over time | Shared append-oriented history store (CS-5) |
| Provenance and staleness | Snapshot metadata (CS-4) |
| Evidence / operational detail | REMOTE providers, fetched after narrowing |

```text
Context catalog ---> snapshot builder ---> versioned membership store
                                              |
                                              +-- Roaring Bitmap
                                              +-- score storage
                                              +-- membership history
                                              +-- snapshot metadata
                                                        |
                                                        v
Context algebra ---> DuckDB membership semi-join ---> limited result rows
```

## 2. Membership Store Abstraction

The store protocol is independent of Roaring:

```python
class ContextMembershipStore(Protocol):
    def put_snapshot(...): ...
    def get_snapshot(...): ...
    def apply_delta(...): ...
    def contains(...): ...
    def union(...): ...
    def intersect(...): ...
    def difference(...): ...
    def serialize(...): ...
```

Implementations:

- `SetMembershipStore` — reference implementation; behavioral baseline for
  equivalence tests.
- `RoaringMembershipStore` — non-negative integer IDs; optional project extra
  so parser/LSP/grammar-only installations carry no bitmap dependency.

`storage = 'auto'` selects Roaring for non-negative integer keys, set storage
otherwise (CS-14). String/UUID/composite keys are deferred: they require a
separately versioned surrogate-key dictionary (CS-9).

## 3. Snapshot Metadata

Stored per snapshot (server migration; shared table):

```text
context_snapshots
  id
  context_id
  version
  storage_kind
  bitmap_location
  score_location
  member_count
  serialized_bytes
  computed_at
  data_as_of
  valid_from
  valid_to
  definition_hash
  source_watermark
  state
  error_detail
```

## 4. Membership History

Shared append-oriented table. One row per membership change — never one row
per current member (current membership lives in the bitmap):

```text
context_membership_history
  id
  context_id
  transaction_id
  change_type          -- added | removed | score_changed
  recorded_at
  effective_at
  context_version
  source               -- native | provider identifier
  evidence_ref
  previous_score
  new_score
```

Entry and exit timestamps are recorded as separate change events. Temporal
qualifiers (`AT`, `BETWEEN`) resolve against this history, at the granularity
declared by `TEMPORAL` (CS-16).

## 5. Refresh Behavior

### Full refresh

1. Evaluate the definition in batches.
2. Build a new bitmap and score store.
3. Validate counts and key ranges.
4. Write immutable snapshot data.
5. Write metadata in `building` state.
6. Atomically promote to `current`.
7. Close the previous snapshot's `valid_to`.
8. Derive and append history changes when history is enabled.

### Incremental refresh

1. Read changes after the committed source watermark.
2. Deduplicate by source event/idempotency key.
3. Apply additions, removals, and score changes to a copy.
4. Reject events older than the committed ordering boundary unless the source
   contract explicitly permits correction.
5. Publish a new immutable snapshot.
6. Commit the new watermark only after promotion.

Invariant: an incremental refresh must produce the same membership as a clean
rebuild against the same source state.

### Reader behavior by state

Per SPEC.md section 6: missing/invalidated snapshot → E200; stale → W100;
refreshing → readers keep the current snapshot; failed refresh → last good
snapshot stays current with W101 (CS-15, CS-7).

## 6. Executor Integration

Bitmap-aware plan path:

1. Resolve each referenced context to a snapshot.
2. Execute bitmap union/intersection/difference before scanning result rows.
3. Select a pushdown strategy from cardinality and table-size estimates.
4. For selective results, expose member IDs to DuckDB through an Arrow table
   or equivalent relation.
5. Semi-join on the entity key.
6. Fetch only projected/limited rows.
7. Attach scores after membership narrowing.
8. Include snapshot versions and provider calls in execution tracing.

The legacy `DuckDB full result -> Pandas -> Series.isin(set)` path remains
only as a small-data compatibility path (CS-11) and must not be used by the
ten-million-row demonstration.

## 7. Security and Operational Constraints

- Validate serialized bitmap payload size before decoding.
- Validate entity-key bounds and cardinality on ingest.
- Audit refresh, promotion, failure, and definition changes.
- Correlation IDs flow across connector calls and query traces.
- Evidence rows are not fetched before membership narrowing unless the query
  explicitly requires them.

## 8. Hardening invariants

- Catalog names resolve as `(namespace, name)` while snapshot payloads,
  history, and synchronizer state use immutable `context_id`.
- A current snapshot is eligible only when its version and definition hash
  match the catalog pointer.
- Explicit `storage = 'set'` and `storage = 'roaring'` are honored per
  snapshot; explicit Roaring never silently falls back.
- Definition results and DuckDB membership relations are consumed in bounded
  batches.
- Roaring delta and algebra paths remain bitmap-native.
- Persistent promotion writes payload, metadata, pointer, history, and
  committed watermark in one transaction.
- Retention preserves one anchor snapshot plus all later events.
