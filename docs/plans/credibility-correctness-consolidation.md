# ContextQL Credibility and Correctness Consolidation Plan

## Objective

Correct the six credibility findings identified after the post-trade hardening
review, prove each correction with regression tests, regenerate benchmark
evidence from an identified clean commit, and consolidate both repositories
onto their `main` branches.

Repositories:

- `contextql`: language, executor, membership storage, history, benchmark.
- `contextql-server`: HTTP boundary, durable connector state, DeepSee mock
  transport.

## Invariants

1. A large Roaring membership is not expanded into a Python `set`, list, or
   dense NumPy array on query-planning, REMOTE narrowing, temporal replay, or
   connector transport paths.
2. Query shapes that cannot be executed with bounded membership pushdown fail
   before the base relation is materialized.
3. Synchronizer state never records a watermark or event as committed before
   the corresponding snapshot has been published.
4. A server request cannot produce an unbounded in-memory result or response.
5. Every published benchmark number is generated from one checked-in JSON
   artifact carrying the measured environment, exact source commit, and a
   clean-worktree flag.
6. After delivery, local and remote branch topology contains only `main` in
   both repositories.

## Workstream 1 — Native and Federated Membership Execution

### Engine contract

- Add a native membership accessor to `MCPResult`.
  - Explicit ID results remain bounded sequences.
  - `roaring64` results deserialize once to a native `BitMap64`.
  - Cardinality inspection must not construct a dense array.
- Use the native accessor during MCP/REMOTE narrowing and trace collection.
- Reuse an existing native Roaring membership when constructing a large
  `EntityFilter`; do not rebuild it through a Python collection.

### Pushdown safety

- Keep the existing bitmap-native DuckDB semi-join path for materialized
  contexts with a matching registered key.
- Before base SQL execution, detect materialized or large federated
  memberships that would fall back to DataFrame filtering.
- Fail these unsupported shapes with a stable diagnostic and remediation
  guidance instead of invoking `members()` and `Series.isin()` over an
  unbounded base result.
- Retain the legacy path only for explicitly bounded compatibility use.

### Acceptance tests

- Monkeypatch `_to_set()` to fail and execute native Roaring pushdown.
- Resolve a serialized MCP bitmap through a REMOTE join and assert no dense
  array or Python set conversion.
- Exercise a materialized context without a pushdown-compatible table key and
  assert failure occurs before `execute_df()`.

## Workstream 2 — Temporal Membership Replay

- Preserve the native membership type when copying an anchor:
  - mutable `BitMap64` for Roaring input;
  - `set` for the compatibility store.
- Preserve the native type for `BETWEEN` accumulated membership.
- Maintain history events in deterministic replay order when appending.
- Add a lazy `iter_events_between()` path and make temporal replay consume it;
  retain `events_between()` only as a compatibility list API.

### Acceptance tests

- Reconstruct `AT` and `BETWEEN` states from a Roaring anchor while `_to_set`
  and dense-array conversion are forbidden.
- Verify deterministic ordering for out-of-order appended events.
- Verify set-backed behavior remains unchanged.

## Workstream 3 — DeepSee Entity-Filter Transport

- Extend the client/mock transport boundary to accept `EntityFilter` directly.
- Preserve serialized `roaring64` above the threshold.
- Apply the entity filter before constructing evidence rows.
- Validate returned rows against the native requested membership without
  expanding the request.
- Record filter encoding and cardinality in mock request introspection.

### Acceptance tests

- Send more than 10,000 requested IDs and assert the mock receives
  `roaring64`, not an explicit tuple.
- Assert evidence construction is restricted to requested members.
- Assert an out-of-filter response still fails closed.

## Workstream 4 — Synchronizer Publication Safety

- Publish the staged snapshot before committing the durable watermark and
  idempotency records.
- Treat replay after a state-commit failure as an at-least-once operation:
  additions, removals, and score updates remain idempotent.
- Do not advance in-memory state until snapshot publication and durable state
  commit both succeed.
- Document that a shared durable snapshot/state repository is required before
  claiming single-transaction atomicity; the current split store uses safe
  snapshot-first ordering.

### Acceptance tests

- Inject failure during snapshot publication and assert durable watermark and
  event IDs remain unchanged after restart.
- Inject failure during state commit after publication and assert replay
  converges to the correct membership without skipping the event.
- Cover bootstrap and incremental paths.

## Workstream 5 — Server Result and Response Bounds

- Add configurable limits:
  - maximum requested result rows;
  - maximum intermediate engine rows;
  - maximum encoded response bytes.
- Require an explicit `LIMIT` on server-executed `SELECT` statements and reject
  limits above the configured maximum.
- Leave DDL and metadata statements unaffected.
- Add an engine-side pre-materialization cardinality guard so hybrid execution
  cannot allocate a DataFrame beyond the configured intermediate bound.
- Reject oversized encoded responses before FastAPI/Pydantic serialization.

### Acceptance tests

- Reject missing and excessive `LIMIT` values.
- Reject an intermediate relation above the engine cap before `execute_df()`.
- Reject an oversized response body.
- Verify bounded queries, DDL, and the existing demo continue to work.

## Workstream 6 — Benchmark Provenance

- Run the 10-million-row benchmark after all code/report artifacts are
  committed and the working tree is clean.
- Store the exact environment, source commit, clean-worktree state, timings,
  correctness checks, peak RSS, and maximum batch size in JSON.
- Generate or update the benchmark README and walkthrough from that artifact.
- Remove every stale number that cannot be traced to the checked-in JSON.
- Describe machine-specific measurements as observations, never guarantees.

## Verification Sequence

1. Focused engine membership, pushdown, temporal, and benchmark-contract tests.
2. Full `contextql` test suite.
3. Focused server query, connector, synchronizer, and persistence tests.
4. Full `contextql-server` test suite.
5. Failure-injection reproduction for synchronizer publication ordering.
6. Clean-commit 10-million-row benchmark and artifact consistency check.
7. Diff review and secret/large-file scan.

## Consolidation Sequence

1. Commit tested engine changes on the current hardening branch.
2. Commit tested server changes on the current hardening branch.
3. Fast-forward each local `main` to the tested hardening commit.
4. Push both `main` branches and verify remote parity.
5. Delete every non-`main` local branch.
6. Delete any non-`main` remote branch if one appears after the final fetch.
7. Confirm both working trees are clean and both local/remote branch listings
   contain only `main` (plus the symbolic `origin/HEAD -> origin/main`).

