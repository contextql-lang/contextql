# Post-Trade Roaring Contexts

**Branch:** `post-trade-roaring-contexts`

**Repositories:** `contextql`, `contextql-server`

**Status:** Approved implementation plan; no runtime implementation yet

**Last reviewed:** 2026-07-23

## 1. Objective

Build an implementation-ready proof that ContextQL can resolve, compose, and
query operational contexts over a deterministic 10-million-transaction
post-trade dataset using Roaring Bitmaps.

The proof must demonstrate a relevant financial-operations question:

> Across 10 million transactions, which trades are most likely to fail
> settlement unless someone intervenes before the applicable market cutoff?

The work also prepares a contract-first DeepSee integration. DeepSee-produced
membership and scores will use ContextQL's MCP provider role; evidence and
operational detail will use its REMOTE provider role. The first implementation
must use a mock connector because no public DeepSee endpoint, authentication,
pagination, or event contract has been established.

## 2. Design decisions

These decisions apply throughout the work:

1. The transaction table stores facts, not context flags.
2. A context is a catalog object, not a dedicated relational table.
3. Current integer membership is stored in a versioned Roaring Bitmap.
4. A bitmap contains entity IDs only. It does not contain per-member
   timestamps or evidence.
5. Snapshot metadata stores `version`, `computed_at`, `data_as_of`,
   `valid_from`, `valid_to`, definition hash, cardinality, and source
   watermark.
6. Membership changes are recorded in a shared append-oriented history store.
7. Scores are stored separately from membership and are joined by entity ID.
8. Refresh creates a new snapshot and atomically promotes it; readers continue
   using the previous snapshot until promotion succeeds.
9. Native SQL contexts and connector-supplied contexts use the same membership
   store and algebra.
10. A new context must not require adding a boolean column to the source
    transaction table.
11. Integer entity keys are the first supported bitmap key type. String, UUID,
    and composite keys require a separately versioned surrogate-key dictionary
    and are outside the first proof.
12. Connector credentials are referenced through deployment configuration or
    a secret manager and are never stored in ContextQL source or DDL.

## 3. Scope

### In scope

- Recover and baseline both repositories.
- Complete executable context DDL and persistent catalog integration.
- Add a context-membership storage abstraction.
- Add an optional Roaring Bitmap implementation.
- Change execution so selective membership is applied before result
  materialization into Pandas.
- Create a deterministic 10-million-transaction post-trade dataset.
- Define and materialize representative settlement and reconciliation
  contexts.
- Implement a mock DeepSee MCP/REMOTE connector and incremental synchronizer.
- Demonstrate native, connector-supplied, and composed contexts.
- Add correctness, performance, failure, and audit tests.
- Document the real DeepSee discovery checklist and integration boundary.

### Not in scope

- A production DeepSee integration before DeepSee supplies an API contract and
  sandbox.
- A web dashboard.
- Production RBAC/RLS or complete multi-tenancy.
- Global entity resolution.
- Rust/PyO3 acceleration.
- Full hot/warm/cold storage promotion.
- Streaming infrastructure such as Kafka or Flink.
- Bitmap storage for non-integer keys.
- Adding `settlement_intervention_required` to a running catalog during the
  planning phase.

## 4. Target architecture

```text
DeepSee or mock DeepSee
  |
  +-- membership + scores ------> MCP provider
  |
  +-- evidence + case details --> REMOTE provider
  |
  +-- changes since cursor -----> incremental synchronizer
                                      |
                                      v
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

The desired query path is:

```text
parse and validate
-> resolve/materialize context snapshots
-> compose bitmaps
-> expose surviving IDs to DuckDB
-> semi-join before row transfer
-> attach scores/evidence
-> order and limit
-> return rows, trace, and snapshot provenance
```

The existing full-result path must not be used by the 10-million-row demo:

```text
DuckDB full result -> Pandas -> Series.isin(set)
```

## 5. Repository and branch preparation

### 5.1 `contextql`

1. Work on `post-trade-roaring-contexts`.
2. Establish a supported Python environment.
3. Install the package with development and executor extras.
4. Run the full test suite without changing behavior.
5. Record baseline test count, runtime, and failures.
6. Add benchmark tooling only after the baseline is captured.

### 5.2 `contextql-server`

1. Work on the matching `post-trade-roaring-contexts` branch.
2. Recover the useful work currently present on
   `origin/context-resolution-layer`.
3. Review its persistent SQLite catalog, provider registry, identity maps,
   audit service, and explain endpoints before merging or porting it.
4. Add `contextql` as an explicit project dependency.
5. Add a shared CI job that installs and tests the server against the intended
   `contextql` version.

### Exit criteria

- Both repositories install together.
- Baseline tests are green or failures are documented.
- The server control-plane work is no longer stranded on an unrelated remote
  branch.
- Both repositories use the same feature branch name.

## 6. Language definition workstream

The grammar already recognizes context DDL, lifecycle statements, MCP/REMOTE
provider registration, and generic `WITH` options. The work is to complete the
semantic representation, specify option behavior, and execute the statements.

### 6.1 Normative language changes

Update:

- `SPEC.md`
- `DECISIONS.md`
- `grammar/contextql.lark` only where the existing grammar cannot express the
  finalized semantics
- `docs/architecture/BITMAP_CONTEXT_STORAGE.md`
- `docs/architecture/DEEPSEE_CONNECTOR.md`

Standardize these context options:

| Option | Values | Meaning |
|---|---|---|
| `materialized` | boolean | Maintain a reusable membership snapshot |
| `storage` | `set`, `roaring`, `auto` | Requested membership representation |
| `refresh_mode` | `manual`, `scheduled`, `incremental` | How new snapshots are produced |
| `refresh_interval` | duration | Scheduled refresh frequency |
| `stale_after` | duration | Maximum acceptable snapshot age |
| `history` | boolean | Record membership changes |
| `history_retention` | duration | Retention policy for membership history |
| `source_watermark` | identifier | Source column/cursor used for incremental refresh |

Specify:

- Whether option names are case-insensitive.
- Duplicate-option behavior.
- Duration syntax.
- Default values.
- Validation errors for incompatible combinations.
- `CREATE OR REPLACE` snapshot behavior.
- `ALTER CONTEXT SET DEFINITION` invalidation behavior.
- `DROP CONTEXT RESTRICT` and `CASCADE` dependency behavior.
- Query behavior for missing, stale, refreshing, and failed snapshots.
- The exact meaning of `TEMPORAL`: event-time semantics and history capability,
  not a timestamp embedded inside a bitmap.

### 6.2 Semantic model changes

Extend `ContextDefinitionModel` to retain:

```text
name
namespace
parameters
entity_key_name
entity_key_type
definition_sql or composition
score_expression
temporal_column
temporal_granularity
description
tags
classification
dependencies
composition_strategy
options
```

Extend `ContextCatalogEntry` with:

```text
context_id
namespace
version
definition_hash
lifecycle_state
materialization settings
current_snapshot_version
last_validated_at
last_refreshed_at
data_as_of
stale_after
```

Update the semantic lowerer so it does not discard the definition body,
classification, parameters, composition, or `WITH` options.

### 6.3 Executable DDL

Add statement handlers for:

- `CREATE CONTEXT`
- `CREATE OR REPLACE CONTEXT`
- `ALTER CONTEXT`
- `DROP CONTEXT`
- `SHOW CONTEXTS`
- `DESCRIBE CONTEXT`
- `VALIDATE CONTEXT`
- `REFRESH CONTEXT`
- `REFRESH ALL CONTEXTS`

Execution sequence for `CREATE CONTEXT`:

1. Parse and lower the complete definition.
2. Validate the entity key and score expression.
3. Validate options.
4. Resolve dependencies.
5. Reject self-reference or dependency cycles.
6. Persist a catalog version.
7. Record an audit event.
8. Enter the defined lifecycle state.
9. Materialize only when policy requests it.
10. Publish the first snapshot atomically after successful evaluation.

### 6.4 Planned context definition

This definition is the target for the demo and tests. Do not register it merely
by adding this plan.

```sql
CREATE CONTEXT settlement_intervention_required
ON transaction_id
SCORE intervention_priority
TEMPORAL (status_recorded_at, MINUTE)
DESCRIPTION 'Transactions requiring action to prevent or resolve settlement failure'
TAGS ('post-trade', 'settlement', 'operations')
CLASSIFICATION internal
WITH (
    materialized = TRUE,
    storage = 'roaring',
    refresh_mode = 'incremental',
    refresh_interval = '1 minute',
    stale_after = '2 minutes',
    history = TRUE,
    history_retention = '90 days',
    source_watermark = status_recorded_at
)
AS
SELECT
    transaction_id,
    status_recorded_at,
    LEAST(
        1.0,
        predicted_fail_probability
        + CASE WHEN ssi_valid = FALSE THEN 0.20 ELSE 0 END
        + CASE WHEN match_status <> 'matched' THEN 0.15 ELSE 0 END
    ) AS intervention_priority
FROM transactions
WHERE settlement_status NOT IN ('settled', 'cancelled')
  AND (
      contractual_settle_date < CURRENT_DATE
      OR market_cutoff_at <= CURRENT_TIMESTAMP + INTERVAL '2 hours'
  )
  AND (
      match_status <> 'matched'
      OR ssi_valid = FALSE
      OR confirmation_received = FALSE
      OR fields_mismatched > 0
      OR predicted_fail_probability >= 0.80
  );
```

Before implementation, parser tests must prove that every expression in this
definition is supported. Adjust the planned expression, not the grammar, when
standard SQL already provides an equivalent supported form.

## 7. Membership storage workstream

### 7.1 Storage abstraction

Add a protocol independent of Roaring:

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

Implement:

- `SetMembershipStore` as a compatibility/reference implementation.
- `RoaringMembershipStore` for non-negative integer IDs.

Add an optional project extra for the selected Roaring library. Parser, LSP,
and grammar-only installations must not require it.

### 7.2 Snapshot metadata

Add a server migration for a shared snapshot table:

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

Add a shared append-oriented history table:

```text
context_membership_history
  id
  context_id
  transaction_id
  change_type
  recorded_at
  effective_at
  context_version
  source
  evidence_ref
  previous_score
  new_score
```

Do not create one relational row per current member. Current membership remains
inside the bitmap.

### 7.3 Refresh behavior

Full refresh:

1. Evaluate the definition in batches.
2. Build a new bitmap and score store.
3. Validate counts and key ranges.
4. Write immutable snapshot data.
5. Write metadata in `building` state.
6. Atomically promote to `current`.
7. Close the previous snapshot's `valid_to`.
8. Derive and append history changes when history is enabled.

Incremental refresh:

1. Read changes after the committed source watermark.
2. Deduplicate by source event/idempotency key.
3. Apply additions, removals, and score changes to a copy.
4. Reject events older than the committed ordering boundary unless the source
   contract explicitly permits correction.
5. Publish a new immutable snapshot.
6. Commit the new watermark only after promotion.

### 7.4 Executor integration

Add a bitmap-aware plan path:

1. Resolve each referenced context to a snapshot.
2. Execute bitmap union/intersection/difference before scanning result rows.
3. Select a pushdown strategy using cardinality and table-size estimates.
4. For selective results, expose member IDs to DuckDB through an Arrow table or
   equivalent relation.
5. Semi-join on the entity key.
6. Fetch only projected/limited rows.
7. Attach scores after membership narrowing.
8. Include snapshot versions and provider calls in execution tracing.

Retain the existing set/Pandas path only as a small-data compatibility path.

## 8. Connector workstream

### 8.1 Connector roles

Implement two provider roles rather than a DeepSee-specific language feature.

#### MCP role

Supplies:

- Entity type and key type.
- Membership as an ID list for small results or a portable bitmap for large
  results.
- Optional scores.
- `data_as_of`.
- Source watermark/cursor.
- Optional evidence references.

#### REMOTE role

Supplies relational evidence:

- Break type.
- Recommended action.
- Agent decision and confidence.
- Explanation.
- Document/evidence reference.
- Case status and owner.
- Last-reviewed timestamp.

This separation keeps lightweight context membership independent of potentially
wide or sensitive evidence rows.

### 8.2 Provider contract changes

Extend `MCPResult` compatibly with:

```text
entity_type
entity_key_type
entity_ids optional
membership_bitmap optional
bitmap_encoding optional
scores optional
data_as_of
source_watermark optional
evidence_refs optional
next_cursor optional
```

Exactly one of `entity_ids` or `membership_bitmap` must be present.

Extend `RemoteResult` with:

```text
schema
data_as_of
source_watermark
next_cursor
```

Do not expand a large bitmap into a Python list inside the connector or
executor.

### 8.3 Mock connector

Add to `contextql-server`:

```text
app/connectors/deepsee/
  __init__.py
  auth.py
  client.py
  models.py
  mcp_provider.py
  remote_provider.py
  synchronizer.py
  mock_service.py
```

The mock contract must cover:

- Full snapshot bootstrap.
- Incremental additions.
- Incremental removals.
- Score changes.
- Cursor pagination.
- Duplicate delivery.
- Out-of-order delivery.
- Retryable and terminal errors.
- Authentication failure.
- Timeout.
- Rate limiting.
- Stale `data_as_of`.
- Invalid key types.
- Malformed bitmap payload.

### 8.4 Provider registration

Use the existing generic provider grammar. Do not add a `DEEPSEE` keyword.

Planned MCP registration:

```sql
REGISTER MCP PROVIDER deepsee.settlement_risk
ENDPOINT 'resolved-by-deployment'
TRANSPORT HTTPS
ENTITY_TYPE transaction
ENTITY_KEY_TYPE INT64
TIMEOUT 5000
ON_FAILURE warn
DESCRIPTION 'Settlement-risk membership provider';
```

Planned REMOTE registration:

```sql
REGISTER REMOTE PROVIDER deepsee
ENDPOINT 'resolved-by-deployment'
TRANSPORT HTTPS
ENTITY_TYPE transaction
ENTITY_KEY_TYPE INT64
RESOURCES (settlement_cases, reconciliation_evidence)
TIMEOUT 10000
ON_FAILURE error
DESCRIPTION 'Operational evidence provider';
```

The final authentication clause depends on the actual DeepSee contract. It
must resolve a credential reference, never contain a secret literal.

### 8.5 DeepSee discovery gate

Confirm before real connector implementation:

- API availability and versioning.
- Sandbox availability.
- Authentication mechanism.
- Trade/entity identifier and key stability.
- Snapshot versus delta endpoints.
- Pull, webhook, stream, or batch delivery.
- Ordering and idempotency guarantees.
- Pagination and maximum response size.
- Rate limits and retry headers.
- Timestamp and timezone semantics.
- Deletion/correction semantics.
- Evidence and explanation schema.
- Audit/correlation identifiers.
- Data residency and retention restrictions.
- Binary Roaring/Arrow support.
- Encryption and customer-managed-key requirements.

The mock remains the executable contract until this gate is satisfied.

## 9. Ten-million-transaction dataset

### 9.1 Primary table

One row represents one canonical operational transaction. Required property
groups:

| Group | Representative columns |
|---|---|
| Identity | `transaction_id`, `trade_id`, `source_system`, `source_record_id` |
| Lifecycle | `trade_timestamp`, `trade_date`, `contractual_settle_date`, `actual_settle_date`, `lifecycle_status`, `status_recorded_at` |
| Economics | `asset_class`, `instrument_id`, `side`, `quantity`, `price`, `gross_amount`, `currency`, `notional_usd` |
| Parties | `counterparty_id`, `legal_entity_id`, `account_id`, `broker_id`, `custodian_id` |
| Reconciliation | `match_status`, `break_type`, `price_difference`, `quantity_difference`, `cash_difference`, `fields_mismatched` |
| Settlement | `settlement_status`, `market`, `market_cutoff_at`, `settlement_method`, `ssi_id`, `ssi_version`, `ssi_valid`, `fail_reason` |
| Documents | `confirmation_received`, `confirmation_timestamp`, `contract_match_confidence`, `document_exception_count` |
| Operations | `exception_opened_at`, `exception_age_minutes`, `owner_team`, `manual_touch_count`, `sla_minutes` |
| Intelligence | `predicted_fail_probability`, `counterparty_risk_score`, `anomaly_score`, `recommended_action` |
| Governance | `data_as_of`, `source_quality_score`, `last_validated_at`, `audit_status` |

Generate data directly in DuckDB. Do not construct ten million Python objects or
a ten-million-row Pandas DataFrame.

### 9.2 Distribution rules

The data must be causally correlated:

- Invalid/stale SSIs increase settlement-failure probability.
- Missing confirmations correlate with economic-term breaks.
- Failure rates cluster by selected counterparties, markets, desks, and source
  systems.
- Cross-border trades have longer settlement chains and more cutoff exposure.
- High-notional trades are uncommon but operationally important.
- Manual touches increase with age and mismatch count.
- Most transactions are clean.
- Exceptions are uncommon enough to make bitmap pushdown selective.

Include both clustered and sparse random contexts so bitmap compression is not
presented only under ideal sequential-ID conditions.

### 9.3 Representative contexts

Target approximate cardinalities, adjusted after deterministic generation:

| Context | Approximate members |
|---|---:|
| `unmatched_trade` | 180,000 |
| `missing_confirmation` | 95,000 |
| `economic_terms_break` | 62,000 |
| `invalid_ssi` | 28,000 |
| `settlement_overdue` | 74,000 |
| `approaching_market_cutoff` | 210,000 |
| `high_notional` | 150,000 |
| `predicted_settlement_fail` | 48,000 |
| `settlement_intervention_required` | 4,000-15,000 |

The exact counts must be deterministic and reported, not forced merely to match
this table.

## 10. Demonstration scenarios

1. Build native SQL context bitmaps from ten million transactions.
2. Show bitmap cardinality and serialized size.
3. Compose union, intersection, and difference.
4. Query the highest-priority settlement interventions.
5. Bootstrap a mock DeepSee settlement-risk bitmap.
6. Apply an incremental DeepSee delta.
7. Compose native and connector-supplied membership.
8. Join REMOTE evidence after bitmap narrowing.
9. Query a prior snapshot by timestamp/version.
10. Show provenance: definitions, snapshot versions, `data_as_of`, provider
    calls, and evidence references.

Representative query:

```sql
SELECT
    t.transaction_id,
    t.counterparty_id,
    t.asset_class,
    t.notional_usd,
    t.contractual_settle_date,
    d.break_type,
    d.recommended_action,
    CONTEXT_SCORE() AS intervention_priority
FROM transactions AS t
LEFT JOIN REMOTE(deepsee.settlement_cases) AS d
  ON t.transaction_id = d.transaction_id
WHERE CONTEXT ON t IN (
    settlement_intervention_required,
    MCP(deepsee.settlement_risk) WEIGHT 1.5
)
ORDER BY CONTEXT DESC
LIMIT 20;
```

## 11. Testing strategy

### Language

- Parser coverage for the complete planned definition.
- Semantic extraction of every field and option.
- Invalid option combinations.
- Type incompatibility.
- Duplicate names and replacement.
- Dependency-cycle detection.
- DDL lifecycle transitions.

### Storage

- Set and Roaring implementations have equivalent behavior.
- Serialization round trips.
- Empty, sparse, dense, and maximum supported IDs.
- Union/intersection/difference correctness.
- Atomic snapshot promotion.
- Reader isolation during refresh.
- History additions, removals, and score changes.
- Full rebuild equals incremental result.

### Connector

- ID-list and bitmap membership responses.
- Cursor pagination.
- Delta idempotency.
- Timeout and retry behavior.
- Rate limiting.
- Authentication failure.
- Stale/out-of-order events.
- Invalid entity types and key types.
- Evidence retrieval after membership narrowing.

### End to end

- Bitmap counts equal equivalent SQL counts.
- Random membership probes equal SQL.
- Query rows equal the reference SQL result.
- Previous versions remain queryable.
- No full ten-million-row Pandas materialization occurs.
- Trace output identifies snapshot versions and providers.

## 12. Performance gates

Record environment information with every benchmark. Do not present a single
machine's numbers as universal guarantees.

Required measurements:

- Dataset generation time and on-disk size.
- Cold context materialization time.
- Incremental refresh time.
- Bitmap cardinality and serialized bytes.
- Union, intersection, and difference latency.
- Warm top-20 query latency.
- Peak resident memory.
- History write volume.
- Connector bootstrap and delta latency.
- Equivalent SQL correctness runtime.

Initial acceptance gates:

1. The demo completes on a documented developer machine.
2. Warm bitmap algebra is measurably faster than rebuilding membership.
3. Selective queries do not transfer ten million rows into Pandas.
4. Peak memory remains bounded and is reported.
5. Every reported result has a SQL correctness check.
6. Incremental refresh produces the same membership as a clean rebuild.

Numeric latency and memory targets should be set only after the first measured
baseline.

## 13. Security and operations

- Store credential references only.
- Redact connector headers and payload secrets from logs.
- Validate bitmap payload size before decoding.
- Validate entity-key bounds and cardinality.
- Apply provider timeouts, rate limits, and circuit breaking.
- Audit provider registration, refresh, promotion, failure, and definition
  changes.
- Include correlation IDs across connector calls and query traces.
- Prevent evidence rows from being fetched before membership narrowing unless
  the query explicitly requires them.
- Document synthetic-data status prominently.

## 14. Pull-request sequence

Each step must keep tests green and land independently where practical.

1. **Baseline and server recovery**
   - Recover reviewed control-plane work.
   - Add dependency linkage and shared CI.
2. **Normative specification**
   - Record design decisions and exact language/storage semantics.
3. **Complete semantic models**
   - Preserve definitions, options, composition, and metadata.
4. **Context DDL execution**
   - Connect language statements to the persistent catalog.
5. **Membership-store abstraction**
   - Add the set reference implementation.
6. **Roaring membership store**
   - Add optional dependency, serialization, and snapshot metadata.
7. **Snapshot lifecycle and history**
   - Full refresh, atomic promotion, deltas, and audit.
8. **Bitmap-aware DuckDB execution**
   - Algebra and semi-join pushdown before Pandas.
9. **Deterministic 10-million-row generator**
   - Dataset, reference SQL, and generation tests.
10. **Native context benchmark**
    - Populate and benchmark representative contexts.
11. **Provider contract extension**
    - Bitmap MCP results, metadata, cursors, and compatibility.
12. **Mock DeepSee connector**
    - MCP membership, REMOTE evidence, and delta synchronizer.
13. **Combined demonstration**
    - Native plus mock DeepSee contexts and evidence.
14. **DeepSee discovery and mapping**
    - Replace mock transport only after the discovery gate.
15. **Hardening and documentation**
    - Failure tests, security review, benchmark report, and walkthrough.

## 15. Definition of done

This branch is complete when:

- Both repositories pass their documented test suites.
- Context DDL is executable and persisted.
- `settlement_intervention_required` can be created from language text rather
  than Python registration.
- Its current membership is stored as a versioned Roaring Bitmap.
- Membership history records entry and exit timestamps separately.
- Ten million deterministic transactions are generated without Pandas
  materialization.
- Context algebra is executed before row retrieval.
- Bitmap results match reference SQL.
- Mock DeepSee membership, scores, deltas, and evidence work end to end.
- The real connector boundary and discovery requirements are documented.
- Benchmark output reports correctness, latency, size, and peak memory.
- No undocumented claim is made about DeepSee's actual API.

## 16. References

- DeepSee solutions: <https://deepsee.ai/solutions/>
- DeepSee platform: <https://deepsee.ai/platform/>
- ContextQL language specification: `SPEC.md`
- ContextQL architecture decisions: `DECISIONS.md`
- ContextQL design whitepaper: `WHITEPAPER.md`
