# DeepSee Connector Boundary

**Contract-first connector architecture for external settlement intelligence
(v0.3)**

Copyright (c) 2026 Anton du Plessis

Normative decisions: DECISIONS.md CS-8, CS-10, CS-12. Storage integration:
`BITMAP_CONTEXT_STORAGE.md`.

> **Status:** No public DeepSee endpoint, authentication, pagination, or
> event contract has been established. Everything in this document describes
> the ContextQL side of the boundary and a mock connector that serves as the
> executable contract until the discovery gate (section 6) is satisfied. No
> claim in this document describes DeepSee's actual API.

---

## 1. Integration Model

DeepSee-produced intelligence integrates through ContextQL's two existing
provider roles — not through a vendor keyword (CS-12):

```text
DeepSee or mock DeepSee
  |
  +-- membership + scores ------> MCP provider
  |
  +-- evidence + case details --> REMOTE provider
  |
  +-- changes since cursor -----> incremental synchronizer
```

The separation keeps lightweight context membership independent of
potentially wide or sensitive evidence rows. Evidence is fetched only after
membership narrowing.

### MCP role (membership + scores)

Supplies:

- Entity type and key type.
- Membership as an ID list for small results or a portable bitmap for large
  results.
- Optional scores.
- `data_as_of`.
- Source watermark/cursor.
- Optional evidence references.

### REMOTE role (relational evidence)

Supplies:

- Break type.
- Recommended action.
- Agent decision and confidence.
- Explanation.
- Document/evidence reference.
- Case status and owner.
- Last-reviewed timestamp.

## 2. Provider Contract Extensions

`MCPResult` is extended compatibly with:

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

`RemoteResult` is extended with:

```text
schema
data_as_of
source_watermark
next_cursor
```

`RemoteProvider.query` also accepts an optional `EntityFilter` containing the
remote join column and either a bounded ID tuple or a portable `roaring64`
payload. Context-filtered REMOTE joins require this capability. The DeepSee
mock rejects evidence outside the requested membership and records requested
and returned cardinality for trace/audit verification.

A large bitmap is never expanded into a Python list inside the connector or
executor — it is handed to the membership store in its serialized form and
validated (size, key bounds, cardinality) before decoding.

## 3. Provider Registration

Registration uses the existing generic provider grammar:

```sql
REGISTER MCP PROVIDER deepsee.settlement_risk
ENDPOINT 'resolved-by-deployment'
TRANSPORT HTTPS
ENTITY_TYPE transaction
ENTITY_KEY_TYPE INT64
TIMEOUT 5000
ON_FAILURE warn
DESCRIPTION 'Settlement-risk membership provider';

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
must resolve a credential reference — never a secret literal (CS-10).

## 4. Incremental Synchronizer

The synchronizer pulls changes after the committed source watermark and
applies them through the membership store's delta path
(`BITMAP_CONTEXT_STORAGE.md` section 5):

- Deduplicate by source event/idempotency key.
- Reject events older than the committed ordering boundary unless the source
  contract explicitly permits correction.
- Commit the new watermark only after snapshot promotion.

## 5. Mock Connector

Location (contextql-server):

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

## 6. Discovery Gate

The real connector transport replaces the mock only after every item below is
confirmed with DeepSee:

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

## 7. Security Requirements

- Credential references only; no secrets in source, DDL, or logs.
- Redact connector headers and payload secrets from logs.
- Apply provider timeouts, rate limits, and circuit breaking.
- Audit provider registration, refresh, promotion, and failure.
- Correlation IDs across connector calls and query traces.

## 8. References

- DeepSee solutions: <https://deepsee.ai/solutions/>
- DeepSee platform: <https://deepsee.ai/platform/>
