"""Provider interfaces for MCP and REMOTE federation.

MCP providers resolve a named context to entity IDs (and optionally scores).
REMOTE providers fetch relational data from an external source.

Example usage::

    import contextql as cql
    from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult

    class FraudModelProvider:
        def resolve(self, entity_type, params, limit=None):
            return MCPResult(
                entity_type=entity_type,
                entity_ids=[1, 5, 10],
                scores=[0.95, 0.80, 0.70],
            )

    class JiraProvider:
        def query(self, resource, filters, columns, limit=None):
            return RemoteResult(rows=[
                {"issue_id": "JIRA-1", "status": "open"},
            ])

    engine = cql.Engine()
    engine.register_mcp_provider("fraud_model", FraudModelProvider())
    engine.register_remote_provider("jira", JiraProvider())
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

#: Refuse to decode serialized membership payloads larger than this
#: (64 MiB) — a Roaring bitmap of tens of millions of IDs is far smaller.
MAX_BITMAP_PAYLOAD_BYTES = 64 * 1024 * 1024


@dataclass
class MCPResult:
    """Result returned by an :class:`MCPProvider`.

    Membership arrives either as an explicit ID list (small results) or as a
    serialized portable bitmap (large results) — exactly one of
    ``entity_ids`` / ``membership_bitmap`` must be provided (plan 8.2).

    Attributes:
        entity_type: The entity type this result applies to (e.g. ``"invoices"``).
            Must match the entity type the executor expects for the query.
        entity_ids: Ordered list of entity IDs that belong to this context.
            ``None`` when membership is supplied as a bitmap.
        scores: Optional per-entity relevance scores. A list parallel to
            *entity_ids*, or a mapping ``{entity_id: score}`` (required form
            for bitmap results). If ``None``, members score 1.0.
        membership_bitmap: Serialized bitmap payload for large results.
        bitmap_encoding: Encoding of *membership_bitmap*; ``"roaring64"``
            (pyroaring ``BitMap64.serialize``) is the supported encoding.
        entity_key_type: Declared entity key type (e.g. ``"INT64"``).
        data_as_of: Source-side freshness timestamp (ISO string).
        source_watermark: Cursor/watermark for incremental synchronization.
        evidence_refs: Optional evidence references keyed by entity ID.
        next_cursor: Pagination cursor; ``None`` when the result is complete.
    """

    entity_type: str
    entity_ids: list[Any] | None = None
    scores: list[float] | dict[int, float] | None = None
    membership_bitmap: bytes | None = None
    bitmap_encoding: str | None = None
    entity_key_type: str | None = None
    data_as_of: str | None = None
    source_watermark: str | None = None
    evidence_refs: dict[int, str] | None = None
    next_cursor: str | None = None
    _native_membership: Any = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        has_ids = self.entity_ids is not None
        has_bitmap = self.membership_bitmap is not None
        if has_ids == has_bitmap:
            raise ValueError(
                "MCPResult requires exactly one of entity_ids or "
                "membership_bitmap."
            )
        if has_bitmap and self.bitmap_encoding is None:
            raise ValueError(
                "MCPResult with membership_bitmap requires bitmap_encoding."
            )

    def membership_object(self):
        """Return a bounded sequence or native Roaring membership."""
        if self._native_membership is not None:
            return self._native_membership
        if self.entity_ids is not None:
            self._native_membership = tuple(
                int(value) for value in self.entity_ids
            )
            return self._native_membership
        payload = self.membership_bitmap or b""
        if len(payload) > MAX_BITMAP_PAYLOAD_BYTES:
            raise ValueError(
                f"Membership bitmap payload of {len(payload)} bytes exceeds "
                f"the {MAX_BITMAP_PAYLOAD_BYTES}-byte limit."
            )
        if self.bitmap_encoding != "roaring64":
            raise ValueError(
                f"Unsupported bitmap encoding {self.bitmap_encoding!r}; "
                "expected 'roaring64'."
            )
        try:
            from pyroaring import BitMap64
        except ImportError as exc:
            raise ImportError(
                "Decoding bitmap MCP results requires the 'pyroaring' "
                "package; install with: pip install 'contextql[roaring]'"
            ) from exc
        try:
            self._native_membership = BitMap64.deserialize(payload)
        except Exception as exc:
            raise ValueError(
                f"Malformed membership bitmap payload: {exc}"
            ) from exc
        return self._native_membership

    @property
    def cardinality(self) -> int:
        """Membership cardinality without dense-array materialization."""
        return len(self.membership_object())

    def membership_array(self):
        """Membership as a dense NumPy array compatibility representation."""
        import numpy as np

        membership = self.membership_object()
        if self.entity_ids is not None:
            return np.asarray(membership, dtype="int64")
        return np.asarray(membership.to_array(), dtype="int64")

    def score_map(self) -> dict[int, float]:
        """Scores as an ``{entity_id: score}`` mapping."""
        if self.scores is None:
            return {}
        if isinstance(self.scores, dict):
            return {int(k): float(v) for k, v in self.scores.items()}
        if self.entity_ids is None:
            raise ValueError(
                "Parallel-list scores require entity_ids; bitmap results "
                "must supply scores as a mapping."
            )
        return {
            int(entity_id): float(score)
            for entity_id, score in zip(self.entity_ids, self.scores)
        }


@dataclass
class RemoteResult:
    """Result returned by a :class:`RemoteProvider`.

    Attributes:
        rows: Fetched rows as a list of dicts (column → value).
            A pandas DataFrame or PyArrow Table may also be supplied.
        schema: Optional column → type mapping describing *rows*.
        data_as_of: Source-side freshness timestamp (ISO string).
        source_watermark: Cursor/watermark for incremental synchronization.
        next_cursor: Pagination cursor; ``None`` when the result is complete.
    """

    rows: list[dict] | Any  # list[dict] canonical; DataFrame/Arrow also accepted
    schema: dict[str, str] | None = None
    data_as_of: str | None = None
    source_watermark: str | None = None
    next_cursor: str | None = None

    def to_dataframe(self) -> "pd.DataFrame":
        """Coerce *rows* to a pandas DataFrame regardless of input type."""
        import pandas as pd

        if isinstance(self.rows, pd.DataFrame):
            return self.rows
        try:
            import pyarrow as pa

            if isinstance(self.rows, pa.Table):
                return self.rows.to_pandas()
        except ImportError:
            pass
        return pd.DataFrame(self.rows)


@dataclass(frozen=True)
class EntityFilter:
    """Bounded entity-key membership pushed into a REMOTE provider."""

    column: str
    entity_ids: tuple[int, ...] | None = None
    membership_bitmap: bytes | None = None
    bitmap_encoding: str | None = None

    def __post_init__(self) -> None:
        has_ids = self.entity_ids is not None
        has_bitmap = self.membership_bitmap is not None
        if has_ids == has_bitmap:
            raise ValueError(
                "EntityFilter requires exactly one of entity_ids or "
                "membership_bitmap."
            )
        if has_bitmap and self.bitmap_encoding != "roaring64":
            raise ValueError(
                "Bitmap entity filters require bitmap_encoding='roaring64'."
            )

    @property
    def cardinality(self) -> int:
        if self.entity_ids is not None:
            return len(self.entity_ids)
        from pyroaring import BitMap64
        return len(BitMap64.deserialize(self.membership_bitmap or b""))

    def ids(self):
        if self.entity_ids is not None:
            return self.entity_ids
        from pyroaring import BitMap64
        return BitMap64.deserialize(self.membership_bitmap or b"")


@runtime_checkable
class MCPProvider(Protocol):
    """Protocol for MCP context providers.

    Implementations resolve a named context to a set of entity IDs, and
    optionally supply per-entity relevance scores.

    The ``entity_type`` argument identifies the entity table/alias the
    context predicate is bound to (e.g. ``"invoices"``).  ``params`` carries
    any named parameters specified in the query as
    ``MCP(name, key => value)``.

    Example implementation::

        class TopInvoicesProvider:
            def resolve(self, entity_type, params, limit=None):
                ids = fetch_top_invoice_ids(params.get("threshold", 0.5))
                return MCPResult(entity_ids=ids, scores=None)

        engine.register_mcp_provider("top_invoices", TopInvoicesProvider())
    """

    def resolve(
        self,
        entity_type: str,
        params: dict,
        limit: int | None = None,
    ) -> MCPResult: ...


@runtime_checkable
class RemoteProvider(Protocol):
    """Protocol for REMOTE data source providers.

    Implementations fetch relational data from an external system and return
    it as a list of row dicts so the executor can materialise it into DuckDB.

    The ``resource`` argument is the qualifier after the provider name in the
    query (e.g. for ``REMOTE(jira.issues)``, ``resource="issues"``).
    ``filters`` and ``columns`` are passed as hints for push-down; they may
    be ignored by simple implementations.

    Example implementation::

        class JiraProvider:
            def query(self, resource, filters, columns, limit=None):
                issues = fetch_jira_issues(project=resource)
                return RemoteResult(rows=[
                    {"issue_id": i.id, "status": i.status}
                    for i in issues
                ])

        engine.register_remote_provider("jira", JiraProvider())
    """

    def query(
        self,
        resource: str,
        filters: dict,
        columns: list[str],
        limit: int | None = None,
        *,
        entity_filter: EntityFilter | None = None,
    ) -> RemoteResult: ...
