"""Provider interfaces for MCP and REMOTE federation.

MCP providers resolve a named context to entity IDs (and optionally scores).
REMOTE providers fetch relational data from an external source.

Example usage::

    import contextql as cql
    from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult

    class FraudModelProvider:
        def resolve(self, entity_type, params, limit=None):
            return MCPResult(
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


@dataclass
class MCPResult:
    """Result returned by an :class:`MCPProvider`.

    Attributes:
        entity_ids: Ordered list of entity IDs that belong to this context.
        scores: Optional per-entity relevance scores, parallel to *entity_ids*.
            If ``None``, all members receive a score of 1.0 in scoring calculations.
    """

    entity_ids: list[Any]
    scores: list[float] | None = None


@dataclass
class RemoteResult:
    """Result returned by a :class:`RemoteProvider`.

    Attributes:
        rows: Fetched rows as a list of dicts (column → value).
    """

    rows: list[dict]


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
    ) -> RemoteResult: ...
