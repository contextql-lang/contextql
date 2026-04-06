"""Federation integration tests: MCP membership + REMOTE data separation.

These tests prove that MCP providers supply only entity membership (bitmask +
scores) while REMOTE providers supply relational data — with minimal data
movement. This validates the core federation model split described in
docs/architecture/CONTEXT_RESOLUTION_PLANE.md.
"""
from __future__ import annotations

import pandas as pd
import pytest

import contextql as cql
from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult


# ── Tracking providers ────────────────────────────────────────────────


class TrackingMCP:
    """MCP provider that tracks calls and returns scored membership."""

    def __init__(self, entity_ids, scores=None):
        self._entity_ids = entity_ids
        self._scores = scores
        self.call_count = 0
        self.last_params = None

    def resolve(self, entity_type, params, limit=None):
        self.call_count += 1
        self.last_params = params
        return MCPResult(
            entity_type=entity_type,
            entity_ids=self._entity_ids,
            scores=self._scores,
        )


class TrackingREMOTE:
    """REMOTE provider that tracks calls and returns a fixed table."""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.call_count = 0
        self.last_resource = None
        self.last_filters = None

    def query(self, resource, filters, columns, limit=None):
        self.call_count += 1
        self.last_resource = resource
        self.last_filters = filters
        return RemoteResult(rows=self._rows)


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def engine():
    e = cql.Engine()
    e.register_table(
        "invoices",
        pd.DataFrame({
            "invoice_id": [1, 2, 3, 4, 5],
            "amount": [100, 500, 200, 800, 50],
            "vendor_id": [10, 20, 30, 40, 50],
            "status": ["open", "open", "paid", "open", "open"],
        }),
        primary_key="invoice_id",
    )
    e.register_context(
        "open_invoice",
        "SELECT invoice_id FROM invoices WHERE status = 'open'",
        entity_key="invoice_id",
    )
    return e


# ── Tests ─────────────────────────────────────────────────────────────


class TestMCPRemoteSeparation:
    """Prove MCP provides membership while REMOTE provides data."""

    def test_mcp_filters_remote_data(self, engine):
        """MCP narrows the result set; REMOTE enriches with columns."""
        mcp = TrackingMCP(entity_ids=[1, 4], scores=[0.9, 0.7])
        remote = TrackingREMOTE(rows=[
            {"issue_id": 1, "jira_status": "OPEN", "priority": "HIGH"},
            {"issue_id": 2, "jira_status": "CLOSED", "priority": "LOW"},
            {"issue_id": 3, "jira_status": "OPEN", "priority": "MEDIUM"},
            {"issue_id": 4, "jira_status": "IN_PROGRESS", "priority": "CRITICAL"},
            {"issue_id": 5, "jira_status": "CLOSED", "priority": "LOW"},
        ])

        engine.register_mcp_provider("risk", mcp)
        engine.register_remote_provider("tracker", remote)

        result = engine.execute("""
            SELECT i.invoice_id, i.amount,
                   t.jira_status, t.priority,
                   CONTEXT_SCORE() AS risk
            FROM invoices AS i
            JOIN REMOTE(tracker.issues) AS t ON i.invoice_id = t.issue_id
            WHERE CONTEXT IN (MCP(risk))
            ORDER BY CONTEXT DESC
            LIMIT 10;
        """)

        df = result.to_pandas()
        # Only invoices 1 and 4 should survive (MCP membership)
        assert set(df["invoice_id"].tolist()) == {1, 4}
        # But the REMOTE data is present (enrichment)
        assert "jira_status" in df.columns
        assert "priority" in df.columns

        # REMOTE provider received no filtering (fetched all 5 rows)
        assert remote.call_count == 1
        assert remote.last_filters == {}

        # MCP provider was called once
        assert mcp.call_count == 1

    def test_remote_receives_no_membership_filter(self, engine):
        """REMOTE provider gets the full table — filtering happens in engine."""
        mcp = TrackingMCP(entity_ids=[2], scores=[0.5])
        remote = TrackingREMOTE(rows=[
            {"issue_id": i, "status": "OPEN"} for i in range(1, 6)
        ])

        engine.register_mcp_provider("narrow", mcp)
        engine.register_remote_provider("wide", remote)

        result = engine.execute("""
            SELECT i.invoice_id, w.status AS remote_status,
                   CONTEXT_SCORE() AS score
            FROM invoices AS i
            JOIN REMOTE(wide.all) AS w ON i.invoice_id = w.issue_id
            WHERE CONTEXT IN (MCP(narrow))
            ORDER BY CONTEXT DESC;
        """)

        df = result.to_pandas()
        # Only invoice 2 survives MCP filter
        assert df["invoice_id"].tolist() == [2]
        # But REMOTE returned all 5 rows (no pushdown)
        assert remote.call_count == 1

    def test_combined_mcp_and_native_context(self, engine):
        """MCP and native SQL contexts compose via union."""
        mcp = TrackingMCP(entity_ids=[3, 5], scores=[0.6, 0.4])
        engine.register_mcp_provider("external", mcp)

        result = engine.execute("""
            SELECT invoice_id, amount, CONTEXT_SCORE() AS score
            FROM invoices
            WHERE CONTEXT IN (open_invoice, MCP(external))
            ORDER BY CONTEXT DESC
            LIMIT 10;
        """)

        df = result.to_pandas()
        # Union: open_invoice gives {1,2,4,5}, MCP gives {3,5}
        # Combined: {1,2,3,4,5} — all invoices
        assert set(df["invoice_id"].tolist()) == {1, 2, 3, 4, 5}


class TestExecutionTrace:
    """Verify trace captures provenance correctly."""

    def test_trace_captures_mcp_calls(self, engine):
        mcp = TrackingMCP(entity_ids=[1, 4], scores=[0.9, 0.7])
        engine.register_mcp_provider("fraud", mcp)

        result = engine.execute("""
            SELECT invoice_id, CONTEXT_SCORE() AS risk
            FROM invoices
            WHERE CONTEXT IN (MCP(fraud))
            ORDER BY CONTEXT DESC;
        """)

        trace = result.trace
        assert trace is not None
        assert "MCP(fraud)" in trace.contexts_resolved

        mcp_calls = [c for c in trace.provider_calls if c.provider_type == "MCP"]
        assert len(mcp_calls) == 1
        assert mcp_calls[0].provider_name == "fraud"
        assert mcp_calls[0].entity_count == 2
        assert mcp_calls[0].elapsed_ms >= 0

    def test_trace_captures_remote_calls(self, engine):
        remote = TrackingREMOTE(rows=[
            {"issue_id": i, "status": "OPEN"} for i in range(1, 6)
        ])
        engine.register_remote_provider("tracker", remote)

        result = engine.execute("""
            SELECT i.invoice_id, t.status AS tracker_status
            FROM invoices AS i
            JOIN REMOTE(tracker.issues) AS t ON i.invoice_id = t.issue_id
            WHERE CONTEXT IN (open_invoice);
        """)

        trace = result.trace
        remote_calls = [c for c in trace.provider_calls if c.provider_type == "REMOTE"]
        assert len(remote_calls) == 1
        assert remote_calls[0].provider_name == "tracker"
        assert remote_calls[0].entity_count == 5

    def test_trace_captures_native_contexts(self, engine):
        result = engine.execute("""
            SELECT invoice_id, CONTEXT_SCORE() AS score
            FROM invoices
            WHERE CONTEXT IN (open_invoice)
            ORDER BY CONTEXT DESC;
        """)

        trace = result.trace
        assert "open_invoice" in trace.contexts_resolved
        # No provider calls for native contexts
        assert len(trace.provider_calls) == 0

    def test_trace_combined_query(self, engine):
        mcp = TrackingMCP(entity_ids=[1, 4], scores=[0.9, 0.7])
        remote = TrackingREMOTE(rows=[
            {"issue_id": i, "priority": "HIGH"} for i in range(1, 6)
        ])
        engine.register_mcp_provider("model", mcp)
        engine.register_remote_provider("ext", remote)

        result = engine.execute("""
            SELECT i.invoice_id, e.priority,
                   CONTEXT_SCORE() AS score
            FROM invoices AS i
            JOIN REMOTE(ext.data) AS e ON i.invoice_id = e.issue_id
            WHERE CONTEXT IN (open_invoice, MCP(model))
            ORDER BY CONTEXT DESC
            LIMIT 5;
        """)

        trace = result.trace
        # Both native and MCP contexts resolved
        assert "open_invoice" in trace.contexts_resolved
        assert "MCP(model)" in trace.contexts_resolved
        # Both provider types called
        types = {c.provider_type for c in trace.provider_calls}
        assert types == {"MCP", "REMOTE"}
