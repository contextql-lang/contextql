"""Tests for MCP and REMOTE provider integration."""

from __future__ import annotations

import time
import warnings

import pandas as pd
import pytest

import contextql as cql
from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def engine():
    """Minimal engine with an invoices table and one SQL context."""
    e = cql.Engine()
    df = pd.DataFrame({
        "invoice_id": [1, 2, 3, 4, 5],
        "amount": [100, 500, 200, 800, 50],
        "status": ["open", "open", "paid", "open", "open"],
    })
    e.register_table("invoices", df, primary_key="invoice_id")
    e.register_context(
        "open_invoice",
        "SELECT invoice_id FROM invoices WHERE status = 'open'",
        entity_key="invoice_id",
    )
    return e


# ── MCPProvider protocol ───────────────────────────────────────────────────────


class _TopInvoices:
    """Returns ids [1, 4] with scores [0.9, 0.7]."""
    def resolve(self, entity_type, params, limit=None):
        return MCPResult(entity_type=entity_type, entity_ids=[1, 4], scores=[0.9, 0.7])


class _AllInvoicesNoScores:
    """Returns ids [1, 2, 3] without scores."""
    def resolve(self, entity_type, params, limit=None):
        return MCPResult(entity_type=entity_type, entity_ids=[1, 2, 3])


class _ParamCapture:
    """Captures the params argument for later inspection."""
    def __init__(self):
        self.captured_params = {}

    def resolve(self, entity_type, params, limit=None):
        self.captured_params = dict(params)
        return MCPResult(entity_type=entity_type, entity_ids=[1])


class _SleepyProvider:
    """Simulates a slow provider."""
    def resolve(self, entity_type, params, limit=None):
        time.sleep(10)
        return MCPResult(entity_type=entity_type, entity_ids=[1])


# ── REMOTE provider helpers ───────────────────────────────────────────────────


class _StaticRemoteProvider:
    """Returns a fixed list of rows."""
    def __init__(self, rows):
        self._rows = rows

    def query(self, resource, filters, columns, limit=None):
        return RemoteResult(rows=self._rows)


class _SleepyRemoteProvider:
    def query(self, resource, filters, columns, limit=None):
        time.sleep(10)
        return RemoteResult(rows=[])


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: MCP registration and basic resolution
# ─────────────────────────────────────────────────────────────────────────────


class TestMCPRegistration:
    def test_register_stores_provider(self, engine):
        provider = _TopInvoices()
        engine.register_mcp_provider("top_inv", provider)
        assert engine._mcp_providers["top_inv"] is provider

    def test_mcp_query_returns_matching_rows(self, engine):
        engine.register_mcp_provider("top_inv", _TopInvoices())
        result = engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(top_inv));"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        assert ids == [1, 4]

    def test_mcp_query_row_count(self, engine):
        engine.register_mcp_provider("top_inv", _TopInvoices())
        result = engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(top_inv));"
        )
        assert result.row_count == 2

    def test_unregistered_mcp_raises(self, engine):
        with pytest.raises(ValueError, match="MCP provider 'missing'"):
            engine.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(missing));"
            )

    def test_mcp_error_message_mentions_register_call(self, engine):
        with pytest.raises(ValueError, match="register_mcp_provider"):
            engine.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(missing));"
            )

    def test_mcp_provider_satisfies_protocol(self):
        assert isinstance(_TopInvoices(), MCPProvider)

    def test_mcp_result_is_dataclass(self):
        r = MCPResult(entity_type="invoices", entity_ids=[1, 2], scores=[0.5, 0.3])
        assert r.entity_type == "invoices"
        assert r.entity_ids == [1, 2]
        assert r.scores == [0.5, 0.3]

    def test_mcp_result_scores_optional(self):
        r = MCPResult(entity_type="invoices", entity_ids=[1, 2])
        assert r.scores is None

    def test_mcp_entity_type_mismatch_raises(self, engine):
        class WrongTypeProvider:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type="widgets", entity_ids=[1])

        engine.register_mcp_provider("wrong", WrongTypeProvider())
        with pytest.raises(ValueError, match="entity_type"):
            engine.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(wrong));"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: MCP scoring
# ─────────────────────────────────────────────────────────────────────────────


class TestMCPScoring:
    def test_context_score_reflects_mcp_scores(self, engine):
        engine.register_mcp_provider("top_inv", _TopInvoices())
        result = engine.execute(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (MCP(top_inv)) ORDER BY CONTEXT DESC;"
        )
        df = result.to_pandas()
        # invoice 1 has score 0.9, invoice 4 has score 0.7 — highest first
        assert df.iloc[0]["invoice_id"] == 1
        assert abs(df.iloc[0]["s"] - 0.9) < 1e-6
        assert df.iloc[1]["invoice_id"] == 4
        assert abs(df.iloc[1]["s"] - 0.7) < 1e-6

    def test_context_score_without_provider_scores_is_one(self, engine):
        engine.register_mcp_provider("all_inv", _AllInvoicesNoScores())
        result = engine.execute(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (MCP(all_inv)) ORDER BY invoice_id;"
        )
        df = result.to_pandas()
        assert all(df["s"] == 1.0)

    def test_order_by_context_desc_with_scores(self, engine):
        engine.register_mcp_provider("top_inv", _TopInvoices())
        result = engine.execute(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (MCP(top_inv)) ORDER BY CONTEXT DESC;"
        )
        scores = result.to_pandas()["s"].tolist()
        assert scores == sorted(scores, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: MCP parameters
# ─────────────────────────────────────────────────────────────────────────────


class TestMCPParameters:
    @pytest.mark.skip(
        reason=(
            "Grammar rule 'MCP LPAR qualified_name RPAR' does not yet include "
            "context_call_args. Named parameters for MCP require a grammar extension "
            "(out of scope for this milestone)."
        )
    )
    def test_mcp_params_passed_to_provider(self, engine):
        provider = _ParamCapture()
        engine.register_mcp_provider("paramtest", provider)
        engine.execute(
            "SELECT invoice_id FROM invoices "
            "WHERE CONTEXT IN (MCP(paramtest, threshold := 0.7));"
        )
        assert "threshold" in provider.captured_params
        assert provider.captured_params["threshold"] == "0.7"

    def test_mcp_no_params_gives_empty_dict(self, engine):
        provider = _ParamCapture()
        engine.register_mcp_provider("noparams", provider)
        engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(noparams));"
        )
        assert provider.captured_params == {}


# ─────────────────────────────────────────────────────────────────────────────
# Section 4: MCP timeout
# ─────────────────────────────────────────────────────────────────────────────


class TestMCPTimeout:
    def test_timeout_warn_returns_empty(self):
        e = cql.Engine(mcp_timeout_ms=50)
        df = pd.DataFrame({"invoice_id": [1, 2], "status": ["open", "open"]})
        e.register_table("invoices", df, primary_key="invoice_id")
        e.register_mcp_provider("slow", _SleepyProvider())

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = e.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(slow));"
            )
        assert result.row_count == 0
        assert any("timed out" in str(warning.message) for warning in w)

    def test_timeout_error_raises(self):
        e = cql.Engine(mcp_timeout_ms=50, mcp_timeout_behavior="error")
        df = pd.DataFrame({"invoice_id": [1], "status": ["open"]})
        e.register_table("invoices", df, primary_key="invoice_id")
        e.register_mcp_provider("slow", _SleepyProvider())

        with pytest.raises(RuntimeError, match="timed out"):
            e.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(slow));"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Section 5: REMOTE registration and resolution
# ─────────────────────────────────────────────────────────────────────────────


_ISSUE_ROWS = [
    {"issue_id": "JIRA-1", "status": "open", "priority": "high"},
    {"issue_id": "JIRA-2", "status": "closed", "priority": "medium"},
    {"issue_id": "JIRA-3", "status": "open", "priority": "low"},
]


class TestREMOTERegistration:
    def test_register_stores_provider(self):
        e = cql.Engine()
        provider = _StaticRemoteProvider(_ISSUE_ROWS)
        e.register_remote_provider("jira", provider)
        assert e._remote_providers["jira"] is provider

    def test_remote_provider_satisfies_protocol(self):
        assert isinstance(_StaticRemoteProvider([]), RemoteProvider)

    def test_remote_result_is_dataclass(self):
        r = RemoteResult(rows=[{"a": 1}])
        assert r.rows == [{"a": 1}]

    def test_unregistered_remote_raises(self):
        e = cql.Engine()
        with pytest.raises(ValueError, match="REMOTE provider 'missing'"):
            e.execute("SELECT issue_id FROM REMOTE(missing.issues);")

    def test_remote_error_message_mentions_register_call(self):
        e = cql.Engine()
        with pytest.raises(ValueError, match="register_remote_provider"):
            e.execute("SELECT issue_id FROM REMOTE(missing.issues);")

    def test_remote_rows_are_queryable(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute("SELECT issue_id FROM REMOTE(jira.issues);")
        ids = sorted(result.to_pandas()["issue_id"].tolist())
        assert ids == ["JIRA-1", "JIRA-2", "JIRA-3"]

    def test_remote_row_count(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute("SELECT issue_id FROM REMOTE(jira.issues);")
        assert result.row_count == 3

    def test_remote_where_filter(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute(
            "SELECT issue_id FROM REMOTE(jira.issues) WHERE status = 'open';"
        )
        ids = sorted(result.to_pandas()["issue_id"].tolist())
        assert ids == ["JIRA-1", "JIRA-3"]

    def test_remote_columns_accessible_in_select(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute(
            "SELECT issue_id, status, priority FROM REMOTE(jira.issues);"
        )
        assert set(result.columns) == {"issue_id", "status", "priority"}


# ─────────────────────────────────────────────────────────────────────────────
# Section 6: REMOTE with alias
# ─────────────────────────────────────────────────────────────────────────────


class TestREMOTEAlias:
    def test_alias_in_where(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute(
            "SELECT j.issue_id FROM REMOTE(jira.issues) AS j "
            "WHERE j.status = 'open';"
        )
        assert result.row_count == 2

    def test_alias_in_select(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        result = e.execute(
            "SELECT j.issue_id, j.priority FROM REMOTE(jira.issues) AS j "
            "WHERE j.status = 'closed';"
        )
        df = result.to_pandas()
        assert list(df["issue_id"]) == ["JIRA-2"]
        assert list(df["priority"]) == ["medium"]


# ─────────────────────────────────────────────────────────────────────────────
# Section 7: REMOTE timeout
# ─────────────────────────────────────────────────────────────────────────────


class TestREMOTETimeout:
    def test_remote_timeout_raises(self):
        e = cql.Engine(remote_timeout_ms=50)
        e.register_remote_provider("slow", _SleepyRemoteProvider())
        with pytest.raises(RuntimeError, match="timed out"):
            e.execute("SELECT x FROM REMOTE(slow.data);")


# ─────────────────────────────────────────────────────────────────────────────
# Section 8: EXPLAIN output
# ─────────────────────────────────────────────────────────────────────────────


class TestExplain:
    def test_explain_mcp_contains_context_resolve(self, engine):
        engine.register_mcp_provider("fraud_model", _TopInvoices())
        plan = engine.explain(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(fraud_model));"
        )
        assert "ContextResolve (MCP)" in plan
        assert "provider=fraud_model" in plan

    def test_explain_mcp_contains_entity_type(self, engine):
        engine.register_mcp_provider("fraud_model", _TopInvoices())
        plan = engine.explain(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(fraud_model));"
        )
        assert "entity_type=invoices" in plan

    def test_explain_remote_contains_remote_scan(self):
        e = cql.Engine()
        e.register_remote_provider("jira", _StaticRemoteProvider(_ISSUE_ROWS))
        plan = e.explain("SELECT issue_id FROM REMOTE(jira.issues);")
        assert "RemoteScan" in plan
        assert "provider=jira" in plan
        assert "resource=issues" in plan

    def test_explain_normal_query_no_mcp_nodes(self, engine):
        plan = engine.explain(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (open_invoice);"
        )
        assert "ContextResolve (MCP)" not in plan
        assert "RemoteScan" not in plan


# ─────────────────────────────────────────────────────────────────────────────
# Section 9: Public exports
# ─────────────────────────────────────────────────────────────────────────────


class TestPublicExports:
    def test_mcp_provider_importable_from_cql(self):
        from contextql import MCPProvider  # noqa: F401

    def test_mcp_result_importable_from_cql(self):
        from contextql import MCPResult  # noqa: F401

    def test_remote_provider_importable_from_cql(self):
        from contextql import RemoteProvider  # noqa: F401

    def test_remote_result_importable_from_cql(self):
        from contextql import RemoteResult  # noqa: F401

    def test_providers_in_all(self):
        import contextql
        for name in ("MCPProvider", "MCPResult", "RemoteProvider", "RemoteResult"):
            assert name in contextql.__all__
