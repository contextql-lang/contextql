"""Tests for the contextql.providers package.

Covers backward compatibility, concrete provider implementations,
RemoteResult.to_dataframe(), and protocol compliance.
"""

from __future__ import annotations

import pandas as pd
import pytest

import contextql as cql
from contextql.providers import (
    FraudDetectionMCP,
    JiraRemoteProvider,
    MCPProvider,
    MCPResult,
    PriorityMCP,
    RemoteProvider,
    RemoteResult,
)


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: Backward compatibility
# ─────────────────────────────────────────────────────────────────────────────


class TestBackwardCompatibility:
    def test_import_from_contextql_providers(self):
        """Original import path still works after file → package migration."""
        from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult
        assert MCPProvider is not None
        assert MCPResult is not None
        assert RemoteProvider is not None
        assert RemoteResult is not None

    def test_import_from_contextql_top_level(self):
        """Top-level re-exports still work."""
        assert hasattr(cql, "MCPProvider")
        assert hasattr(cql, "MCPResult")
        assert hasattr(cql, "RemoteProvider")
        assert hasattr(cql, "RemoteResult")

    def test_concrete_providers_importable_from_top_level(self):
        """New concrete providers are available at the top level."""
        assert hasattr(cql, "FraudDetectionMCP")
        assert hasattr(cql, "PriorityMCP")
        assert hasattr(cql, "JiraRemoteProvider")

    def test_import_from_base_submodule(self):
        """Direct import from base submodule works."""
        from contextql.providers.base import MCPProvider, MCPResult
        assert MCPProvider is not None
        assert MCPResult is not None


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: FraudDetectionMCP
# ─────────────────────────────────────────────────────────────────────────────


class TestFraudDetectionMCP:
    def test_satisfies_protocol(self):
        assert isinstance(FraudDetectionMCP(), MCPProvider)

    def test_custom_scores_and_threshold(self):
        scores = {1: 0.9, 2: 0.4, 3: 0.8, 4: 0.1}
        p = FraudDetectionMCP(scores=scores, threshold=0.5)
        result = p.resolve(entity_type="invoices", params={})
        assert set(result.entity_ids) == {1, 3}
        assert result.entity_type == "invoices"
        assert len(result.scores) == 2

    def test_threshold_override_via_params(self):
        scores = {1: 0.9, 2: 0.4, 3: 0.8}
        p = FraudDetectionMCP(scores=scores, threshold=0.5)
        result = p.resolve(entity_type="t", params={"threshold": "0.85"})
        assert result.entity_ids == [1]

    def test_limit(self):
        scores = {i: float(i) / 10 for i in range(1, 11)}
        p = FraudDetectionMCP(scores=scores, threshold=0.0)
        result = p.resolve(entity_type="t", params={}, limit=3)
        assert len(result.entity_ids) == 3
        assert result.entity_ids[0] == 10  # highest score first

    def test_default_scores_deterministic(self):
        p = FraudDetectionMCP()
        r1 = p.resolve(entity_type="x", params={})
        r2 = p.resolve(entity_type="x", params={})
        assert r1.entity_ids == r2.entity_ids
        assert r1.scores == r2.scores

    def test_callable_scores(self):
        def gen(entity_type):
            return {1: 0.9, 2: 0.1}

        p = FraudDetectionMCP(scores=gen, threshold=0.5)
        result = p.resolve(entity_type="t", params={})
        assert result.entity_ids == [1]

    def test_sorted_descending_by_score(self):
        scores = {1: 0.3, 2: 0.9, 3: 0.6}
        p = FraudDetectionMCP(scores=scores, threshold=0.0)
        result = p.resolve(entity_type="t", params={})
        assert result.scores == sorted(result.scores, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: PriorityMCP
# ─────────────────────────────────────────────────────────────────────────────


class TestPriorityMCP:
    def test_satisfies_protocol(self):
        assert isinstance(PriorityMCP(), MCPProvider)

    def test_normalization(self):
        scores = {1: 100.0, 2: 50.0, 3: 200.0}
        p = PriorityMCP(scores=scores, normalize=True)
        result = p.resolve(entity_type="t", params={})
        # entity 3 has max score → normalised to 1.0
        idx = result.entity_ids.index(3)
        assert result.scores[idx] == pytest.approx(1.0)

    def test_no_normalization(self):
        scores = {1: 100.0, 2: 50.0}
        p = PriorityMCP(scores=scores, normalize=False)
        result = p.resolve(entity_type="t", params={})
        assert max(result.scores) == pytest.approx(100.0)

    def test_limit(self):
        scores = {i: float(i) for i in range(1, 21)}
        p = PriorityMCP(scores=scores, normalize=False)
        result = p.resolve(entity_type="t", params={}, limit=5)
        assert len(result.entity_ids) == 5
        assert result.entity_ids[0] == 20

    def test_default_scores_deterministic(self):
        p = PriorityMCP()
        r1 = p.resolve(entity_type="x", params={})
        r2 = p.resolve(entity_type="x", params={})
        assert r1.entity_ids == r2.entity_ids

    def test_includes_all_entities(self):
        """PriorityMCP scores all entities, not just a filtered subset."""
        scores = {1: 0.1, 2: 0.2, 3: 0.3}
        p = PriorityMCP(scores=scores)
        result = p.resolve(entity_type="t", params={})
        assert len(result.entity_ids) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Section 4: JiraRemoteProvider
# ─────────────────────────────────────────────────────────────────────────────


class TestJiraRemoteProvider:
    def test_satisfies_protocol(self):
        assert isinstance(JiraRemoteProvider(), RemoteProvider)

    def test_synthetic_data_count(self):
        p = JiraRemoteProvider(num_issues=50)
        result = p.query(resource="issues", filters={}, columns=[])
        assert len(result.rows) == 50

    def test_deterministic_with_seed(self):
        p1 = JiraRemoteProvider(seed=99)
        p2 = JiraRemoteProvider(seed=99)
        r1 = p1.query(resource="issues", filters={}, columns=[])
        r2 = p2.query(resource="issues", filters={}, columns=[])
        assert r1.rows == r2.rows

    def test_static_rows(self):
        static = [{"issue_id": 1, "status": "OPEN"}]
        p = JiraRemoteProvider(rows=static)
        result = p.query(resource="issues", filters={}, columns=[])
        assert result.rows == static

    def test_limit(self):
        p = JiraRemoteProvider(num_issues=100)
        result = p.query(resource="issues", filters={}, columns=[], limit=5)
        assert len(result.rows) == 5

    def test_row_structure(self):
        p = JiraRemoteProvider(num_issues=1)
        result = p.query(resource="issues", filters={}, columns=[])
        row = result.rows[0]
        assert "issue_id" in row
        assert "status" in row
        assert "priority" in row
        assert "assigned_team" in row
        assert "story_points" in row


# ─────────────────────────────────────────────────────────────────────────────
# Section 5: RemoteResult.to_dataframe()
# ─────────────────────────────────────────────────────────────────────────────


class TestRemoteResultToDataframe:
    def test_list_of_dicts(self):
        r = RemoteResult(rows=[{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        df = r.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 2

    def test_pandas_dataframe_passthrough(self):
        original = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        r = RemoteResult(rows=original)
        df = r.to_dataframe()
        assert df is original  # same object, no copy

    def test_arrow_table(self):
        pytest.importorskip("pyarrow")
        import pyarrow as pa

        table = pa.table({"col": [10, 20, 30]})
        r = RemoteResult(rows=table)
        df = r.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert list(df["col"]) == [10, 20, 30]


# ─────────────────────────────────────────────────────────────────────────────
# Section 6: Integration — concrete providers with Engine
# ─────────────────────────────────────────────────────────────────────────────


class TestProviderIntegration:
    def test_fraud_mcp_with_engine(self):
        engine = cql.Engine()
        df = pd.DataFrame({"invoice_id": [1, 2, 3], "amount": [100, 200, 300]})
        engine.register_table("invoices", df, primary_key="invoice_id")

        scores = {1: 0.9, 2: 0.1, 3: 0.8}
        engine.register_mcp_provider(
            "fraud", FraudDetectionMCP(scores=scores, threshold=0.5)
        )
        result = engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(fraud));"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        assert ids == [1, 3]

    def test_jira_remote_with_engine(self):
        engine = cql.Engine()
        df = pd.DataFrame({"invoice_id": [1, 2, 3], "amount": [100, 200, 300]})
        engine.register_table("invoices", df, primary_key="invoice_id")
        engine.register_remote_provider("jira", JiraRemoteProvider(num_issues=10))

        result = engine.execute(
            "SELECT i.invoice_id, j.status "
            "FROM invoices AS i "
            "JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id;"
        )
        assert result.row_count == 3  # 3 invoices joined with 10 issues
