"""Tests for contextql.executor and contextql.adapters.duckdb_adapter."""
from __future__ import annotations

import pytest
import pandas as pd

from contextql.adapters.duckdb_adapter import DuckDBAdapter, DuckDBRegisteredContext
from contextql.executor import ContextQLExecutor
from contextql.semantic import (
    ContextCatalogEntry,
    EntityKeyType,
    InMemoryCatalog,
    TableCatalogEntry,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def invoices_df():
    return pd.DataFrame({
        "invoice_id": [1, 2, 3, 4, 5],
        "vendor_id":  [10, 11, 10, 12, 11],
        "amount":     [100, 500, 200, 800, 50],
        "status":     ["open", "open", "paid", "open", "open"],
    })


@pytest.fixture
def vendors_df():
    return pd.DataFrame({
        "vendor_id":   [10, 11, 12],
        "vendor_name": ["Alpha", "Beta", "Gamma"],
        "risk_score":  [0.2, 0.9, 0.8],
    })


@pytest.fixture
def adapter(invoices_df, vendors_df):
    a = DuckDBAdapter()
    a.register_table("invoices", invoices_df)
    a.register_table("vendors", vendors_df)
    return a


@pytest.fixture
def catalog():
    return InMemoryCatalog(
        contexts={
            "open_invoice": ContextCatalogEntry(
                "open_invoice", "invoice_id", EntityKeyType.INT64
            ),
            "high_value": ContextCatalogEntry(
                "high_value", "invoice_id", EntityKeyType.INT64, has_score=True
            ),
            "risky_vendor": ContextCatalogEntry(
                "risky_vendor", "vendor_id", EntityKeyType.INT64, has_score=True
            ),
        },
        tables={
            "invoices": TableCatalogEntry("invoices", "i", "invoice_id", EntityKeyType.INT64),
            "vendors":  TableCatalogEntry("vendors",  "v", "vendor_id",  EntityKeyType.INT64),
        },
    )


@pytest.fixture
def executor(catalog, adapter):
    adapter.register_context(
        "open_invoice",
        "SELECT invoice_id FROM invoices WHERE status = 'open'",
        entity_key_name="invoice_id",
        has_score=False,
    )
    adapter.register_context(
        "high_value",
        "SELECT invoice_id, amount / 1000.0 AS val_score FROM invoices WHERE amount >= 300",
        entity_key_name="invoice_id",
        has_score=True,
        score_column_name="val_score",
    )
    adapter.register_context(
        "risky_vendor",
        "SELECT vendor_id, risk_score FROM vendors WHERE risk_score >= 0.7",
        entity_key_name="vendor_id",
        has_score=True,
        score_column_name="risk_score",
    )
    return ContextQLExecutor(catalog=catalog, adapter=adapter)


# ── DuckDBAdapter ────────────────────────────────────────────────────────────


class TestDuckDBAdapter:
    def test_register_and_list_tables(self, adapter):
        tables = adapter.list_tables()
        assert "invoices" in tables
        assert "vendors" in tables

    def test_execute_df(self, adapter):
        df = adapter.execute_df("SELECT COUNT(*) AS n FROM invoices")
        assert df["n"].iloc[0] == 5

    def test_register_context_stores_metadata(self, adapter):
        adapter.register_context(
            "test_ctx", "SELECT invoice_id FROM invoices", entity_key_name="invoice_id"
        )
        ctx = adapter.get_context("test_ctx")
        assert isinstance(ctx, DuckDBRegisteredContext)
        assert ctx.entity_key_name == "invoice_id"
        assert ctx.has_score is False

    def test_get_context_unknown_raises(self, adapter):
        with pytest.raises(KeyError):
            adapter.get_context("nonexistent")

    def test_register_context_no_replace_raises(self, adapter):
        adapter.register_context("dup", "SELECT 1 AS invoice_id", entity_key_name="invoice_id")
        with pytest.raises(ValueError, match="already exists"):
            adapter.register_context("dup", "SELECT 1 AS invoice_id", entity_key_name="invoice_id", replace=False)

    def test_list_contexts(self, adapter):
        adapter.register_context("c1", "SELECT invoice_id FROM invoices", entity_key_name="invoice_id")
        assert "c1" in adapter.list_contexts()

    def test_resolve_context_keys_boolean(self, adapter):
        adapter.register_context(
            "open_inv",
            "SELECT invoice_id FROM invoices WHERE status = 'open'",
            entity_key_name="invoice_id",
        )
        keys = adapter.resolve_context_keys("open_inv")
        assert keys == {1, 2, 4, 5}

    def test_resolve_context_score_map(self, adapter):
        adapter.register_context(
            "hv",
            "SELECT invoice_id, amount / 1000.0 AS score FROM invoices WHERE amount >= 300",
            entity_key_name="invoice_id",
            has_score=True,
            score_column_name="score",
        )
        score_map = adapter.resolve_context_score_map("hv")
        assert set(score_map.keys()) == {2, 4}
        assert score_map[4] == pytest.approx(0.8)

    def test_resolve_context_score_map_unscored_returns_empty(self, adapter):
        adapter.register_context(
            "bool_ctx", "SELECT invoice_id FROM invoices", entity_key_name="invoice_id"
        )
        assert adapter.resolve_context_score_map("bool_ctx") == {}

    def test_resolve_context_df_missing_key_column_raises(self, adapter):
        adapter.register_context(
            "bad", "SELECT amount FROM invoices", entity_key_name="invoice_id"
        )
        with pytest.raises(ValueError, match="entity key column"):
            adapter.resolve_context_df("bad")


# ── ContextQLExecutor — basic filtering ─────────────────────────────────────


class TestExecutorFiltering:
    def test_simple_boolean_context_filter(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, status FROM invoices WHERE CONTEXT IN (open_invoice);"
        )
        df = result.dataframe
        assert set(df["invoice_id"].tolist()) == {1, 2, 4, 5}
        assert "paid" not in df["status"].tolist()

    def test_not_in_negation(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id FROM invoices WHERE CONTEXT NOT IN (open_invoice);"
        )
        df = result.dataframe
        assert df["invoice_id"].tolist() == [3]

    def test_non_context_where_passes_through(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id FROM invoices WHERE amount > 400 AND CONTEXT IN (open_invoice);"
        )
        df = result.dataframe
        # open invoices (1,2,4,5) with amount > 400 → invoice 2 (500) and 4 (800)
        assert set(df["invoice_id"].tolist()) == {2, 4}

    def test_empty_result_when_no_matches(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (open_invoice) AND amount > 9999;"
        )
        assert result.dataframe.empty

    def test_no_context_predicate_returns_all_rows(self, executor):
        result = executor.execute_sql("SELECT invoice_id FROM invoices;")
        assert len(result.dataframe) == 5


# ── ContextQLExecutor — scoring ─────────────────────────────────────────────


class TestExecutorScoring:
    def test_context_score_column_renamed_to_alias(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS priority FROM invoices WHERE CONTEXT IN (high_value);"
        )
        assert "priority" in result.dataframe.columns
        assert "__context_score" not in result.dataframe.columns

    def test_context_count_not_in_output_when_not_selected(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (high_value);"
        )
        assert "__context_count" not in result.dataframe.columns

    def test_scored_context_scores_nonzero(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (high_value);"
        )
        df = result.dataframe
        # high_value context covers invoices 2 (500→0.5) and 4 (800→0.8)
        assert set(df["invoice_id"].tolist()) == {2, 4}
        assert all(df["s"] > 0)

    def test_weight_scales_score(self, executor):
        # WEIGHT 2.0 should double the score
        r1 = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (high_value);"
        )
        r2 = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (high_value WEIGHT 2.0);"
        )
        df1 = r1.dataframe.set_index("invoice_id")
        df2 = r2.dataframe.set_index("invoice_id")
        for idx in df1.index:
            assert df2.loc[idx, "s"] == pytest.approx(df1.loc[idx, "s"] * 2.0)

    def test_boolean_context_membership_is_one(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (open_invoice);"
        )
        df = result.dataframe
        assert df["s"].tolist() == pytest.approx([1.0] * len(df))


# ── ContextQLExecutor — ORDER BY CONTEXT ────────────────────────────────────


class TestExecutorOrdering:
    def test_order_by_context_desc(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (high_value) ORDER BY CONTEXT DESC;"
        )
        scores = result.dataframe["s"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_order_by_context_asc(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (high_value) ORDER BY CONTEXT ASC;"
        )
        scores = result.dataframe["s"].tolist()
        assert scores == sorted(scores)

    def test_limit_applied_after_order(self, executor):
        result = executor.execute_sql(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (high_value) ORDER BY CONTEXT DESC LIMIT 1;"
        )
        df = result.dataframe
        assert len(df) == 1
        # highest score = invoice 4 (amount 800 → 0.8)
        assert df["invoice_id"].iloc[0] == 4


# ── ContextQLExecutor — JOIN with ON condition ───────────────────────────────


class TestExecutorJoin:
    def test_join_on_condition_in_generated_sql(self, executor):
        result = executor.execute_sql(
            "SELECT i.invoice_id, v.vendor_name FROM invoices AS i "
            "JOIN vendors AS v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON i IN (open_invoice);"
        )
        assert "ON" in result.generated_sql.upper()
        assert "vendor_id" in result.generated_sql

    def test_join_result_has_correct_columns(self, executor):
        result = executor.execute_sql(
            "SELECT i.invoice_id, v.vendor_name FROM invoices AS i "
            "JOIN vendors AS v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON i IN (open_invoice);"
        )
        df = result.dataframe
        assert "invoice_id" in df.columns
        assert "vendor_name" in df.columns

    def test_join_two_context_predicates(self, executor):
        result = executor.execute_sql(
            "SELECT i.invoice_id, v.vendor_name, CONTEXT_SCORE() AS s "
            "FROM invoices AS i "
            "JOIN vendors AS v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON i IN (open_invoice) "
            "AND CONTEXT ON v IN (risky_vendor);"
        )
        df = result.dataframe
        # open invoices: 1(v10), 2(v11), 4(v12), 5(v11)
        # risky vendors: 11 (0.9), 12 (0.8) — not 10
        # intersection: 2(v11), 4(v12), 5(v11)
        assert 1 not in df["invoice_id"].tolist()
        assert set(df["invoice_id"].tolist()) == {2, 4, 5}

    def test_extra_key_col_not_in_output(self, executor):
        result = executor.execute_sql(
            "SELECT i.invoice_id, v.vendor_name FROM invoices AS i "
            "JOIN vendors AS v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON v IN (risky_vendor);"
        )
        # vendor_id was added as extra key col for risky_vendor resolution
        # but the user didn't SELECT it — it must not appear in output
        assert "vendor_id" not in result.dataframe.columns


# ── ContextQLExecutor — error handling ──────────────────────────────────────


class TestExecutorErrors:
    def test_undefined_context_raises(self, executor):
        with pytest.raises(ValueError):
            executor.execute_sql(
                "SELECT * FROM invoices WHERE CONTEXT IN (nonexistent_ctx);"
            )

    def test_ddl_statement_raises(self, executor):
        with pytest.raises(ValueError, match="SELECT queries only"):
            executor.execute_sql(
                "CREATE CONTEXT x ON id AS SELECT id FROM t;"
            )

    def test_analysis_result_attached(self, executor):
        result = executor.execute_sql("SELECT invoice_id FROM invoices;")
        assert result.analysis is not None
        assert result.analysis.ok is True

    def test_generated_sql_attached(self, executor):
        result = executor.execute_sql("SELECT invoice_id FROM invoices;")
        assert result.generated_sql is not None
        assert "SELECT" in result.generated_sql.upper()


# ── _collect_extra_key_cols / _strip_context_predicates ─────────────────────


class TestInternalHelpers:
    def test_strip_context_predicates_pure_context(self, executor):
        result = executor._strip_context_predicates("CONTEXT ON i IN (ctx)")
        assert result == ""

    def test_strip_context_predicates_mixed(self, executor):
        result = executor._strip_context_predicates("amount > 100 AND CONTEXT IN (ctx)")
        assert "CONTEXT" not in result.upper()
        assert "amount" in result

    def test_strip_context_predicates_empty(self, executor):
        assert executor._strip_context_predicates("") == ""

    def test_collect_extra_key_cols_missing_key_added(self, executor, catalog):
        from contextql.semantic import (
            ContextPredicate, ContextReference, QueryModel, StatementKind, TableRef
        )
        # Simulate a query that doesn't project vendor_id but uses risky_vendor context
        q = QueryModel(
            kind=StatementKind.SELECT,
            raw_sql="",
            projections=["invoice_id"],  # vendor_id NOT here
            from_table=TableRef("invoices", "i"),
            context_predicates=[
                ContextPredicate(
                    binding_alias="v",
                    negated=False,
                    all_mode=False,
                    sequence_mode=False,
                    refs=[ContextReference("risky_vendor", "CONTEXT")],
                )
            ],
        )
        extra = executor._collect_extra_key_cols(q)
        assert "vendor_id" in extra

    def test_collect_extra_key_cols_already_projected(self, executor, catalog):
        from contextql.semantic import (
            ContextPredicate, ContextReference, QueryModel, StatementKind, TableRef
        )
        q = QueryModel(
            kind=StatementKind.SELECT,
            raw_sql="",
            projections=["invoice_id"],  # invoice_id IS here
            from_table=TableRef("invoices", "i"),
            context_predicates=[
                ContextPredicate(
                    binding_alias="i",
                    negated=False,
                    all_mode=False,
                    sequence_mode=False,
                    refs=[ContextReference("open_invoice", "CONTEXT")],
                )
            ],
        )
        extra = executor._collect_extra_key_cols(q)
        assert "invoice_id" not in extra

    def test_collect_extra_key_cols_mcp_skipped(self, executor):
        from contextql.semantic import (
            ContextPredicate, ContextReference, QueryModel, StatementKind, TableRef
        )
        q = QueryModel(
            kind=StatementKind.SELECT,
            raw_sql="",
            projections=["invoice_id"],
            from_table=TableRef("invoices"),
            context_predicates=[
                ContextPredicate(
                    binding_alias=None,
                    negated=False,
                    all_mode=False,
                    sequence_mode=False,
                    refs=[ContextReference("remote_ctx", "MCP")],
                )
            ],
        )
        extra = executor._collect_extra_key_cols(q)
        assert extra == {}
