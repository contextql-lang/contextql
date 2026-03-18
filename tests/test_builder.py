"""Tests for contextql._builder.QueryBuilder."""
from __future__ import annotations

import pytest

from contextql._builder import QueryBuilder


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def engine():
    import contextql as cql
    return cql.demo()


@pytest.fixture
def bare_engine():
    import contextql as cql
    import pandas as pd

    e = cql.Engine()
    df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": [10, 50, 30, 20, 40], "active": [True, True, False, True, False]})
    e.register_table("items", df, primary_key="id")
    e.register_context("active_item", "SELECT id FROM items WHERE active = true", entity_key="id")
    return e


# ── SQL generation (build()) ─────────────────────────────────────────────────


class TestQueryBuilderBuild:
    def test_simple_select_star(self, bare_engine):
        sql = bare_engine.query("items").build()
        assert "SELECT *" in sql
        assert "FROM items" in sql

    def test_select_columns(self, bare_engine):
        sql = bare_engine.query("items").select("id", "val").build()
        assert "SELECT id, val" in sql

    def test_context_predicate_no_alias(self, bare_engine):
        sql = bare_engine.query("items").where_context("active_item").build()
        assert "CONTEXT IN (active_item)" in sql

    def test_context_predicate_with_alias(self, bare_engine):
        sql = bare_engine.query("items AS i").where_context("active_item", table_alias="i").build()
        assert "CONTEXT ON i IN (active_item)" in sql

    def test_context_predicate_negated(self, bare_engine):
        sql = bare_engine.query("items").where_context("active_item", negated=True).build()
        assert "CONTEXT NOT IN (active_item)" in sql

    def test_context_predicate_all_mode(self, bare_engine):
        sql = bare_engine.query("items").where_context("active_item", all_mode=True).build()
        assert "CONTEXT IN ALL (active_item)" in sql

    def test_multiple_context_predicates_joined_and(self, engine):
        sql = (engine.query("invoices AS i")
               .where_context("open_invoice", table_alias="i")
               .where_context("risky_vendor WEIGHT 1.5", table_alias="v")
               .build())
        assert "CONTEXT ON i IN (open_invoice)" in sql
        assert "CONTEXT ON v IN (risky_vendor WEIGHT 1.5)" in sql
        assert "AND" in sql

    def test_context_with_weight_in_string(self, engine):
        sql = engine.query("invoices").where_context("high_value WEIGHT 2.0").build()
        assert "high_value WEIGHT 2.0" in sql

    def test_where_plain_condition(self, bare_engine):
        sql = bare_engine.query("items").where("val > 20").build()
        assert "WHERE val > 20" in sql

    def test_where_combined_plain_and_context(self, bare_engine):
        sql = bare_engine.query("items").where("val > 20").where_context("active_item").build()
        assert "val > 20" in sql
        assert "CONTEXT IN (active_item)" in sql
        assert sql.count("WHERE") == 1

    def test_join(self, engine):
        sql = (engine.query("invoices AS i")
               .join("vendors AS v", on="i.vendor_id = v.vendor_id")
               .build())
        assert "JOIN vendors AS v ON i.vendor_id = v.vendor_id" in sql

    def test_left_join(self, engine):
        sql = (engine.query("invoices AS i")
               .join("vendors AS v", on="i.vendor_id = v.vendor_id", how="LEFT JOIN")
               .build())
        assert "LEFT JOIN" in sql

    def test_order_by_context_desc(self, engine):
        sql = engine.query("invoices").where_context("open_invoice").order_by_context().build()
        assert "ORDER BY CONTEXT DESC" in sql

    def test_order_by_context_asc(self, engine):
        sql = engine.query("invoices").where_context("open_invoice").order_by_context(desc=False).build()
        assert "ORDER BY CONTEXT ASC" in sql

    def test_order_by_column(self, bare_engine):
        sql = bare_engine.query("items").order_by("val DESC").build()
        assert "ORDER BY val DESC" in sql

    def test_order_by_context_and_column(self, engine):
        sql = (engine.query("invoices")
               .where_context("open_invoice")
               .order_by_context()
               .order_by("invoice_id ASC")
               .build())
        assert "ORDER BY CONTEXT DESC" in sql
        assert "invoice_id ASC" in sql

    def test_limit(self, bare_engine):
        sql = bare_engine.query("items").limit(5).build()
        assert "LIMIT 5" in sql

    def test_offset(self, bare_engine):
        sql = bare_engine.query("items").limit(10).offset(20).build()
        assert "LIMIT 10" in sql
        assert "OFFSET 20" in sql

    def test_sql_ends_with_semicolon(self, bare_engine):
        sql = bare_engine.query("items").build()
        assert sql.endswith(";")

    def test_context_score_in_select(self, engine):
        sql = (engine.query("invoices")
               .select("invoice_id", "CONTEXT_SCORE() AS score")
               .where_context("open_invoice")
               .build())
        assert "CONTEXT_SCORE() AS score" in sql


# ── Execution (execute()) ─────────────────────────────────────────────────────


class TestQueryBuilderExecute:
    def test_simple_execute_returns_result(self, bare_engine):
        result = bare_engine.query("items").execute()
        assert result.row_count == 5

    def test_context_filter(self, bare_engine):
        result = bare_engine.query("items").where_context("active_item").execute()
        df = result.to_pandas()
        assert set(df["id"].tolist()) == {1, 2, 4}

    def test_select_columns_present(self, bare_engine):
        result = bare_engine.query("items").select("id", "val").execute()
        assert result.columns == ["id", "val"]

    def test_limit_applied(self, engine):
        result = engine.query("invoices").limit(3).execute()
        assert result.row_count == 3

    def test_order_by_context_scored(self, engine):
        result = (engine.query("invoices")
                  .select("invoice_id", "CONTEXT_SCORE() AS s")
                  .where_context("overdue_invoice")
                  .order_by_context(desc=True)
                  .execute())
        scores = result.to_pandas()["s"].tolist()
        assert scores == sorted(scores, reverse=True)

    def test_join_result(self, engine):
        result = (engine.query("invoices AS i")
                  .select("i.invoice_id", "v.vendor_name")
                  .join("vendors AS v", on="i.vendor_id = v.vendor_id")
                  .where_context("open_invoice", table_alias="i")
                  .limit(5)
                  .execute())
        df = result.to_pandas()
        assert "invoice_id" in df.columns
        assert "vendor_name" in df.columns

    def test_where_plain_condition_filters(self, bare_engine):
        result = (bare_engine.query("items")
                  .select("id", "val")
                  .where("val > 30")
                  .execute())
        df = result.to_pandas()
        assert all(df["val"] > 30)

    def test_negated_context(self, bare_engine):
        result = bare_engine.query("items").where_context("active_item", negated=True).execute()
        df = result.to_pandas()
        assert set(df["id"].tolist()) == {3, 5}


# ── explain() ────────────────────────────────────────────────────────────────


class TestQueryBuilderExplain:
    def test_explain_returns_string(self, engine):
        plan = engine.query("invoices").where_context("open_invoice").explain()
        assert isinstance(plan, str)
        assert "SELECT" in plan.upper()

    def test_explain_contains_context(self, engine):
        plan = engine.query("invoices").where_context("open_invoice").explain()
        assert "open_invoice" in plan


# ── @context decorator ───────────────────────────────────────────────────────


class TestContextDecorator:
    def test_decorator_registers_context(self, bare_engine):
        import contextql as cql
        import pandas as pd

        e = cql.Engine()
        df = pd.DataFrame({"item_id": [1, 2, 3], "score": [0.1, 0.9, 0.5]})
        e.register_table("scores", df, primary_key="item_id")

        @e.context("high_score", entity_key="item_id")
        def high_score():
            return "SELECT item_id FROM scores WHERE score > 0.7"

        assert "high_score" in e.catalog.contexts()
        result = e.execute("SELECT item_id FROM scores WHERE CONTEXT IN (high_score);")
        assert result.to_pandas()["item_id"].tolist() == [2]

    def test_decorator_with_score(self, bare_engine):
        import contextql as cql
        import pandas as pd

        e = cql.Engine()
        df = pd.DataFrame({"item_id": [1, 2, 3], "score": [0.1, 0.9, 0.5]})
        e.register_table("scores", df, primary_key="item_id")

        @e.context("scored_item", entity_key="item_id",
                   has_score=True, score_column="score")
        def scored_item():
            return "SELECT item_id, score FROM scores WHERE score > 0.3"

        result = e.execute(
            "SELECT item_id, CONTEXT_SCORE() AS s FROM scores "
            "WHERE CONTEXT IN (scored_item) ORDER BY s DESC;"
        )
        df = result.to_pandas()
        assert list(df["item_id"]) == [2, 3]
        assert df["s"].iloc[0] == pytest.approx(0.9)


# ── QueryBuilder repr ─────────────────────────────────────────────────────────


class TestQueryBuilderRepr:
    def test_repr_contains_table(self, bare_engine):
        qb = bare_engine.query("items")
        r = repr(qb)
        assert "items" in r
        assert "QueryBuilder" in r
