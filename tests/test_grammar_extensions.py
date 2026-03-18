"""Tests for grammar extensions: window functions, GLOBAL(), ZSCORE()."""
from __future__ import annotations

import pytest

from contextql.parser import ContextQLParser
from contextql.semantic import SemanticLowerer, QueryModel


def parse_lower(sql: str) -> QueryModel:
    parser = ContextQLParser()
    tree = parser.parse(sql)
    stmts = SemanticLowerer().lower(tree)
    assert stmts, "Expected at least one statement"
    stmt = stmts[0]
    assert isinstance(stmt, QueryModel)
    return stmt


# ── Grammar: OVER clause (window functions) ──────────────────────────────────


class TestWindowFunctionGrammar:
    def test_simple_over_empty_spec(self):
        q = parse_lower("SELECT SUM(amount) OVER () AS total FROM invoices;")
        proj = " ".join(q.projections)
        assert "OVER" in proj.upper()
        assert "total" in proj.lower()

    def test_partition_by(self):
        q = parse_lower(
            "SELECT SUM(amount) OVER (PARTITION BY vendor_id) AS vendor_sum FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "PARTITION" in proj.upper()
        assert "vendor_id" in proj

    def test_order_by_in_over(self):
        q = parse_lower(
            "SELECT SUM(amount) OVER (ORDER BY invoice_id) AS running FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "ORDER" in proj.upper()
        assert "invoice_id" in proj

    def test_partition_and_order(self):
        q = parse_lower(
            "SELECT AVG(amount) OVER (PARTITION BY vendor_id ORDER BY invoice_date) AS avg "
            "FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "PARTITION" in proj.upper()
        assert "ORDER" in proj.upper()

    def test_frame_rows_between(self):
        q = parse_lower(
            "SELECT SUM(amount) OVER "
            "(ORDER BY invoice_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ma "
            "FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "ROWS" in proj.upper()
        assert "PRECEDING" in proj.upper()

    def test_frame_unbounded_preceding(self):
        q = parse_lower(
            "SELECT SUM(amount) OVER "
            "(ORDER BY invoice_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rt "
            "FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "UNBOUNDED" in proj.upper()

    def test_row_number(self):
        q = parse_lower(
            "SELECT ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY amount DESC) AS rn "
            "FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "ROW_NUMBER" in proj.upper()

    def test_rank(self):
        q = parse_lower(
            "SELECT RANK() OVER (ORDER BY amount DESC) AS rnk FROM invoices;"
        )
        proj = " ".join(q.projections)
        assert "RANK" in proj.upper()

    def test_window_does_not_break_existing_projections(self):
        q = parse_lower(
            "SELECT invoice_id, amount, SUM(amount) OVER () AS total FROM invoices;"
        )
        assert "invoice_id" in " ".join(q.projections)
        assert "amount" in " ".join(q.projections)
        assert "total" in " ".join(q.projections)


# ── Semantic lowerer: GLOBAL() expansion ─────────────────────────────────────


class TestGlobalCallLowering:
    def test_global_sum_expands_to_over(self):
        q = parse_lower("SELECT GLOBAL(SUM(amount)) AS total FROM invoices;")
        proj = " ".join(q.projections)
        assert "OVER" in proj.upper()
        # Should not contain the GLOBAL keyword — it's been expanded
        assert "GLOBAL" not in proj.upper()

    def test_global_sum_with_alias(self):
        q = parse_lower("SELECT GLOBAL(SUM(amount)) AS grand_total FROM invoices;")
        proj = " ".join(q.projections)
        assert "grand_total" in proj.lower()

    def test_global_avg(self):
        q = parse_lower("SELECT GLOBAL(AVG(amount)) AS avg_total FROM invoices;")
        proj = " ".join(q.projections)
        assert "AVG" in proj.upper()
        assert "OVER" in proj.upper()

    def test_global_count(self):
        q = parse_lower("SELECT GLOBAL(COUNT(*)) AS total_count FROM invoices;")
        proj = " ".join(q.projections)
        assert "COUNT" in proj.upper()
        assert "OVER" in proj.upper()

    def test_global_in_arithmetic_expr(self):
        q = parse_lower(
            "SELECT invoice_id, amount / GLOBAL(SUM(amount)) AS pct FROM invoices;"
        )
        # The projection for pct should contain OVER and not GLOBAL
        proj_pct = [p for p in q.projections if "pct" in p.lower()]
        assert proj_pct, "Expected pct projection"
        assert "OVER" in proj_pct[0].upper()
        assert "GLOBAL" not in proj_pct[0].upper()

    def test_global_mixed_with_other_projections(self):
        q = parse_lower(
            "SELECT invoice_id, GLOBAL(SUM(amount)) AS total FROM invoices;"
        )
        assert len(q.projections) == 2
        assert "invoice_id" in q.projections[0].lower()


# ── Semantic lowerer: ZSCORE() expansion ─────────────────────────────────────


class TestZscoreCallLowering:
    def test_zscore_expands_to_window_expr(self):
        q = parse_lower("SELECT ZSCORE(amount) AS z FROM invoices;")
        proj = " ".join(q.projections)
        # Should contain AVG and STDDEV_SAMP, not ZSCORE
        assert "ZSCORE" not in proj.upper()
        assert "AVG" in proj.upper()
        assert "STDDEV_SAMP" in proj.upper()
        assert "OVER" in proj.upper()

    def test_zscore_with_alias(self):
        q = parse_lower("SELECT invoice_id, ZSCORE(amount) AS z_score FROM invoices;")
        proj = " ".join(q.projections)
        assert "z_score" in proj.lower()

    def test_zscore_contains_nullif(self):
        q = parse_lower("SELECT ZSCORE(amount) AS z FROM invoices;")
        proj = " ".join(q.projections)
        assert "NULLIF" in proj.upper()

    def test_zscore_different_column(self):
        q = parse_lower("SELECT ZSCORE(risk_score) AS z FROM vendors;")
        proj = " ".join(q.projections)
        assert "risk_score" in proj.lower()

    def test_zscore_mixed_with_other_projections(self):
        q = parse_lower("SELECT vendor_id, ZSCORE(risk_score) AS z FROM vendors;")
        assert len(q.projections) == 2
        assert "vendor_id" in q.projections[0].lower()


# ── End-to-end: DuckDB execution ─────────────────────────────────────────────


class TestWindowFunctionExecution:
    @pytest.fixture
    def engine(self):
        import contextql as cql
        return cql.demo()

    def test_running_total(self, engine):
        r = engine.execute(
            "SELECT invoice_id, amount, "
            "SUM(amount) OVER (ORDER BY invoice_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            " AS running_total "
            "FROM invoices "
            "ORDER BY invoice_id LIMIT 3;"
        )
        df = r.to_pandas()
        assert "running_total" in df.columns
        assert list(df["running_total"]) == pytest.approx(
            [df["amount"].iloc[0],
             df["amount"].iloc[0] + df["amount"].iloc[1],
             df["amount"].iloc[0] + df["amount"].iloc[1] + df["amount"].iloc[2]]
        )

    def test_global_market_share(self, engine):
        r = engine.execute(
            "SELECT invoice_id, amount, amount / GLOBAL(SUM(amount)) AS pct "
            "FROM invoices LIMIT 3;"
        )
        df = r.to_pandas()
        assert "pct" in df.columns
        assert all(df["pct"] > 0)
        assert all(df["pct"] < 1)

    def test_global_sum_constant_across_rows(self, engine):
        r = engine.execute(
            "SELECT invoice_id, GLOBAL(SUM(amount)) AS grand_total FROM invoices LIMIT 5;"
        )
        df = r.to_pandas()
        # All rows should have the same grand total
        assert df["grand_total"].nunique() == 1

    def test_zscore_numeric(self, engine):
        r = engine.execute(
            "SELECT invoice_id, ZSCORE(amount) AS z FROM invoices LIMIT 10;"
        )
        df = r.to_pandas()
        assert "z" in df.columns
        # Z-scores can be positive or negative
        assert df["z"].notna().all()

    def test_row_number_window(self, engine):
        r = engine.execute(
            "SELECT vendor_id, SUM(amount) AS total, "
            "ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rank "
            "FROM invoices GROUP BY vendor_id ORDER BY rank LIMIT 5;"
        )
        df = r.to_pandas()
        assert list(df["rank"]) == [1, 2, 3, 4, 5]

    def test_window_with_context_filter(self, engine):
        r = engine.execute(
            "SELECT invoice_id, amount, ZSCORE(amount) AS z "
            "FROM invoices WHERE CONTEXT IN (open_invoice) LIMIT 5;"
        )
        df = r.to_pandas()
        assert len(df) == 5
        assert "z" in df.columns
