"""Tests for contextql.Result API enhancements."""
from __future__ import annotations

import pytest


@pytest.fixture
def engine():
    import contextql as cql
    return cql.demo()


@pytest.fixture
def small_result(engine):
    return engine.execute("SELECT invoice_id, amount FROM invoices ORDER BY invoice_id LIMIT 5;")


@pytest.fixture
def context_result(engine):
    return engine.execute(
        "SELECT invoice_id, CONTEXT_SCORE() AS s "
        "FROM invoices WHERE CONTEXT IN (overdue_invoice) "
        "ORDER BY CONTEXT DESC LIMIT 5;"
    )


# ── row_count ─────────────────────────────────────────────────────────────────


class TestRowCount:
    def test_row_count_correct(self, small_result):
        assert small_result.row_count == 5

    def test_row_count_matches_dataframe(self, small_result):
        assert small_result.row_count == len(small_result.to_pandas())

    def test_row_count_zero_for_empty(self, engine):
        r = engine.execute("SELECT invoice_id FROM invoices WHERE amount > 9999999;")
        assert r.row_count == 0


# ── columns ───────────────────────────────────────────────────────────────────


class TestColumns:
    def test_columns_list(self, small_result):
        assert small_result.columns == ["invoice_id", "amount"]

    def test_columns_with_alias(self, context_result):
        assert "invoice_id" in context_result.columns
        assert "s" in context_result.columns

    def test_columns_is_list_of_strings(self, small_result):
        cols = small_result.columns
        assert isinstance(cols, list)
        assert all(isinstance(c, str) for c in cols)


# ── show() ────────────────────────────────────────────────────────────────────


class TestShow:
    def test_show_prints_output(self, small_result, capsys):
        small_result.show()
        out = capsys.readouterr().out
        assert "invoice_id" in out
        assert "amount" in out

    def test_show_prints_row_count(self, small_result, capsys):
        small_result.show()
        out = capsys.readouterr().out
        assert "5 rows" in out

    def test_show_max_rows_truncates(self, engine, capsys):
        r = engine.execute("SELECT invoice_id FROM invoices LIMIT 20;")
        r.show(max_rows=5)
        out = capsys.readouterr().out
        assert "more rows not shown" in out

    def test_show_no_truncation_when_rows_le_max(self, small_result, capsys):
        small_result.show(max_rows=100)
        out = capsys.readouterr().out
        assert "not shown" not in out

    def test_show_singular_row(self, engine, capsys):
        r = engine.execute("SELECT invoice_id FROM invoices LIMIT 1;")
        r.show()
        out = capsys.readouterr().out
        assert "(1 row)" in out


# ── sql property ──────────────────────────────────────────────────────────────


class TestSqlProperty:
    def test_sql_contains_select(self, small_result):
        assert "SELECT" in small_result.sql.upper()

    def test_sql_contains_from(self, small_result):
        assert "FROM" in small_result.sql.upper()


# ── diagnostics ───────────────────────────────────────────────────────────────


class TestDiagnostics:
    def test_diagnostics_empty_for_clean_query(self, small_result):
        assert small_result.diagnostics == []

    def test_diagnostics_is_list(self, small_result):
        assert isinstance(small_result.diagnostics, list)


# ── to_arrow() and to_polars() ────────────────────────────────────────────────


class TestOptionalOutputFormats:
    def test_to_arrow_raises_without_pyarrow(self, small_result):
        import sys
        import importlib

        # Patch pyarrow out of sys.modules if present
        pyarrow_mod = sys.modules.get("pyarrow")
        if pyarrow_mod is not None:
            # pyarrow IS installed — just test it works
            tbl = small_result.to_arrow()
            assert tbl.num_rows == 5
            assert "invoice_id" in tbl.schema.names
        else:
            with pytest.raises(ImportError, match="pyarrow"):
                small_result.to_arrow()

    def test_to_polars_raises_without_polars(self, small_result):
        import sys

        polars_mod = sys.modules.get("polars")
        if polars_mod is not None:
            pl_df = small_result.to_polars()
            assert len(pl_df) == 5
        else:
            with pytest.raises(ImportError, match="polars"):
                small_result.to_polars()


# ── repr / str ────────────────────────────────────────────────────────────────


class TestResultRepr:
    def test_repr_contains_rows(self, small_result):
        r = repr(small_result)
        assert "rows=5" in r
        assert "Result" in r

    def test_str_is_dataframe_string(self, small_result):
        s = str(small_result)
        assert "invoice_id" in s
        assert "amount" in s
