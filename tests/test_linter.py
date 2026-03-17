"""Linter tests: each semantic rule verified independently."""
import pytest
from contextql.linter import (
    Catalog, CatalogContext, CatalogTable, CatalogEventLog,
    ContextQLLinter, LintDiagnostic,
)


def _codes(diags: list[LintDiagnostic]) -> list[str]:
    return [d.rule_id for d in diags]


class TestE100UndefinedContext:
    def test_undefined_context(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (nonexistent);")
        assert "E100" in _codes(diags)

    def test_defined_context_no_error(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        assert "E100" not in _codes(diags)

    def test_did_you_mean(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoce);")
        assert "E100" in _codes(diags)
        assert any("late_invoice" in (d.suggestion or "") for d in diags if d.rule_id == "E100")


class TestE102EntityKeyMismatch:
    def test_type_mismatch(self, linter):
        # supplier_risk is keyed on vendor_id (INT64) but the table invoices has INT64 too
        # Need a VARCHAR context to trigger mismatch
        linter.catalog.add_context(
            CatalogContext("text_ctx", "code", "VARCHAR")
        )
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (text_ctx);")
        assert "E102" in _codes(diags)

    def test_compatible_types_no_error(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        assert "E102" not in _codes(diags)


class TestE103CircularDependency:
    def test_self_reference(self, linter):
        diags = linter.lint(
            "CREATE CONTEXT self_ref ON id AS "
            "SELECT id FROM t WHERE CONTEXT IN (self_ref);"
        )
        assert "E103" in _codes(diags)

    def test_transitive_cycle(self, linter):
        # dep_child depends on dep_parent (in catalog), so creating dep_parent depending on dep_child is a cycle
        diags = linter.lint(
            "CREATE CONTEXT dep_parent ON id AS "
            "SELECT id FROM t WHERE CONTEXT IN (dep_child);"
        )
        assert "E103" in _codes(diags)


class TestE107OrderByContext:
    def test_order_by_without_where(self, linter):
        diags = linter.lint("SELECT * FROM invoices ORDER BY CONTEXT DESC;")
        assert "E107" in _codes(diags)

    def test_order_by_with_where(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice) ORDER BY CONTEXT DESC;"
        )
        assert "E107" not in _codes(diags)


class TestE108ContextScoreScope:
    def test_score_without_context(self, linter):
        diags = linter.lint("SELECT CONTEXT_SCORE() FROM invoices;")
        assert "E108" in _codes(diags)

    def test_score_with_context(self, linter):
        diags = linter.lint(
            "SELECT CONTEXT_SCORE() FROM invoices WHERE CONTEXT IN (late_invoice);"
        )
        assert "E108" not in _codes(diags)


class TestE109TemporalOnNonTemporal:
    def test_temporal_on_non_temporal(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice AT '2024-01-01');"
        )
        assert "E109" in _codes(diags)

    def test_temporal_on_temporal_ok(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (temporal_ctx AT '2024-01-01');"
        )
        assert "E109" not in _codes(diags)


class TestE110NegativeWeight:
    def test_negative_weight(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice WEIGHT -1.0);"
        )
        assert "E110" in _codes(diags)

    def test_positive_weight_ok(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice WEIGHT 0.5);"
        )
        assert "E110" not in _codes(diags)


class TestE118OrderByInContextDef:
    def test_order_by_in_definition(self, linter):
        diags = linter.lint(
            "CREATE CONTEXT bad ON id AS SELECT id FROM t ORDER BY id;"
        )
        assert "E118" in _codes(diags)

    def test_no_order_by_ok(self, linter):
        diags = linter.lint(
            "CREATE CONTEXT good ON id AS SELECT id FROM t WHERE x > 1;"
        )
        assert "E118" not in _codes(diags)


class TestW001WindowWithoutScore:
    def test_window_unscored(self, linter):
        diags = linter.lint(
            "WITH CONTEXT WINDOW 100 SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);"
        )
        assert "W001" in _codes(diags)

    def test_window_scored_ok(self, linter):
        diags = linter.lint(
            "WITH CONTEXT WINDOW 100 SELECT * FROM invoices WHERE CONTEXT IN (high_value);"
        )
        assert "W001" not in _codes(diags)


class TestW002MissingContextOn:
    def test_join_without_context_on(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT IN (late_invoice);"
        )
        assert "W002" in _codes(diags)

    def test_join_with_context_on_ok(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON i IN (late_invoice);"
        )
        assert "W002" not in _codes(diags)

    def test_no_join_no_warning(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        assert "W002" not in _codes(diags)


class TestW004WeightZero:
    def test_zero_weight(self, linter):
        diags = linter.lint(
            "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice WEIGHT 0.0);"
        )
        assert "W004" in _codes(diags)


class TestSyntaxErrors:
    def test_syntax_error_produces_diagnostic(self, linter):
        diags = linter.lint("SELECT * FROM;")
        assert len(diags) >= 1
        assert diags[0].rule_id == "E001"
        assert diags[0].severity == "error"

    def test_clean_query(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        assert len(diags) == 0
