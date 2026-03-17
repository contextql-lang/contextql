"""Tests for the ContextQL LSP server module.

Tests the conversion layer and server helpers without spawning a real LSP client.
"""
import pytest
from lsprotocol import types

from contextql.linter import LintDiagnostic, Catalog, CatalogContext, CatalogTable, ContextQLLinter
from contextql.lsp.server import (
    lint_to_lsp_diagnostics,
    _SEVERITY_MAP,
    _KEYWORD_COMPLETIONS,
    _HOVER_DOCS,
    _CREATE_PATTERN,
)


# ── lint_to_lsp_diagnostics conversion ───────────────────────────────


class TestLintToLspDiagnostics:
    def test_empty_input(self):
        assert lint_to_lsp_diagnostics([]) == []

    def test_single_error(self):
        diag = LintDiagnostic(
            rule_id="E100",
            severity="error",
            message="Context 'bad' is not defined.",
            line=3,
            column=10,
        )
        result = lint_to_lsp_diagnostics([diag])
        assert len(result) == 1
        d = result[0]
        assert d.code == "E100"
        assert d.source == "contextql"
        assert d.severity == types.DiagnosticSeverity.Error
        # 1-indexed → 0-indexed
        assert d.range.start.line == 2
        assert d.range.start.character == 9

    def test_warning_severity(self):
        diag = LintDiagnostic(rule_id="W001", severity="warning", message="test")
        result = lint_to_lsp_diagnostics([diag])
        assert result[0].severity == types.DiagnosticSeverity.Warning

    def test_info_severity(self):
        diag = LintDiagnostic(rule_id="W002", severity="info", message="test")
        result = lint_to_lsp_diagnostics([diag])
        assert result[0].severity == types.DiagnosticSeverity.Information

    def test_suggestion_appended(self):
        diag = LintDiagnostic(
            rule_id="E100",
            severity="error",
            message="Context 'bad' is not defined.",
            suggestion="Did you mean 'good'?",
        )
        result = lint_to_lsp_diagnostics([diag])
        assert "Did you mean 'good'?" in result[0].message

    def test_no_suggestion(self):
        diag = LintDiagnostic(rule_id="E107", severity="error", message="ORDER BY CONTEXT requires WHERE CONTEXT IN.")
        result = lint_to_lsp_diagnostics([diag])
        assert "Suggestion" not in result[0].message

    def test_line_column_floor_at_zero(self):
        """Line/column values of 1 should map to 0, not go negative."""
        diag = LintDiagnostic(rule_id="E001", severity="error", message="err", line=1, column=1)
        result = lint_to_lsp_diagnostics([diag])
        assert result[0].range.start.line == 0
        assert result[0].range.start.character == 0

    def test_multiple_diagnostics(self):
        diags = [
            LintDiagnostic(rule_id="E100", severity="error", message="first", line=1, column=1),
            LintDiagnostic(rule_id="W001", severity="warning", message="second", line=5, column=3),
        ]
        result = lint_to_lsp_diagnostics(diags)
        assert len(result) == 2
        assert result[0].code == "E100"
        assert result[1].code == "W001"
        assert result[1].range.start.line == 4


# ── Real linter → LSP pipeline ───────────────────────────────────────


class TestLinterIntegration:
    @pytest.fixture
    def linter(self):
        catalog = Catalog()
        catalog.add_table(CatalogTable("invoices", "invoice_id", "INT64"))
        catalog.add_context(CatalogContext("late_invoice", "invoice_id", "INT64"))
        return ContextQLLinter(catalog)

    def test_valid_query_no_diagnostics(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        result = lint_to_lsp_diagnostics(diags)
        assert result == []

    def test_undefined_context_produces_lsp_error(self, linter):
        diags = linter.lint("SELECT * FROM invoices WHERE CONTEXT IN (nonexistent);")
        result = lint_to_lsp_diagnostics(diags)
        assert len(result) >= 1
        assert result[0].code == "E100"
        assert result[0].severity == types.DiagnosticSeverity.Error

    def test_syntax_error_produces_lsp_error(self, linter):
        diags = linter.lint("SELECT * FROM;")
        result = lint_to_lsp_diagnostics(diags)
        assert len(result) >= 1
        assert result[0].code == "E001"


# ── Severity map coverage ────────────────────────────────────────────


class TestSeverityMap:
    def test_all_severities_mapped(self):
        assert "error" in _SEVERITY_MAP
        assert "warning" in _SEVERITY_MAP
        assert "info" in _SEVERITY_MAP


# ── Completions ──────────────────────────────────────────────────────


class TestCompletions:
    def test_keyword_completions_not_empty(self):
        assert len(_KEYWORD_COMPLETIONS) > 0

    def test_core_keywords_present(self):
        labels = {item.label for item in _KEYWORD_COMPLETIONS}
        assert "SELECT" in labels
        assert "CONTEXT IN" in labels
        assert "CONTEXT_SCORE()" in labels
        assert "CREATE CONTEXT" in labels

    def test_snippet_format(self):
        context_in = next(i for i in _KEYWORD_COMPLETIONS if i.label == "CONTEXT IN")
        assert context_in.insert_text_format == types.InsertTextFormat.Snippet
        assert "${1:" in context_in.insert_text


# ── Hover documentation ──────────────────────────────────────────────


class TestHoverDocs:
    def test_context_documented(self):
        assert "CONTEXT" in _HOVER_DOCS
        assert "first-class" in _HOVER_DOCS["CONTEXT"].lower()

    def test_context_score_documented(self):
        assert "CONTEXT_SCORE" in _HOVER_DOCS
        assert "score" in _HOVER_DOCS["CONTEXT_SCORE"].lower()

    def test_all_docs_are_markdown(self):
        for key, val in _HOVER_DOCS.items():
            assert "**" in val, f"Hover doc for {key} should use markdown bold"


# ── Document symbols regex ───────────────────────────────────────────


class TestDocumentSymbols:
    def test_create_context_match(self):
        text = "CREATE CONTEXT late_invoice ON invoice_id AS SELECT id FROM t;"
        matches = list(_CREATE_PATTERN.finditer(text))
        assert len(matches) == 1
        assert matches[0].group(1).upper() == "CONTEXT"
        assert matches[0].group(2) == "late_invoice"

    def test_create_event_log_match(self):
        text = "CREATE EVENT LOG order_log FROM orders ON order_id ACTIVITY act TIMESTAMP ts;"
        matches = list(_CREATE_PATTERN.finditer(text))
        assert len(matches) == 1
        assert "EVENT" in matches[0].group(1).upper()
        assert matches[0].group(2) == "order_log"

    def test_create_process_model_match(self):
        text = "CREATE PROCESS MODEL my_model EXPECTED PATH ('a', 'b');"
        matches = list(_CREATE_PATTERN.finditer(text))
        assert len(matches) == 1
        assert "PROCESS" in matches[0].group(1).upper()
        assert matches[0].group(2) == "my_model"

    def test_multiple_statements(self):
        text = (
            "CREATE CONTEXT ctx1 ON id AS SELECT id FROM t;\n"
            "CREATE CONTEXT ctx2 ON id AS SELECT id FROM t;\n"
        )
        matches = list(_CREATE_PATTERN.finditer(text))
        assert len(matches) == 2

    def test_no_match_on_select(self):
        text = "SELECT * FROM invoices;"
        matches = list(_CREATE_PATTERN.finditer(text))
        assert len(matches) == 0
