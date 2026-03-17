"""Tests for the rich diagnostic formatter."""
from contextql.diagnostics import Diagnostic, Span, Annotation, format_diagnostic, format_simple


class TestFormatDiagnostic:
    def test_basic_error(self):
        diag = Diagnostic(
            code="E100",
            severity="error",
            message="Context 'nonexistent' is not defined.",
        )
        out = format_diagnostic(diag)
        assert "error[E100]" in out
        assert "nonexistent" in out

    def test_with_location(self):
        diag = Diagnostic(
            code="E102",
            severity="error",
            message="Entity key type mismatch",
            source="query.cql",
            span=Span(line=4, column=7),
        )
        out = format_diagnostic(diag)
        assert "--> query.cql:4:7" in out

    def test_with_annotations(self):
        source = "SELECT *\nFROM invoices\nWHERE CONTEXT IN (bad_ctx)"
        diag = Diagnostic(
            code="E100",
            severity="error",
            message="Context 'bad_ctx' is not defined.",
            span=Span(line=3, column=19, length=7),
            annotations=[
                Annotation(Span(line=3, column=19, length=7), "undefined context", is_primary=True),
            ],
        )
        out = format_diagnostic(diag, source)
        assert "^^^^^^^" in out
        assert "undefined context" in out

    def test_with_help_and_note(self):
        diag = Diagnostic(
            code="E107",
            severity="error",
            message="ORDER BY CONTEXT requires WHERE CONTEXT IN",
            help="Add WHERE CONTEXT IN (...) before ORDER BY CONTEXT",
            note="See whitepaper Section 9",
        )
        out = format_diagnostic(diag)
        assert "= help:" in out
        assert "= note:" in out

    def test_secondary_annotation(self):
        source = "SELECT *\nFROM invoices i\nWHERE CONTEXT IN (supplier_risk)"
        diag = Diagnostic(
            code="E102",
            severity="error",
            message="Entity key type mismatch",
            span=Span(line=3, column=19, length=13),
            annotations=[
                Annotation(Span(line=2, column=6, length=8), "table 'invoices' has key 'invoice_id'", is_primary=False),
                Annotation(Span(line=3, column=19, length=13), "context key mismatch", is_primary=True),
            ],
        )
        out = format_diagnostic(diag, source)
        assert "--------" in out  # secondary annotation
        assert "^^^^^^^^^^^^^" in out  # primary annotation


class TestFormatSimple:
    def test_one_line_format(self):
        diag = Diagnostic(
            code="E100",
            severity="error",
            message="Context 'x' is not defined.",
            source="test.cql",
            span=Span(line=5, column=10),
        )
        out = format_simple(diag)
        assert out == "test.cql:5:10: error[E100]: Context 'x' is not defined."

    def test_no_location(self):
        diag = Diagnostic(
            code="W001",
            severity="warning",
            message="Window without scores",
        )
        out = format_simple(diag)
        assert out == "query: warning[W001]: Window without scores"
