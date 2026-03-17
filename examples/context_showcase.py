#!/usr/bin/env python3
"""ContextQL Language Showcase

Demonstrates what makes ContextQL different from plain SQL:
  - Named, reusable contexts with scoring
  - Context algebra (union, THEN chains, weighted combination)
  - Temporal qualifiers
  - CONTEXT_SCORE() and ORDER BY CONTEXT
  - Real-time linting that catches semantic errors
  - Rich Rust/Elm-style diagnostic output
"""
from contextql.parser import ContextQLParser
from contextql.linter import (
    Catalog, CatalogContext, CatalogTable, CatalogEventLog, ContextQLLinter,
)
from contextql.diagnostics import Diagnostic, Span, Annotation, format_diagnostic

# ── Setup ─────────────────────────────────────────────────────────────

parser = ContextQLParser()

catalog = Catalog()
catalog.add_table(CatalogTable(
    "invoices", "invoice_id", "INT64",
    columns={"invoice_id": "INT64", "amount": "DOUBLE", "vendor_id": "INT64",
             "due_date": "DATE", "status": "VARCHAR"},
))
catalog.add_table(CatalogTable(
    "vendors", "vendor_id", "INT64",
    columns={"vendor_id": "INT64", "name": "VARCHAR", "country": "VARCHAR"},
))
catalog.add_context(CatalogContext("late_invoice", "invoice_id", "INT64", has_score=True))
catalog.add_context(CatalogContext("high_value", "invoice_id", "INT64", has_score=True))
catalog.add_context(CatalogContext("supplier_risk", "vendor_id", "INT64", has_score=True))
catalog.add_context(CatalogContext("seasonal_pattern", "invoice_id", "INT64",
                                   has_score=True, is_temporal=True))

linter = ContextQLLinter(catalog)


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def parse_and_show(label: str, query: str) -> None:
    """Parse a query and show it succeeded."""
    tree = parser.parse(query)
    print(f"  {label}")
    print(f"  {query.strip()[:70]}{'...' if len(query.strip()) > 70 else ''}")
    print(f"  -> Parsed OK ({tree.data})\n")


def lint_and_show(query: str) -> None:
    """Lint a query and display any diagnostics."""
    diags = linter.lint(query)
    if not diags:
        print("  No diagnostics (clean query)\n")
    for d in diags:
        severity = d.severity.upper()
        print(f"  {severity} {d.rule_id}: {d.message}")
        if d.suggestion:
            print(f"    -> {d.suggestion}")
    print()


# ── 1. Context Definition ─────────────────────────────────────────────

section("1. Defining Contexts")

parse_and_show("Basic scored context:",
    "CREATE CONTEXT late_invoice ON invoice_id "
    "SCORE 1.0 - (days_overdue / 90.0) "
    "AS SELECT invoice_id, days_overdue FROM invoices WHERE status = 'overdue';")

parse_and_show("Parameterized context:",
    "CREATE CONTEXT high_value(threshold DOUBLE DEFAULT 10000.0) ON invoice_id "
    "SCORE amount / 100000.0 "
    "DESCRIPTION 'Invoices exceeding a value threshold' "
    "TAGS ('finance', 'risk') "
    "AS SELECT invoice_id, amount FROM invoices WHERE amount > threshold;")

parse_and_show("Temporal context:",
    "CREATE CONTEXT seasonal_pattern ON invoice_id "
    "SCORE trend_score "
    "TEMPORAL (created_date, MONTH) "
    "AS SELECT invoice_id, created_date, trend_score FROM invoice_trends;")

parse_and_show("Composite context (weighted strategy):",
    "CREATE CONTEXT invoice_risk ON invoice_id AS "
    "COMPOSE (late_invoice WEIGHT 0.6, high_value WEIGHT 0.4) WITH STRATEGY WEIGHTED;")


# ── 2. Context Queries ────────────────────────────────────────────────

section("2. Querying with Contexts")

parse_and_show("Union of contexts with ranking:",
    "SELECT invoice_id, amount, CONTEXT_SCORE() AS risk "
    "FROM invoices "
    "WHERE CONTEXT IN (late_invoice, high_value WEIGHT 0.8) "
    "ORDER BY CONTEXT DESC LIMIT 20;")

parse_and_show("Intersection mode (ALL):",
    "SELECT invoice_id FROM invoices "
    "WHERE CONTEXT IN ALL (late_invoice, high_value);")

parse_and_show("THEN chain (sequential pattern):",
    "SELECT invoice_id FROM invoices "
    "WHERE CONTEXT IN (late_invoice THEN high_value);")

parse_and_show("Temporal query (point-in-time):",
    "SELECT invoice_id, CONTEXT_SCORE() FROM invoices "
    "WHERE CONTEXT IN (seasonal_pattern AT '2024-06-01');")

parse_and_show("Multi-table with explicit binding:",
    "SELECT i.invoice_id, v.name, CONTEXT_SCORE() "
    "FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id "
    "WHERE CONTEXT ON i IN (late_invoice) "
    "ORDER BY CONTEXT DESC;")

parse_and_show("Context window (top-k):",
    "WITH CONTEXT WINDOW 50 "
    "SELECT * FROM invoices "
    "WHERE CONTEXT IN (late_invoice, high_value) "
    "ORDER BY CONTEXT USING WEIGHTED_SUM DESC;")


# ── 3. Linter Catching Real Errors ───────────────────────────────────

section("3. Linter: Catching Errors at Authoring Time")

print("  E100 — Undefined context:")
lint_and_show("SELECT * FROM invoices WHERE CONTEXT IN (nonexistent_ctx);")

print("  E102 — Entity key type mismatch:")
catalog.add_context(CatalogContext("text_ctx", "code", "VARCHAR", has_score=True))
lint_and_show("SELECT * FROM invoices WHERE CONTEXT IN (text_ctx);")

print("  E107 — ORDER BY CONTEXT without WHERE CONTEXT IN:")
lint_and_show("SELECT * FROM invoices ORDER BY CONTEXT DESC;")

print("  E108 — CONTEXT_SCORE() outside context query:")
lint_and_show("SELECT CONTEXT_SCORE() FROM invoices;")

print("  E109 — Temporal qualifier on non-temporal context:")
lint_and_show(
    "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice AT '2024-01-01');")

print("  E110 — Negative weight:")
lint_and_show(
    "SELECT * FROM invoices WHERE CONTEXT IN (late_invoice WEIGHT -0.5);")

print("  W001 — Context window without scores:")
catalog.add_context(CatalogContext("unscored", "invoice_id", "INT64", has_score=False))
lint_and_show(
    "WITH CONTEXT WINDOW 100 SELECT * FROM invoices "
    "WHERE CONTEXT IN (unscored);")

print("  W002 — Joined query missing CONTEXT ON:")
lint_and_show(
    "SELECT * FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id "
    "WHERE CONTEXT IN (late_invoice);")


# ── 4. Rich Diagnostics ──────────────────────────────────────────────

section("4. Rich Diagnostic Output (Rust/Elm-style)")

source = """SELECT i.invoice_id, v.name
FROM invoices i
JOIN vendors v ON i.vendor_id = v.vendor_id
WHERE CONTEXT IN (late_invoice, nonexistent_ctx)
ORDER BY CONTEXT DESC"""

diag = Diagnostic(
    code="E100",
    severity="error",
    message="Context 'nonexistent_ctx' is not defined.",
    source="example.cql",
    span=Span(line=4, column=35, length=15),
    annotations=[
        Annotation(Span(line=4, column=35, length=15), "undefined context", is_primary=True),
        Annotation(Span(line=4, column=19, length=12), "this context is defined", is_primary=False),
    ],
    help="Did you mean 'high_value' or 'supplier_risk'?",
    note="Use SHOW CONTEXTS to list available contexts.",
)

print(format_diagnostic(diag, source))


# ── 5. Clean Query ───────────────────────────────────────────────────

section("5. A Clean Query (Zero Diagnostics)")

clean = (
    "SELECT i.invoice_id, v.name, CONTEXT_SCORE() AS risk_score "
    "FROM invoices i "
    "JOIN vendors v ON i.vendor_id = v.vendor_id "
    "WHERE CONTEXT ON i IN (late_invoice, high_value WEIGHT 0.8) "
    "ORDER BY CONTEXT DESC LIMIT 20;"
)
print(f"  {clean}\n")
lint_and_show(clean)

print("Done.")
