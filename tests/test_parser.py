"""Parser tests: valid queries and syntax error detection."""
import pytest
from contextql.parser import ContextQLParser, ContextQLSyntaxError


class TestParseValid:
    """Queries that must parse without error."""

    def test_simple_select(self, parser):
        tree = parser.parse("SELECT * FROM invoices;")
        assert tree.data == "start"

    def test_select_columns(self, parser):
        parser.parse("SELECT invoice_id, amount FROM invoices;")

    def test_select_with_alias(self, parser):
        parser.parse("SELECT i.invoice_id AS id FROM invoices i;")

    def test_where_clause(self, parser):
        parser.parse("SELECT * FROM invoices WHERE amount > 100;")

    def test_order_by(self, parser):
        parser.parse("SELECT * FROM invoices ORDER BY amount DESC;")

    def test_limit_offset(self, parser):
        parser.parse("SELECT * FROM invoices LIMIT 10 OFFSET 5;")

    def test_group_by_having(self, parser):
        parser.parse("SELECT status, COUNT(*) FROM invoices GROUP BY status HAVING COUNT(*) > 1;")

    def test_join(self, parser):
        parser.parse("SELECT * FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id;")

    def test_left_join(self, parser):
        parser.parse("SELECT * FROM invoices i LEFT JOIN vendors v ON i.vendor_id = v.vendor_id;")

    def test_cross_join(self, parser):
        parser.parse("SELECT * FROM invoices CROSS JOIN vendors;")

    def test_subquery_in_from(self, parser):
        parser.parse("SELECT * FROM (SELECT * FROM invoices) sub;")

    def test_expression_arithmetic(self, parser):
        parser.parse("SELECT amount * 2 + 1 FROM invoices;")

    def test_case_expression(self, parser):
        parser.parse("SELECT CASE WHEN amount > 100 THEN 'high' ELSE 'low' END FROM invoices;")

    def test_cast_expression(self, parser):
        parser.parse("SELECT CAST(amount AS INTEGER) FROM invoices;")

    def test_is_null(self, parser):
        parser.parse("SELECT * FROM invoices WHERE paid_date IS NULL;")

    def test_is_not_null(self, parser):
        parser.parse("SELECT * FROM invoices WHERE paid_date IS NOT NULL;")

    def test_between(self, parser):
        parser.parse("SELECT * FROM invoices WHERE amount BETWEEN 100 AND 500;")

    def test_like(self, parser):
        parser.parse("SELECT * FROM invoices WHERE status LIKE 'open%';")

    def test_in_list(self, parser):
        parser.parse("SELECT * FROM invoices WHERE status IN ('open', 'pending');")

    def test_exists(self, parser):
        parser.parse("SELECT * FROM invoices WHERE EXISTS (SELECT * FROM vendors);")

    def test_multiple_statements(self, parser):
        parser.parse("SELECT * FROM a; SELECT * FROM b;")

    def test_comments_ignored(self, parser):
        parser.parse("-- this is a comment\nSELECT * FROM invoices;")

    def test_block_comment(self, parser):
        parser.parse("/* block comment */ SELECT * FROM invoices;")


class TestParseContextQL:
    """ContextQL-specific syntax."""

    def test_context_in(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")

    def test_context_in_multiple(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice, high_value);")

    def test_context_not_in(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT NOT IN (late_invoice);")

    def test_context_in_all(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN ALL (late_invoice, high_value);")

    def test_context_on(self, parser):
        parser.parse("SELECT * FROM invoices i WHERE CONTEXT ON i IN (late_invoice);")

    def test_context_on_both(self, parser):
        parser.parse(
            "SELECT * FROM invoices i JOIN vendors v ON i.vendor_id = v.vendor_id "
            "WHERE CONTEXT ON i IN (late_invoice) AND CONTEXT ON v IN (supplier_risk);"
        )

    def test_weight(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (a WEIGHT 0.7, b WEIGHT 0.3);")

    def test_then_chain(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (a THEN b);")

    def test_then_chain_triple(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (a THEN b THEN c);")

    def test_temporal_at(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (ctx AT '2024-01-01');")

    def test_temporal_between(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (ctx BETWEEN '2024-01-01' AND '2024-12-31');")

    def test_mcp_context(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (MCP(fraud_engine));")

    def test_order_by_context(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (a) ORDER BY CONTEXT DESC;")

    def test_order_by_context_using(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (a) ORDER BY CONTEXT USING MAX DESC;")

    def test_context_window(self, parser):
        parser.parse("WITH CONTEXT WINDOW 1000 SELECT * FROM invoices WHERE CONTEXT IN (a);")

    def test_context_score_function(self, parser):
        parser.parse("SELECT CONTEXT_SCORE() FROM invoices WHERE CONTEXT IN (a);")

    def test_context_count_function(self, parser):
        parser.parse("SELECT CONTEXT_COUNT() FROM invoices WHERE CONTEXT IN (a);")

    def test_context_mixed_predicates(self, parser):
        parser.parse(
            "SELECT * FROM invoices "
            "WHERE CONTEXT IN (late_invoice) AND amount > 100 "
            "ORDER BY CONTEXT DESC LIMIT 50;"
        )

    def test_parameterized_context(self, parser):
        parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (ctx(threshold := 100));")


class TestParseDDL:
    """DDL statement parsing."""

    def test_create_context_basic(self, parser):
        parser.parse("CREATE CONTEXT late_invoice ON invoice_id AS SELECT invoice_id FROM invoices WHERE due_date < CURRENT_DATE;")

    def test_create_context_scored(self, parser):
        parser.parse("CREATE CONTEXT high_value ON invoice_id SCORE amount AS SELECT invoice_id, amount FROM invoices WHERE amount > 100;")

    def test_create_or_replace(self, parser):
        parser.parse("CREATE OR REPLACE CONTEXT test ON id AS SELECT id FROM t;")

    def test_create_context_description_tags(self, parser):
        parser.parse("CREATE CONTEXT test ON id DESCRIPTION 'A test context' TAGS ('finance', 'risk') AS SELECT id FROM t;")

    def test_alter_context_rename(self, parser):
        parser.parse("ALTER CONTEXT old_name RENAME TO new_name;")

    def test_alter_context_set_score(self, parser):
        parser.parse("ALTER CONTEXT test SET SCORE amount * 0.5;")

    def test_drop_context(self, parser):
        parser.parse("DROP CONTEXT late_invoice;")

    def test_drop_context_if_exists_cascade(self, parser):
        parser.parse("DROP CONTEXT IF EXISTS late_invoice CASCADE;")

    def test_show_contexts(self, parser):
        parser.parse("SHOW CONTEXTS;")

    def test_show_contexts_like(self, parser):
        parser.parse("SHOW CONTEXTS LIKE 'late%';")

    def test_describe_context(self, parser):
        parser.parse("DESCRIBE CONTEXT late_invoice;")

    def test_refresh_context(self, parser):
        parser.parse("REFRESH CONTEXT late_invoice;")

    def test_refresh_all(self, parser):
        parser.parse("REFRESH ALL CONTEXTS;")

    def test_validate_context(self, parser):
        parser.parse("VALIDATE CONTEXT late_invoice;")

    def test_create_event_log(self, parser):
        parser.parse(
            "CREATE EVENT LOG order_log FROM orders ON order_id "
            "ACTIVITY status TIMESTAMP event_time;"
        )

    def test_create_event_log_with_resource(self, parser):
        parser.parse(
            "CREATE EVENT LOG order_log FROM orders ON order_id "
            "ACTIVITY status TIMESTAMP event_time RESOURCE handler;"
        )

    def test_create_process_model(self, parser):
        parser.parse(
            "CREATE PROCESS MODEL happy_path "
            "EXPECTED PATH ('Create', 'Approve', 'Ship');"
        )

    def test_show_event_logs(self, parser):
        parser.parse("SHOW EVENT LOGS;")


class TestParseSyntaxErrors:
    """Queries that must produce syntax errors."""

    def test_missing_from(self, parser):
        with pytest.raises(ContextQLSyntaxError) as exc_info:
            parser.parse("SELECT * WHERE x > 1;")
        assert exc_info.value.detail.code == "E001"

    def test_unclosed_paren(self, parser):
        with pytest.raises(ContextQLSyntaxError):
            parser.parse("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice;")

    def test_empty_input(self, parser):
        with pytest.raises(ContextQLSyntaxError):
            parser.parse("")

    def test_gibberish(self, parser):
        with pytest.raises(ContextQLSyntaxError):
            parser.parse("NOT A VALID QUERY AT ALL;")

    def test_error_has_line_info(self, parser):
        with pytest.raises(ContextQLSyntaxError) as exc_info:
            parser.parse("SELECT * FROM;")
        detail = exc_info.value.detail
        assert detail.line >= 1
        assert detail.column >= 1
