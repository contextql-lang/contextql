"""Tests for contextql.semantic — SemanticLowerer, SemanticAnalyzer, catalog types."""
from __future__ import annotations

import pytest

from contextql.semantic import (
    AnalysisResult,
    ContextCatalogEntry,
    ContextDefinitionModel,
    ContextPredicate,
    ContextReference,
    EntityKeyType,
    EventLogCatalogEntry,
    EventLogDefinitionModel,
    InMemoryCatalog,
    JoinRef,
    OrderItem,
    ProcessModelCatalogEntry,
    ProcessModelDefinitionModel,
    QueryModel,
    SemanticAnalyzer,
    SemanticLowerer,
    SemanticStatement,
    StatementKind,
    TableCatalogEntry,
    TableRef,
    analyze_sql,
)
from contextql.parser import ContextQLParser


# ── helpers ────────────────────────────────────────────────────────────────


def lower(sql: str):
    parser = ContextQLParser()
    tree = parser.parse(sql)
    return SemanticLowerer().lower(tree)


def lower_first(sql: str):
    return lower(sql)[0]


# ── EntityKeyType enum ──────────────────────────────────────────────────────


class TestEntityKeyType:
    def test_values(self):
        assert EntityKeyType.INT64.value == "INT64"
        assert EntityKeyType.VARCHAR.value == "VARCHAR"
        assert EntityKeyType.UUID.value == "UUID"
        assert EntityKeyType.COMPOSITE.value == "COMPOSITE"
        assert EntityKeyType.UNKNOWN.value == "UNKNOWN"

    def test_str_enum(self):
        assert EntityKeyType.INT64 == "INT64"

    def test_default_in_catalog_entry(self):
        entry = ContextCatalogEntry(name="x", entity_key_name="id")
        assert entry.entity_key_type == EntityKeyType.UNKNOWN

    def test_explicit_in_catalog_entry(self):
        entry = ContextCatalogEntry(name="x", entity_key_name="id", entity_key_type=EntityKeyType.INT64)
        assert entry.entity_key_type == EntityKeyType.INT64


# ── InMemoryCatalog ─────────────────────────────────────────────────────────


class TestInMemoryCatalog:
    def _catalog(self):
        return InMemoryCatalog(
            contexts={
                "late_invoice": ContextCatalogEntry("late_invoice", "invoice_id", EntityKeyType.INT64),
                "high_value": ContextCatalogEntry("high_value", "invoice_id", EntityKeyType.INT64, has_score=True),
            },
            event_logs={
                "ap_log": EventLogCatalogEntry("ap_log", "case_id", "VARCHAR"),
            },
            process_models={
                "ap_model": ProcessModelCatalogEntry("ap_model", "ap_log"),
            },
            tables={
                "invoices": TableCatalogEntry("invoices", alias="i", primary_key_name="invoice_id"),
            },
        )

    def test_get_context_found(self):
        cat = self._catalog()
        entry = cat.get_context("late_invoice")
        assert entry is not None
        assert entry.entity_key_name == "invoice_id"

    def test_get_context_case_insensitive(self):
        cat = self._catalog()
        assert cat.get_context("LATE_INVOICE") is not None

    def test_get_context_missing(self):
        cat = self._catalog()
        assert cat.get_context("unknown") is None

    def test_list_contexts(self):
        cat = self._catalog()
        assert len(cat.list_contexts()) == 2

    def test_get_event_log(self):
        cat = self._catalog()
        assert cat.get_event_log("ap_log") is not None

    def test_list_event_logs(self):
        cat = self._catalog()
        assert len(cat.list_event_logs()) == 1

    def test_get_process_model(self):
        cat = self._catalog()
        assert cat.get_process_model("ap_model") is not None

    def test_get_table_by_name(self):
        cat = self._catalog()
        t = cat.get_table("invoices")
        assert t is not None
        assert t.primary_key_name == "invoice_id"

    def test_get_table_by_alias(self):
        cat = self._catalog()
        t = cat.get_table("i")
        assert t is not None
        assert t.name == "invoices"

    def test_get_table_missing(self):
        cat = self._catalog()
        assert cat.get_table("vendors") is None


# ── SemanticLowerer — SELECT ────────────────────────────────────────────────


class TestLowerSelect:
    def test_simple_projections(self):
        q = lower_first("SELECT invoice_id, amount FROM invoices;")
        assert isinstance(q, QueryModel)
        assert q.kind == StatementKind.SELECT
        assert "invoice_id" in " ".join(q.projections)
        assert "amount" in " ".join(q.projections)

    def test_star_projection(self):
        q = lower_first("SELECT * FROM invoices;")
        assert isinstance(q, QueryModel)
        assert q.projections == ["*"]

    def test_from_table_name(self):
        q = lower_first("SELECT * FROM invoices;")
        assert q.from_table is not None
        assert q.from_table.name == "invoices"
        assert q.from_table.alias is None

    def test_from_table_alias(self):
        q = lower_first("SELECT * FROM invoices AS i;")
        assert q.from_table.alias == "i"

    def test_join_with_condition(self):
        q = lower_first("SELECT * FROM invoices AS i JOIN vendors AS v ON i.vendor_id = v.vendor_id WHERE CONTEXT IN (ctx);")
        assert len(q.joins) == 1
        j = q.joins[0]
        assert isinstance(j, JoinRef)
        assert j.join_type == "JOIN"
        assert j.table.name == "vendors"
        assert j.table.alias == "v"
        assert j.condition is not None
        assert "vendor_id" in j.condition

    def test_where_text_populated(self):
        q = lower_first("SELECT * FROM invoices WHERE status = 'open';")
        assert q.where_text is not None
        assert "status" in q.where_text

    def test_where_text_with_context(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT IN (ctx);")
        assert q.where_text is not None
        assert "CONTEXT" in q.where_text.upper()

    def test_limit(self):
        q = lower_first("SELECT * FROM invoices LIMIT 10;")
        assert q.limit == 10

    def test_offset(self):
        q = lower_first("SELECT * FROM invoices LIMIT 5 OFFSET 20;")
        assert q.offset == 20

    def test_order_by_context(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT IN (ctx) ORDER BY CONTEXT DESC;")
        assert len(q.order_items) == 1
        item = q.order_items[0]
        assert item.is_context_order is True
        assert item.direction == "DESC"

    def test_context_score_projection_canonical(self):
        q = lower_first("SELECT invoice_id, CONTEXT_SCORE() AS score FROM invoices WHERE CONTEXT IN (ctx);")
        # Must be canonical form (no spaces inside parens)
        proj_text = " ".join(q.projections)
        assert "CONTEXT_SCORE() AS score" in proj_text

    def test_context_count_projection_canonical(self):
        q = lower_first("SELECT CONTEXT_COUNT() AS cnt FROM invoices WHERE CONTEXT IN (ctx);")
        proj_text = " ".join(q.projections)
        assert "CONTEXT_COUNT() AS cnt" in proj_text

    def test_uses_context_score_flag(self):
        q = lower_first("SELECT CONTEXT_SCORE() AS s FROM invoices WHERE CONTEXT IN (ctx);")
        assert q.uses_context_score is True

    def test_uses_context_count_flag(self):
        q = lower_first("SELECT CONTEXT_COUNT() AS c FROM invoices WHERE CONTEXT IN (ctx);")
        assert q.uses_context_count is True

    def test_context_predicate_basic(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT IN (late_invoice);")
        assert len(q.context_predicates) == 1
        pred = q.context_predicates[0]
        assert isinstance(pred, ContextPredicate)
        assert pred.negated is False
        assert len(pred.refs) == 1
        assert pred.refs[0].name == "late_invoice"

    def test_context_predicate_not_in(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT NOT IN (ctx);")
        pred = q.context_predicates[0]
        assert pred.negated is True

    def test_context_predicate_binding_alias(self):
        q = lower_first("SELECT * FROM invoices AS i WHERE CONTEXT ON i IN (ctx);")
        pred = q.context_predicates[0]
        assert pred.binding_alias == "i"

    def test_context_ref_weight(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT IN (ctx WEIGHT 0.8);")
        ref = q.context_predicates[0].refs[0]
        assert ref.weight == pytest.approx(0.8)

    def test_context_ref_then_chain(self):
        q = lower_first("SELECT * FROM invoices WHERE CONTEXT IN (a THEN b);")
        pred = q.context_predicates[0]
        assert pred.sequence_mode is True
        assert len(pred.refs) == 2

    def test_context_score_without_alias(self):
        q = lower_first("SELECT CONTEXT_SCORE() FROM invoices WHERE CONTEXT IN (ctx);")
        proj_text = " ".join(q.projections)
        assert "CONTEXT_SCORE()" in proj_text
        # No "AS" without alias
        assert "AS" not in proj_text.upper() or "CONTEXT_SCORE() AS" not in proj_text


# ── SemanticLowerer — DDL ───────────────────────────────────────────────────


class TestLowerDDL:
    def test_create_context_name(self):
        stmt = lower_first(
            "CREATE CONTEXT late_invoice ON invoice_id AS SELECT invoice_id FROM invoices WHERE status = 'open';"
        )
        assert isinstance(stmt, ContextDefinitionModel)
        assert stmt.name == "late_invoice"

    def test_create_context_entity_key(self):
        stmt = lower_first(
            "CREATE CONTEXT late_invoice ON invoice_id AS SELECT invoice_id FROM invoices;"
        )
        assert stmt.entity_key_name == "invoice_id"

    def test_create_context_with_score(self):
        stmt = lower_first(
            "CREATE CONTEXT risky ON vendor_id SCORE vendor_risk_score AS SELECT vendor_id FROM vendors;"
        )
        assert stmt.score_expression is not None

    def test_create_event_log_fields(self):
        stmt = lower_first(
            "CREATE EVENT LOG ap_log FROM invoices ON case_id ACTIVITY activity TIMESTAMP ts;"
        )
        assert isinstance(stmt, EventLogDefinitionModel)
        assert stmt.name == "ap_log"
        assert stmt.source_table == "invoices"
        assert stmt.case_column == "case_id"
        assert stmt.activity_column == "activity"
        assert stmt.timestamp_column == "ts"

    def test_create_event_log_with_resource(self):
        stmt = lower_first(
            "CREATE EVENT LOG ap_log FROM invoices ON case_id ACTIVITY act TIMESTAMP ts RESOURCE res;"
        )
        assert stmt.resource_column == "res"

    def test_create_process_model_fields(self):
        stmt = lower_first(
            "CREATE PROCESS MODEL ap_model FOR EVENT LOG ap_log EXPECTED PATH ('submit', 'approve', 'pay');"
        )
        assert isinstance(stmt, ProcessModelDefinitionModel)
        assert stmt.name == "ap_model"
        assert stmt.event_log_name == "ap_log"
        assert stmt.expected_paths == [["submit", "approve", "pay"]]


# ── SemanticAnalyzer — diagnostics ─────────────────────────────────────────


class TestSemanticAnalyzerDiagnostics:
    def test_e120_missing_entity_key(self):
        # Grammar requires ON key so we test the analyzer path directly
        from contextql.semantic import ContextDefinitionModel, StatementKind, Severity
        stmt = ContextDefinitionModel(
            kind=StatementKind.CREATE_CONTEXT,
            raw_sql="",
            name="no_key",
            entity_key_name=None,
        )
        analyzer = SemanticAnalyzer()
        result = analyzer.analyze([stmt])
        codes = [d.code for d in result.diagnostics]
        assert "E120" in codes

    def test_e130_event_log_missing_source(self):
        from contextql.semantic import EventLogDefinitionModel, StatementKind
        stmt = EventLogDefinitionModel(
            kind=StatementKind.CREATE_EVENT_LOG,
            raw_sql="",
            name="mylog",
            source_table=None,
            case_column="case_id",
            activity_column="act",
            timestamp_column="ts",
        )
        result = SemanticAnalyzer().analyze([stmt])
        assert any(d.code == "E130" for d in result.diagnostics)

    def test_e131_event_log_missing_case(self):
        from contextql.semantic import EventLogDefinitionModel, StatementKind
        stmt = EventLogDefinitionModel(
            kind=StatementKind.CREATE_EVENT_LOG,
            raw_sql="",
            name="mylog",
            source_table="src",
            case_column=None,
            activity_column="act",
            timestamp_column="ts",
        )
        result = SemanticAnalyzer().analyze([stmt])
        assert any(d.code == "E131" for d in result.diagnostics)

    def test_e132_e133_missing_activity_timestamp(self):
        from contextql.semantic import EventLogDefinitionModel, StatementKind
        stmt = EventLogDefinitionModel(
            kind=StatementKind.CREATE_EVENT_LOG,
            raw_sql="",
            name="mylog",
            source_table="src",
            case_column="case_id",
            activity_column=None,
            timestamp_column=None,
        )
        result = SemanticAnalyzer().analyze([stmt])
        codes = [d.code for d in result.diagnostics]
        assert "E132" in codes
        assert "E133" in codes

    def test_e140_process_model_no_paths(self):
        from contextql.semantic import ProcessModelDefinitionModel, StatementKind
        stmt = ProcessModelDefinitionModel(
            kind=StatementKind.CREATE_PROCESS_MODEL,
            raw_sql="",
            name="m",
            event_log_name=None,
            expected_paths=[],
        )
        result = SemanticAnalyzer().analyze([stmt])
        assert any(d.code == "E140" for d in result.diagnostics)

    def test_e141_undefined_event_log(self):
        from contextql.semantic import ProcessModelDefinitionModel, StatementKind
        stmt = ProcessModelDefinitionModel(
            kind=StatementKind.CREATE_PROCESS_MODEL,
            raw_sql="",
            name="m",
            event_log_name="nonexistent",
            expected_paths=[["a", "b"]],
        )
        result = SemanticAnalyzer().analyze([stmt])
        assert any(d.code == "E141" for d in result.diagnostics)

    def test_clean_query_ok(self):
        catalog = InMemoryCatalog(
            contexts={"ctx": ContextCatalogEntry("ctx", "id")},
        )
        result = analyze_sql(
            "SELECT * FROM t WHERE CONTEXT IN (ctx);",
            catalog=catalog,
        )
        assert result.ok is True
        assert len(result.diagnostics) == 0

    def test_analyze_sql_returns_statements(self):
        result = analyze_sql("SELECT * FROM t;")
        assert len(result.statements) == 1
        assert isinstance(result.statements[0], QueryModel)
