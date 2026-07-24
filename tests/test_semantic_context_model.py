"""Semantic model completeness for context definitions (plan section 6.2).

The lowerer must retain every part of a CREATE CONTEXT statement: namespace,
parameters, entity key, definition body or composition, score expression,
temporal declaration, description, tags, classification, dependencies,
composition strategy, and WITH options.
"""
import pytest

from contextql.parser import ContextQLParser
from contextql.semantic import (
    ContextCatalogEntry,
    ContextDefinitionModel,
    MaterializationSettings,
    SemanticLowerer,
    StatementKind,
)

from tests.test_post_trade_definition import SETTLEMENT_INTERVENTION_REQUIRED


@pytest.fixture
def lowerer():
    return SemanticLowerer()


def lower_one(lowerer, sql: str) -> ContextDefinitionModel:
    parser = ContextQLParser()
    statements = lowerer.lower(parser.parse(sql))
    assert len(statements) == 1
    model = statements[0]
    assert isinstance(model, ContextDefinitionModel)
    return model


class TestPlannedDefinitionLowering:
    """The full planned definition must survive lowering intact."""

    def test_core_fields(self, lowerer):
        model = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert model.kind == StatementKind.CREATE_CONTEXT
        assert model.name == "settlement_intervention_required"
        assert model.entity_key_name == "transaction_id"
        assert model.score_expression == "intervention_priority"
        assert model.temporal_column == "status_recorded_at"
        assert model.temporal_granularity == "MINUTE"
        assert model.description == (
            "Transactions requiring action to prevent or resolve settlement failure"
        )
        assert model.tags == ["post-trade", "settlement", "operations"]

    def test_classification_is_retained(self, lowerer):
        model = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert model.classification == "internal"

    def test_options_are_retained_and_typed(self, lowerer):
        model = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert model.options == {
            "materialized": True,
            "storage": "roaring",
            "refresh_mode": "scheduled",
            "refresh_interval": "1 minute",
            "stale_after": "2 minutes",
            "history": True,
            "history_retention": "90 days",
            "source_watermark": "status_recorded_at",
        }

    def test_definition_sql_is_retained(self, lowerer):
        model = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert model.definition_sql is not None
        assert model.definition_sql.lstrip().upper().startswith("SELECT")
        assert "predicted_fail_probability" in model.definition_sql
        assert model.composition is None

    def test_no_spurious_dependencies(self, lowerer):
        model = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert model.dependencies == []


class TestOptionValueTyping:
    """Option values lower to Python-native values for simple literals."""

    def test_boolean_number_string_identifier(self, lowerer):
        model = lower_one(
            lowerer,
            """
            CREATE CONTEXT c ON id WITH (
                materialized = FALSE,
                storage = 'set',
                refresh_interval = '5 minutes',
                source_watermark = updated_at
            ) AS SELECT id FROM t;
            """,
        )
        assert model.options["materialized"] is False
        assert model.options["storage"] == "set"
        assert model.options["refresh_interval"] == "5 minutes"
        assert model.options["source_watermark"] == "updated_at"

    def test_option_names_lowered_case_insensitively(self, lowerer):
        model = lower_one(
            lowerer,
            "CREATE CONTEXT c ON id WITH (MATERIALIZED = TRUE) AS SELECT id FROM t;",
        )
        assert model.options == {"materialized": True}


class TestParameters:
    def test_parameters_with_defaults(self, lowerer):
        model = lower_one(
            lowerer,
            """
            CREATE CONTEXT overdue(threshold INT DEFAULT 30, region VARCHAR)
              ON invoice_id
            AS SELECT invoice_id FROM invoices WHERE days_overdue > threshold;
            """,
        )
        assert len(model.parameters) == 2
        p0, p1 = model.parameters
        assert (p0.name, p0.type_name, p0.default) == ("threshold", "INT", 30)
        assert (p1.name, p1.type_name, p1.default) == ("region", "VARCHAR", None)


class TestNamespace:
    def test_qualified_name_splits_namespace(self, lowerer):
        model = lower_one(
            lowerer,
            "CREATE CONTEXT ops.settlement_risk ON txn_id AS SELECT txn_id FROM t;",
        )
        assert model.namespace == "ops"
        assert model.name == "settlement_risk"

    def test_unqualified_name_has_no_namespace(self, lowerer):
        model = lower_one(
            lowerer,
            "CREATE CONTEXT settlement_risk ON txn_id AS SELECT txn_id FROM t;",
        )
        assert model.namespace is None
        assert model.name == "settlement_risk"


class TestComposition:
    def test_compose_items_weights_strategy(self, lowerer):
        model = lower_one(
            lowerer,
            """
            CREATE CONTEXT combined ON entity_id AS
              COMPOSE (ctx_a WEIGHT 0.6, ctx_b WEIGHT 0.4, ctx_c)
              WITH STRATEGY WEIGHTED;
            """,
        )
        assert model.definition_sql is None
        assert model.composition is not None
        assert model.composition_strategy == "WEIGHTED"
        items = model.composition.items
        assert [(i.name, i.weight) for i in items] == [
            ("ctx_a", 0.6),
            ("ctx_b", 0.4),
            ("ctx_c", None),
        ]
        assert model.dependencies == ["ctx_a", "ctx_b", "ctx_c"]


class TestDefinitionHash:
    def test_hash_is_stable(self, lowerer):
        a = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        b = lower_one(lowerer, SETTLEMENT_INTERVENTION_REQUIRED)
        assert a.definition_hash() == b.definition_hash()

    def test_hash_changes_with_body(self, lowerer):
        a = lower_one(lowerer, "CREATE CONTEXT c ON id AS SELECT id FROM t;")
        b = lower_one(lowerer, "CREATE CONTEXT c ON id AS SELECT id FROM u;")
        assert a.definition_hash() != b.definition_hash()


class TestCatalogEntryFields:
    """ContextCatalogEntry carries plan section 6.2 metadata with defaults."""

    def test_new_fields_default(self):
        entry = ContextCatalogEntry(name="c", entity_key_name="id")
        assert entry.context_id is not None
        assert entry.namespace == "default"
        assert entry.version == 1
        assert entry.definition_hash is None
        assert entry.lifecycle_state == "draft"
        assert isinstance(entry.materialization, MaterializationSettings)
        assert entry.current_snapshot_version is None
        assert entry.last_validated_at is None
        assert entry.last_refreshed_at is None
        assert entry.data_as_of is None

    def test_materialization_defaults_match_spec(self):
        settings = MaterializationSettings()
        assert settings.materialized is False
        assert settings.storage == "auto"
        assert settings.refresh_mode == "manual"
        assert settings.refresh_interval is None
        assert settings.stale_after is None
        assert settings.history is False
        assert settings.history_retention is None
        assert settings.source_watermark is None
