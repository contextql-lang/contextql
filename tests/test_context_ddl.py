"""Executable context DDL (plan section 6.3, PR 4).

CREATE / ALTER / DROP / SHOW / DESCRIBE / VALIDATE / REFRESH CONTEXT execute
against the engine catalog. Option validation follows SPEC.md section 6
(E150-E158) and DROP dependency behavior follows section 8 (E159).
"""
import pandas as pd
import pytest

import contextql as cql


@pytest.fixture
def engine():
    eng = cql.Engine()
    df = pd.DataFrame(
        {
            "txn_id": [1, 2, 3, 4],
            "status": ["open", "failed", "settled", "failed"],
            "amount": [100.0, 250.0, 75.0, 900.0],
        }
    )
    eng.register_table("txns", df, primary_key="txn_id")
    return eng


CREATE_FAILED = """
CREATE CONTEXT failed_txn ON txn_id
AS SELECT txn_id FROM txns WHERE status = 'failed';
"""


class TestCreateContext:
    def test_create_registers_and_query_works(self, engine):
        engine.execute(CREATE_FAILED)
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn) ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [2, 4]

    def test_create_persists_catalog_metadata(self, engine):
        engine.execute(CREATE_FAILED)
        entry = engine._catalog.get_context("failed_txn")
        assert entry is not None
        assert entry.version == 1
        assert entry.definition_hash is not None
        assert entry.lifecycle_state == "active"

    def test_duplicate_create_fails_without_replace(self, engine):
        engine.execute(CREATE_FAILED)
        with pytest.raises(ValueError, match="exists"):
            engine.execute(CREATE_FAILED)

    def test_create_or_replace_bumps_version(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            """
            CREATE OR REPLACE CONTEXT failed_txn ON txn_id
            AS SELECT txn_id FROM txns WHERE status = 'failed' AND amount > 500;
            """
        )
        entry = engine._catalog.get_context("failed_txn")
        assert entry.version == 2
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn);"
        )
        assert list(result.to_pandas()["txn_id"]) == [4]

    def test_self_reference_is_rejected(self, engine):
        with pytest.raises(ValueError, match="[Ss]elf|[Cc]ycle"):
            engine.execute(
                """
                CREATE CONTEXT loopy ON txn_id AS
                  COMPOSE (loopy) WITH STRATEGY UNION;
                """
            )

    def test_dependency_cycle_is_rejected(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            """
            CREATE CONTEXT big_failed ON txn_id AS
              COMPOSE (failed_txn) WITH STRATEGY UNION;
            """
        )
        with pytest.raises(ValueError, match="[Cc]ycle"):
            engine.execute(
                """
                CREATE OR REPLACE CONTEXT failed_txn ON txn_id AS
                  COMPOSE (big_failed) WITH STRATEGY UNION;
                """
            )

    def test_unknown_dependency_is_rejected(self, engine):
        with pytest.raises(ValueError, match="nope_ctx"):
            engine.execute(
                """
                CREATE CONTEXT c ON txn_id AS
                  COMPOSE (nope_ctx) WITH STRATEGY UNION;
                """
            )


class TestOptionValidation:
    def _create(self, engine, options: str):
        return engine.execute(
            f"CREATE CONTEXT c ON txn_id WITH ({options}) "
            "AS SELECT txn_id FROM txns;"
        )

    def test_unknown_option_e150(self, engine):
        with pytest.raises(ValueError, match="E150"):
            self._create(engine, "no_such_option = TRUE")

    def test_duplicate_option_e151(self, engine):
        with pytest.raises(ValueError, match="E151"):
            self._create(engine, "materialized = TRUE, MATERIALIZED = FALSE")

    def test_bad_duration_e152(self, engine):
        with pytest.raises(ValueError, match="E152"):
            self._create(engine, "materialized = TRUE, stale_after = 'soonish'")

    def test_interval_without_scheduled_e153(self, engine):
        with pytest.raises(ValueError, match="E153"):
            self._create(
                engine,
                "materialized = TRUE, refresh_interval = '5 minutes'",
            )

    def test_incremental_without_watermark_e154(self, engine):
        with pytest.raises(ValueError, match="E154"):
            self._create(
                engine,
                "materialized = TRUE, refresh_mode = 'incremental'",
            )

    def test_retention_without_history_e155(self, engine):
        with pytest.raises(ValueError, match="E155"):
            self._create(
                engine,
                "materialized = TRUE, history_retention = '90 days'",
            )

    def test_scheduled_without_materialized_e156(self, engine):
        with pytest.raises(ValueError, match="E156"):
            self._create(
                engine,
                "refresh_mode = 'scheduled', refresh_interval = '5 minutes'",
            )

    def test_stale_after_below_interval_e158(self, engine):
        with pytest.raises(ValueError, match="E158"):
            self._create(
                engine,
                "materialized = TRUE, refresh_mode = 'scheduled', "
                "refresh_interval = '10 minutes', stale_after = '5 minutes'",
            )

    def test_valid_options_accepted(self, engine):
        self._create(
            engine,
            "materialized = TRUE, storage = 'roaring', history = TRUE, "
            "history_retention = '90 days'",
        )
        entry = engine._catalog.get_context("c")
        assert entry.materialization.materialized is True
        assert entry.materialization.storage == "roaring"
        assert entry.materialization.history is True
        assert entry.materialization.history_retention == "90 days"


class TestDropContext:
    def test_drop_removes_context(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute("DROP CONTEXT failed_txn;")
        assert engine._catalog.get_context("failed_txn") is None

    def test_drop_missing_errors(self, engine):
        with pytest.raises(ValueError, match="failed_txn"):
            engine.execute("DROP CONTEXT failed_txn;")

    def test_drop_if_exists_is_silent(self, engine):
        engine.execute("DROP CONTEXT IF EXISTS failed_txn;")

    def test_drop_restrict_with_dependent_e159(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT wrapper ON txn_id AS "
            "COMPOSE (failed_txn) WITH STRATEGY UNION;"
        )
        with pytest.raises(ValueError, match="E159"):
            engine.execute("DROP CONTEXT failed_txn RESTRICT;")
        # default mode is RESTRICT
        with pytest.raises(ValueError, match="E159"):
            engine.execute("DROP CONTEXT failed_txn;")

    def test_drop_cascade_removes_dependents(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT wrapper ON txn_id AS "
            "COMPOSE (failed_txn) WITH STRATEGY UNION;"
        )
        engine.execute("DROP CONTEXT failed_txn CASCADE;")
        assert engine._catalog.get_context("failed_txn") is None
        assert engine._catalog.get_context("wrapper") is None


class TestShowDescribe:
    def test_show_contexts_lists(self, engine):
        engine.execute(CREATE_FAILED)
        df = engine.execute("SHOW CONTEXTS;").to_pandas()
        assert "failed_txn" in list(df["name"])

    def test_show_contexts_like(self, engine):
        engine.execute(CREATE_FAILED)
        df = engine.execute("SHOW CONTEXTS LIKE 'nope%';").to_pandas()
        assert len(df) == 0

    def test_describe_context(self, engine):
        engine.execute(CREATE_FAILED)
        df = engine.execute("DESCRIBE CONTEXT failed_txn;").to_pandas()
        row = df.iloc[0]
        assert row["name"] == "failed_txn"
        assert row["entity_key"] == "txn_id"
        assert row["lifecycle_state"] == "active"


class TestValidateRefresh:
    def test_validate_context(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute("VALIDATE CONTEXT failed_txn;")
        entry = engine._catalog.get_context("failed_txn")
        assert entry.last_validated_at is not None

    def test_refresh_context(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        entry = engine._catalog.get_context("failed_txn")
        assert entry.last_refreshed_at is not None

    def test_refresh_all(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute("REFRESH ALL CONTEXTS;")
        entry = engine._catalog.get_context("failed_txn")
        assert entry.last_refreshed_at is not None


class TestAlterContext:
    def test_set_description(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "ALTER CONTEXT failed_txn SET DESCRIPTION 'failed transactions';"
        )
        df = engine.execute("DESCRIBE CONTEXT failed_txn;").to_pandas()
        assert df.iloc[0]["description"] == "failed transactions"

    def test_set_definition_bumps_version_and_invalidates(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "ALTER CONTEXT failed_txn SET DEFINITION AS "
            "SELECT txn_id FROM txns WHERE status = 'failed' AND amount > 500;"
        )
        entry = engine._catalog.get_context("failed_txn")
        assert entry.version == 2
        assert entry.current_snapshot_version is None
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn);"
        )
        assert list(result.to_pandas()["txn_id"]) == [4]

    def test_rename(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute("ALTER CONTEXT failed_txn RENAME TO bad_txn;")
        assert engine._catalog.get_context("failed_txn") is None
        assert engine._catalog.get_context("bad_txn") is not None


class TestAuditEvents:
    def test_ddl_emits_audit_events(self, engine):
        events = []
        engine.on_ddl_audit(lambda event: events.append(event))
        engine.execute(CREATE_FAILED)
        engine.execute("DROP CONTEXT failed_txn;")
        actions = [e["action"] for e in events]
        assert actions == ["create_context", "drop_context"]
        assert events[0]["context"] == "failed_txn"

    def test_failing_audit_callback_does_not_fail_ddl(self, engine):
        def bad_callback(event):
            raise RuntimeError("audit sink down")

        engine.on_ddl_audit(bad_callback)
        engine.execute(CREATE_FAILED)  # must not raise
        assert engine._catalog.get_context("failed_txn") is not None


class TestReviewRegressions:
    """Regressions for the PR 4 code-review findings."""

    def test_rename_onto_existing_context_is_rejected(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT other ON txn_id AS "
            "SELECT txn_id FROM txns WHERE status = 'open';"
        )
        with pytest.raises(ValueError, match="already"):
            engine.execute("ALTER CONTEXT failed_txn RENAME TO other;")
        # both contexts intact
        assert engine._catalog.get_context("failed_txn") is not None
        assert engine._catalog.get_context("other") is not None

    def test_rename_with_dependents_is_rejected(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT wrapper ON txn_id AS "
            "COMPOSE (failed_txn) WITH STRATEGY UNION;"
        )
        with pytest.raises(ValueError, match="depended on by"):
            engine.execute("ALTER CONTEXT failed_txn RENAME TO renamed;")

    def test_set_definition_recomputes_dependencies(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT open_txn ON txn_id AS "
            "SELECT txn_id FROM txns WHERE status = 'open';"
        )
        engine.execute(
            "CREATE CONTEXT combo ON txn_id AS "
            "COMPOSE (failed_txn) WITH STRATEGY UNION;"
        )
        engine.execute(
            "ALTER CONTEXT combo SET DEFINITION AS "
            "SELECT txn_id FROM txns WHERE CONTEXT IN (open_txn);"
        )
        entry = engine._catalog.get_context("combo")
        assert entry.dependencies == ["open_txn"]
        # dropping the now-unreferenced context succeeds
        engine.execute("DROP CONTEXT failed_txn;")
        # dropping the newly-referenced one is blocked
        with pytest.raises(ValueError, match="E159"):
            engine.execute("DROP CONTEXT open_txn;")

    def test_roaring_storage_on_string_key_e157(self, engine):
        import pandas as pd
        engine.register_table(
            "customers",
            pd.DataFrame({"customer_code": ["a", "b"], "tier": [1, 2]}),
            primary_key="customer_code",
            primary_key_type="VARCHAR",
        )
        with pytest.raises(ValueError, match="E157"):
            engine.execute(
                "CREATE CONTEXT vip ON customer_code "
                "WITH (materialized = TRUE, storage = 'roaring') "
                "AS SELECT customer_code FROM customers WHERE tier = 1;"
            )

    def test_non_identifier_score_rejected_at_create(self, engine):
        with pytest.raises(ValueError, match="SCORE"):
            engine.execute(
                "CREATE CONTEXT scored ON txn_id SCORE amount * 2 "
                "AS SELECT txn_id, amount FROM txns;"
            )

    def test_set_state_rejects_unknown_state(self, engine):
        engine.execute(CREATE_FAILED)
        with pytest.raises(ValueError, match="lifecycle state"):
            engine.execute("ALTER CONTEXT failed_txn SET STATE 'banana';")
        engine.execute("ALTER CONTEXT failed_txn SET STATE 'retired';")
        entry = engine._catalog.get_context("failed_txn")
        assert entry.lifecycle_state == "retired"

    def test_validate_catches_missing_score_column(self, engine):
        engine.execute(
            "CREATE CONTEXT scored ON txn_id SCORE risk_score "
            "AS SELECT txn_id FROM txns WHERE status = 'failed';"
        )
        with pytest.raises(ValueError, match="risk_score"):
            engine.execute("VALIDATE CONTEXT scored;")

    def test_validate_catches_missing_entity_key(self, engine):
        engine.execute(
            "CREATE CONTEXT keyless ON txn_id "
            "AS SELECT status FROM txns WHERE status = 'failed';"
        )
        with pytest.raises(ValueError, match="entity key"):
            engine.execute("VALIDATE CONTEXT keyless;")

    def test_refresh_all_continues_past_failures(self, engine):
        engine.execute(CREATE_FAILED)
        engine.execute(
            "CREATE CONTEXT doomed ON txn_id "
            "AS SELECT txn_id FROM vanishing_table;"
        )
        result = engine.execute("REFRESH ALL CONTEXTS;").to_pandas()
        by_context = dict(zip(result["context"], result["status"]))
        assert by_context["failed_txn"] == "ok"
        assert by_context["doomed"] == "failed"
        entry = engine._catalog.get_context("failed_txn")
        assert entry.last_refreshed_at is not None
