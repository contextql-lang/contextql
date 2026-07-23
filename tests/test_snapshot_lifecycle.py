"""Snapshot lifecycle and membership history (plan sections 7.2-7.3, PR 7).

REFRESH CONTEXT on a materialized context builds a versioned snapshot in the
membership store, atomically promotes it, and (when history is enabled)
records membership changes in the shared history store.
"""
import pandas as pd
import pytest

import contextql as cql


@pytest.fixture
def engine():
    eng = cql.Engine()
    df = pd.DataFrame(
        {
            "txn_id": [1, 2, 3, 4, 5],
            "status": ["failed", "failed", "settled", "open", "failed"],
            "risk": [0.9, 0.4, 0.1, 0.2, 0.7],
        }
    )
    eng.register_table("txns", df, primary_key="txn_id")
    return eng


CREATE_MATERIALIZED = """
CREATE CONTEXT failed_txn ON txn_id
SCORE risk
WITH (materialized = TRUE, storage = 'set', history = TRUE)
AS SELECT txn_id, risk FROM txns WHERE status = 'failed';
"""


class TestSnapshotBuild:
    def test_refresh_builds_snapshot(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        entry = engine._catalog.get_context("failed_txn")
        assert entry.current_snapshot_version == 1
        store = engine._executor.membership
        snap = store.get_snapshot("failed_txn")
        assert snap is not None
        assert snap.member_count == 3
        assert store.members("failed_txn") == {1, 2, 5}

    def test_refresh_captures_scores(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        scores = engine._executor.membership.scores("failed_txn")
        assert scores == {1: 0.9, 2: 0.4, 5: 0.7}

    def test_second_refresh_promotes_new_version(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        engine.execute("REFRESH CONTEXT failed_txn;")
        store = engine._executor.membership
        assert store.get_snapshot("failed_txn").version == 2
        assert store.get_snapshot("failed_txn", version=1).state == "superseded"
        entry = engine._catalog.get_context("failed_txn")
        assert entry.current_snapshot_version == 2

    def test_snapshot_records_definition_hash(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        entry = engine._catalog.get_context("failed_txn")
        snap = engine._executor.membership.get_snapshot("failed_txn")
        assert snap.definition_hash == entry.definition_hash

    def test_non_materialized_context_builds_no_snapshot(self, engine):
        engine.execute(
            "CREATE CONTEXT plain ON txn_id "
            "AS SELECT txn_id FROM txns WHERE status = 'open';"
        )
        engine.execute("REFRESH CONTEXT plain;")
        assert engine._executor.membership.get_snapshot("plain") is None
        entry = engine._catalog.get_context("plain")
        assert entry.last_refreshed_at is not None
        assert entry.current_snapshot_version is None


class TestHistory:
    def test_first_refresh_records_additions(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        events = engine._executor.history.events("failed_txn")
        added = {e.entity_id for e in events if e.change_type == "added"}
        assert added == {1, 2, 5}

    def test_membership_change_recorded(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        # Transaction 2 settles; transaction 4 fails.
        engine.register_table(
            "txns",
            pd.DataFrame(
                {
                    "txn_id": [1, 2, 3, 4, 5],
                    "status": [
                        "failed", "settled", "settled", "failed", "failed",
                    ],
                    "risk": [0.9, 0.4, 0.1, 0.8, 0.7],
                }
            ),
            primary_key="txn_id",
        )
        engine.execute("REFRESH CONTEXT failed_txn;")
        events = engine._executor.history.events(
            "failed_txn", context_version=2
        )
        added = {e.entity_id for e in events if e.change_type == "added"}
        removed = {e.entity_id for e in events if e.change_type == "removed"}
        assert added == {4}
        assert removed == {2}
        assert engine._executor.membership.members("failed_txn") == {1, 4, 5}

    def test_score_change_recorded(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        engine.register_table(
            "txns",
            pd.DataFrame(
                {
                    "txn_id": [1, 2, 3, 4, 5],
                    "status": ["failed", "failed", "settled", "open", "failed"],
                    "risk": [0.95, 0.4, 0.1, 0.2, 0.7],
                }
            ),
            primary_key="txn_id",
        )
        engine.execute("REFRESH CONTEXT failed_txn;")
        events = engine._executor.history.events(
            "failed_txn", context_version=2
        )
        changed = [e for e in events if e.change_type == "score_changed"]
        assert len(changed) == 1
        assert changed[0].entity_id == 1
        assert changed[0].previous_score == 0.9
        assert changed[0].new_score == 0.95

    def test_history_disabled_records_nothing(self, engine):
        engine.execute(
            """
            CREATE CONTEXT quiet ON txn_id
            WITH (materialized = TRUE, storage = 'set')
            AS SELECT txn_id FROM txns WHERE status = 'failed';
            """
        )
        engine.execute("REFRESH CONTEXT quiet;")
        assert engine._executor.history.events("quiet") == []


class TestPromotionFailure:
    def test_failed_refresh_keeps_last_good_snapshot(self, engine):
        engine.execute(CREATE_MATERIALIZED)
        engine.execute("REFRESH CONTEXT failed_txn;")
        # Break the definition's source table, then attempt refresh.
        engine._adapter.unregister_table("txns")
        with pytest.raises(Exception):
            engine.execute("REFRESH CONTEXT failed_txn;")
        store = engine._executor.membership
        snap = store.get_snapshot("failed_txn")
        assert snap.version == 1
        assert snap.state == "current"
        assert store.members("failed_txn") == {1, 2, 5}
        entry = engine._catalog.get_context("failed_txn")
        assert entry.current_snapshot_version == 1
