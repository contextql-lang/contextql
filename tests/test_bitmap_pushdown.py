"""Bitmap-aware execution: semi-join pushdown before Pandas (plan 7.4, PR 8).

When every context in a query's predicates is materialized with a current
snapshot, membership narrowing happens inside DuckDB via a semi-join on the
entity key; only surviving rows are transferred (CS-11). Materialized
contexts without a snapshot raise E200; stale snapshots attach W100.
"""
import pandas as pd
import pytest

import contextql as cql


@pytest.fixture
def engine():
    eng = cql.Engine()
    df = pd.DataFrame(
        {
            "txn_id": list(range(1, 11)),
            "status": [
                "failed", "failed", "settled", "open", "failed",
                "settled", "open", "failed", "settled", "failed",
            ],
            "amount": [
                100.0, 900.0, 50.0, 75.0, 300.0,
                60.0, 80.0, 700.0, 40.0, 550.0,
            ],
        }
    )
    eng.register_table("txns", df, primary_key="txn_id")
    eng.execute(
        """
        CREATE CONTEXT failed_txn ON txn_id
        WITH (materialized = TRUE)
        AS SELECT txn_id FROM txns WHERE status = 'failed';
        """
    )
    eng.execute(
        """
        CREATE CONTEXT high_value ON txn_id
        WITH (materialized = TRUE)
        AS SELECT txn_id FROM txns WHERE amount > 500;
        """
    )
    return eng


class TestPushdown:
    def test_pushdown_filters_in_sql(self, engine):
        engine.execute("REFRESH CONTEXT failed_txn;")
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn) "
            "ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [1, 2, 5, 8, 10]
        # membership narrowing happened inside DuckDB, not in Pandas
        assert "__cql_members_0" in result.sql

    def test_union_semantics(self, engine):
        engine.execute("REFRESH ALL CONTEXTS;")
        result = engine.execute(
            "SELECT txn_id FROM txns "
            "WHERE CONTEXT IN (failed_txn, high_value) ORDER BY txn_id;"
        )
        # failed {1,2,5,8,10} union high_value {2,8,10} -> {1,2,5,8,10}
        assert list(result.to_pandas()["txn_id"]) == [1, 2, 5, 8, 10]

    def test_intersection_semantics(self, engine):
        engine.execute("REFRESH ALL CONTEXTS;")
        result = engine.execute(
            "SELECT txn_id FROM txns "
            "WHERE CONTEXT IN ALL (failed_txn, high_value) ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [2, 8, 10]

    def test_negation_semantics(self, engine):
        engine.execute("REFRESH ALL CONTEXTS;")
        result = engine.execute(
            "SELECT txn_id FROM txns "
            "WHERE CONTEXT NOT IN (failed_txn) ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [3, 4, 6, 7, 9]

    def test_pushdown_composes_with_where(self, engine):
        engine.execute("REFRESH CONTEXT failed_txn;")
        result = engine.execute(
            "SELECT txn_id FROM txns "
            "WHERE amount > 400 AND CONTEXT IN (failed_txn) "
            "ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [2, 8, 10]

    def test_snapshot_used_after_source_change(self, engine):
        """Pushdown must read the snapshot, not re-evaluate the definition."""
        engine.execute("REFRESH CONTEXT failed_txn;")
        # Change source data after the snapshot was taken.
        engine.register_table(
            "txns",
            pd.DataFrame(
                {
                    "txn_id": list(range(1, 11)),
                    "status": ["settled"] * 10,
                    "amount": [100.0] * 10,
                }
            ),
            primary_key="txn_id",
        )
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn) "
            "ORDER BY txn_id;"
        )
        # Snapshot still holds the pre-change membership.
        assert list(result.to_pandas()["txn_id"]) == [1, 2, 5, 8, 10]

    def test_trace_records_snapshot_version(self, engine):
        engine.execute("REFRESH CONTEXT failed_txn;")
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn);"
        )
        assert "failed_txn@v1" in result.trace.contexts_resolved


class TestSnapshotStates:
    def test_materialized_without_snapshot_e200(self, engine):
        with pytest.raises(ValueError, match="E200"):
            engine.execute(
                "SELECT txn_id FROM txns WHERE CONTEXT IN (failed_txn);"
            )

    def test_non_materialized_context_uses_live_path(self, engine):
        engine.execute(
            "CREATE CONTEXT open_txn ON txn_id "
            "AS SELECT txn_id FROM txns WHERE status = 'open';"
        )
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (open_txn) "
            "ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [4, 7]
        assert "__cql_members" not in result.sql

    def test_mixed_predicates_fall_back_to_live_path(self, engine):
        engine.execute("REFRESH CONTEXT failed_txn;")
        engine.execute(
            "CREATE CONTEXT open_txn ON txn_id "
            "AS SELECT txn_id FROM txns WHERE status = 'open';"
        )
        result = engine.execute(
            "SELECT txn_id FROM txns "
            "WHERE CONTEXT IN (failed_txn, open_txn) ORDER BY txn_id;"
        )
        assert list(result.to_pandas()["txn_id"]) == [1, 2, 4, 5, 7, 8, 10]
        assert "__cql_members" not in result.sql

    def test_stale_snapshot_warns_w100(self, engine):
        engine.execute(
            """
            CREATE CONTEXT quick_stale ON txn_id
            WITH (materialized = TRUE, refresh_mode = 'scheduled',
                  refresh_interval = '1 second', stale_after = '1 second')
            AS SELECT txn_id FROM txns WHERE status = 'open';
            """
        )
        engine.execute("REFRESH CONTEXT quick_stale;")
        import time
        time.sleep(1.1)
        result = engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (quick_stale);"
        )
        codes = [d.code for d in result.diagnostics]
        assert "W100" in codes


class TestScoringWithSnapshots:
    def test_scores_come_from_snapshot(self, engine):
        engine.execute(
            """
            CREATE CONTEXT scored_failed ON txn_id
            SCORE fail_score
            WITH (materialized = TRUE)
            AS SELECT txn_id, amount / 1000.0 AS fail_score
               FROM txns WHERE status = 'failed';
            """
        )
        engine.execute("REFRESH CONTEXT scored_failed;")
        result = engine.execute(
            "SELECT txn_id, CONTEXT_SCORE() AS s FROM txns "
            "WHERE CONTEXT IN (scored_failed) ORDER BY CONTEXT DESC LIMIT 3;"
        )
        df = result.to_pandas()
        assert list(df["txn_id"]) == [2, 8, 10]
        assert df["s"].iloc[0] == pytest.approx(0.9)
