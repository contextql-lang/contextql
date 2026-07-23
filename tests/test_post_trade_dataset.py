"""Deterministic post-trade dataset generation (plan section 9, PR 9)."""
import duckdb
import pytest

from contextql.datasets import generate_post_trade_dataset
from contextql.datasets.post_trade import (
    DEFAULT_AS_OF,
    reference_context_sql,
)

ROWS = 50_000


@pytest.fixture(scope="module")
def conn():
    connection = duckdb.connect(":memory:")
    generate_post_trade_dataset(connection, ROWS)
    yield connection
    connection.close()


def count(conn, name: str) -> int:
    sql = reference_context_sql(name)
    return conn.execute(
        f"SELECT COUNT(*) FROM ({sql}) AS _c"
    ).fetchone()[0]


class TestGeneration:
    def test_exact_row_count(self, conn):
        assert conn.execute(
            "SELECT COUNT(*) FROM transactions"
        ).fetchone()[0] == ROWS

    def test_transaction_ids_are_dense_integers(self, conn):
        low, high, distinct = conn.execute(
            "SELECT MIN(transaction_id), MAX(transaction_id), "
            "COUNT(DISTINCT transaction_id) FROM transactions"
        ).fetchone()
        assert (low, high, distinct) == (1, ROWS, ROWS)

    @staticmethod
    def _checksum(seed: int = 42) -> int:
        c = duckdb.connect(":memory:")
        generate_post_trade_dataset(c, 5_000, seed=seed)
        value = c.execute(
            "SELECT SUM(hash(t::VARCHAR)) FROM transactions t"
        ).fetchone()[0]
        c.close()
        return value

    def test_generation_is_deterministic(self):
        assert self._checksum() == self._checksum()

    def test_different_seed_changes_data(self):
        assert self._checksum(42) != self._checksum(43)


class TestDistributions:
    """Target proportions from plan 9.3 (10M scale), generous tolerance."""

    @pytest.mark.parametrize(
        "context, target_rate, tolerance",
        [
            ("unmatched_trade", 0.018, 0.6),
            ("missing_confirmation", 0.0095, 0.6),
            ("economic_terms_break", 0.0062, 0.6),
            ("invalid_ssi", 0.0028, 0.6),
            ("settlement_overdue", 0.0074, 0.9),
            ("approaching_market_cutoff", 0.021, 0.9),
            ("high_notional", 0.015, 0.6),
            ("predicted_settlement_fail", 0.0048, 0.9),
        ],
    )
    def test_context_rates(self, conn, context, target_rate, tolerance):
        rate = count(conn, context) / ROWS
        assert rate == pytest.approx(target_rate, rel=tolerance), context

    def test_intervention_context_is_selective(self, conn):
        rate = count(conn, "settlement_intervention_required") / ROWS
        # 10M-scale target band is 4k-15k members (0.04% - 0.15%);
        # allow a wider band at 50k test scale.
        assert 0.0002 < rate < 0.01

    def test_most_transactions_are_clean(self, conn):
        settled = conn.execute(
            "SELECT COUNT(*) FROM transactions "
            "WHERE settlement_status IN ('settled', 'pending')"
        ).fetchone()[0]
        assert settled / ROWS > 0.90


class TestCausalCorrelations:
    def test_invalid_ssi_raises_failure_rate(self, conn):
        invalid_fail, valid_fail = conn.execute(
            """
            SELECT
                AVG(CASE WHEN NOT ssi_valid AND settlement_status = 'failed'
                         THEN 1.0
                         WHEN NOT ssi_valid THEN 0.0 END),
                AVG(CASE WHEN ssi_valid AND settlement_status = 'failed'
                         THEN 1.0
                         WHEN ssi_valid THEN 0.0 END)
            FROM transactions
            """
        ).fetchone()
        assert invalid_fail > valid_fail * 5

    def test_missing_confirmation_correlates_with_breaks(self, conn):
        missing_break, present_break = conn.execute(
            """
            SELECT
                AVG(CASE WHEN NOT confirmation_received
                              AND match_status <> 'matched' THEN 1.0
                         WHEN NOT confirmation_received THEN 0.0 END),
                AVG(CASE WHEN confirmation_received
                              AND match_status <> 'matched' THEN 1.0
                         WHEN confirmation_received THEN 0.0 END)
            FROM transactions
            """
        ).fetchone()
        assert missing_break > present_break * 2

    def test_prediction_tracks_actual_failures(self, conn):
        failed_pred, settled_pred = conn.execute(
            """
            SELECT
                AVG(CASE WHEN settlement_status = 'failed'
                         THEN predicted_fail_probability END),
                AVG(CASE WHEN settlement_status = 'settled'
                         THEN predicted_fail_probability END)
            FROM transactions
            """
        ).fetchone()
        assert failed_pred > settled_pred * 5

    def test_failures_cluster_by_counterparty(self, conn):
        top_share = conn.execute(
            """
            WITH failures AS (
                SELECT counterparty_id, COUNT(*) AS n
                FROM transactions
                WHERE settlement_status = 'failed'
                GROUP BY counterparty_id
            )
            SELECT SUM(n) FILTER (
                       WHERE counterparty_id IN (
                           SELECT counterparty_id FROM failures
                           ORDER BY n DESC
                           LIMIT 200
                       )
                   ) * 1.0 / SUM(n)
            FROM failures
            """
        ).fetchone()[0]
        # 10% of counterparties should account for a disproportionate share
        assert top_share > 0.3

    def test_manual_touches_grow_with_mismatches(self, conn):
        unmatched_touches, matched_touches = conn.execute(
            """
            SELECT
                AVG(CASE WHEN match_status = 'unmatched'
                         THEN manual_touch_count END),
                AVG(CASE WHEN match_status = 'matched'
                         THEN manual_touch_count END)
            FROM transactions
            """
        ).fetchone()
        assert unmatched_touches > matched_touches + 1

    def test_as_of_anchor_used(self, conn):
        anchored = conn.execute(
            f"SELECT COUNT(*) FROM transactions "
            f"WHERE data_as_of = TIMESTAMP '{DEFAULT_AS_OF}'"
        ).fetchone()[0]
        assert anchored == ROWS
