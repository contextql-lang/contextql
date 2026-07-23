"""Regression coverage for post-trade correctness hardening WP1/WP4/WP6."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import contextql as cql
from contextql.history import MembershipChange, MembershipHistoryStore
from contextql.membership_roaring import RoaringMembershipStore
from contextql.providers import RemoteResult


@pytest.fixture
def engine():
    engine = cql.Engine()
    engine.register_table(
        "txns",
        pd.DataFrame(
            {
                "txn_id": [1, 2, 3, 4],
                "status": ["failed", "open", "failed", "open"],
                "event_at": pd.to_datetime(
                    [
                        "2026-07-01T00:00:00Z",
                        "2026-07-01T01:00:00Z",
                        "2026-07-01T02:00:00Z",
                        "2026-07-01T03:00:00Z",
                    ],
                    utc=True,
                ),
                "risk": [0.9, 0.2, 0.7, 0.1],
            }
        ),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    return engine


def _create_materialized(engine, name="failed"):
    engine.execute(
        f"""
        CREATE CONTEXT {name} ON txn_id
        WITH (materialized = TRUE, storage = 'set', history = TRUE)
        AS SELECT txn_id FROM txns WHERE status = 'failed';
        """
    )
    engine.execute(f"REFRESH CONTEXT {name};")


def test_replace_invalidates_old_snapshot(engine):
    _create_materialized(engine)
    old = engine._catalog.get_context("failed")
    engine.execute(
        """
        CREATE OR REPLACE CONTEXT failed ON txn_id
        WITH (materialized = TRUE, storage = 'set')
        AS SELECT txn_id FROM txns WHERE status = 'open';
        """
    )
    replaced = engine._catalog.get_context("failed")
    assert replaced.context_id == old.context_id
    assert replaced.current_snapshot_version is None
    with pytest.raises(ValueError, match="E200"):
        engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN (failed);"
        )
    engine.execute("REFRESH CONTEXT failed;")
    result = engine.execute(
        "SELECT txn_id FROM txns WHERE CONTEXT IN (failed) ORDER BY txn_id;"
    )
    assert list(result.to_pandas()["txn_id"]) == [2, 4]


def test_drop_recreate_gets_new_identity(engine):
    _create_materialized(engine)
    old_id = engine._catalog.get_context("failed").context_id
    engine.execute("DROP CONTEXT failed;")
    _create_materialized(engine)
    assert engine._catalog.get_context("failed").context_id != old_id


def test_rename_retains_identity_and_snapshot(engine):
    _create_materialized(engine, "ops.failed")
    before = engine._catalog.get_context("ops.failed")
    engine.execute("ALTER CONTEXT ops.failed RENAME TO needs_action;")
    after = engine._catalog.get_context("ops.needs_action")
    assert after.context_id == before.context_id
    assert after.current_snapshot_version == before.current_snapshot_version
    result = engine.execute(
        "SELECT txn_id FROM txns "
        "WHERE CONTEXT IN (ops.needs_action) ORDER BY txn_id;"
    )
    assert list(result.to_pandas()["txn_id"]) == [1, 3]


def test_same_name_in_two_namespaces(engine):
    engine.execute(
        "CREATE CONTEXT ops.risk ON txn_id "
        "AS SELECT txn_id FROM txns WHERE status = 'failed';"
    )
    engine.execute(
        "CREATE CONTEXT finance.risk ON txn_id "
        "AS SELECT txn_id FROM txns WHERE status = 'open';"
    )
    assert engine._catalog.get_context("ops.risk") is not None
    assert engine._catalog.get_context("finance.risk") is not None


def test_unqualified_name_uses_only_default_namespace(engine):
    engine.execute(
        "CREATE CONTEXT risk ON txn_id "
        "AS SELECT txn_id FROM txns WHERE status = 'failed';"
    )
    engine.execute(
        "CREATE CONTEXT ops.risk ON txn_id "
        "AS SELECT txn_id FROM txns WHERE status = 'open';"
    )
    result = engine.execute(
        "SELECT txn_id FROM txns WHERE CONTEXT IN (risk) "
        "ORDER BY txn_id;"
    )
    assert list(result.to_pandas()["txn_id"]) == [1, 3]


def test_rename_collision_is_rejected(engine):
    engine.execute(
        "CREATE CONTEXT ops.left_side ON txn_id "
        "AS SELECT txn_id FROM txns;"
    )
    engine.execute(
        "CREATE CONTEXT ops.right_side ON txn_id "
        "AS SELECT txn_id FROM txns;"
    )
    with pytest.raises(ValueError, match="already exists"):
        engine.execute(
            "ALTER CONTEXT ops.left_side RENAME TO right_side;"
        )


def test_explicit_set_is_honored_with_roaring_installed(engine):
    _create_materialized(engine)
    snapshot = engine.membership.get_snapshot("failed")
    assert snapshot.storage_kind == "set"


def test_auto_resolves_to_roaring_when_backend_is_installed(engine):
    engine.execute(
        """
        CREATE CONTEXT automatic ON txn_id
        WITH (materialized = TRUE, storage = 'auto')
        AS SELECT txn_id FROM txns;
        """
    )
    engine.execute("REFRESH CONTEXT automatic;")
    assert engine.membership.get_snapshot(
        "automatic"
    ).storage_kind == "roaring"


def test_roaring_delta_does_not_expand_current_membership(monkeypatch):
    now = datetime.now(timezone.utc)
    store = RoaringMembershipStore()
    store.put_snapshot(
        "ctx",
        range(1000),
        computed_at=now,
        data_as_of=now,
        storage_kind="roaring",
    )
    monkeypatch.setattr(
        store,
        "_to_set",
        lambda *_: (_ for _ in ()).throw(
            AssertionError("Roaring delta expanded to a Python set")
        ),
    )
    store.apply_delta(
        "ctx",
        additions=[1001, 1002],
        removals=[1, 2],
        computed_at=now,
        data_as_of=now,
    )
    bitmap = store.membership_object("ctx")
    assert 1 not in bitmap
    assert 1002 in bitmap


def test_history_false_does_not_read_old_members(engine, monkeypatch):
    engine.execute(
        """
        CREATE CONTEXT no_history ON txn_id
        WITH (materialized = TRUE, storage = 'set', history = FALSE)
        AS SELECT txn_id FROM txns WHERE status = 'failed';
        """
    )
    engine.execute("REFRESH CONTEXT no_history;")
    monkeypatch.setattr(
        engine.membership,
        "members",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("old membership was expanded")
        ),
    )
    engine.execute("REFRESH CONTEXT no_history;")
    assert engine._executor.history.events("no_history") == []


def test_observed_watermark_is_maximum_and_empty_retains_it(engine):
    engine.execute(
        """
        CREATE CONTEXT watermarked ON txn_id
        WITH (
            materialized = TRUE,
            storage = 'set',
            source_watermark = event_at
        )
        AS SELECT txn_id, event_at FROM txns WHERE status = 'failed';
        """
    )
    engine.execute("REFRESH CONTEXT watermarked;")
    first = engine.membership.get_snapshot("watermarked")
    assert pd.Timestamp(first.source_watermark).tz_convert("UTC") == (
        pd.Timestamp("2026-07-01T02:00:00Z")
    )

    engine.register_table(
        "txns",
        pd.DataFrame(
            columns=["txn_id", "status", "event_at", "risk"]
        ),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    engine.execute("REFRESH CONTEXT watermarked;")
    second = engine.membership.get_snapshot("watermarked")
    assert second.source_watermark == first.source_watermark


def test_native_incremental_is_rejected(engine):
    with pytest.raises(ValueError, match="E161"):
        engine.execute(
            """
            CREATE CONTEXT incremental_native ON txn_id
            WITH (
                materialized = TRUE,
                refresh_mode = 'incremental',
                source_watermark = event_at
            )
            AS SELECT txn_id, event_at FROM txns;
            """
        )


def test_temporal_at_between_and_version(engine):
    engine.execute(
        """
        CREATE CONTEXT temporal_failed ON txn_id
        SCORE risk
        TEMPORAL (event_at, SECOND)
        WITH (materialized = TRUE, storage = 'set', history = TRUE)
        AS SELECT txn_id, risk, event_at
           FROM txns WHERE status = 'failed';
        """
    )
    engine.execute("REFRESH CONTEXT temporal_failed;")

    at_result = engine.execute(
        "SELECT txn_id FROM txns "
        "WHERE CONTEXT IN ("
        "temporal_failed AT '2026-07-01T00:30:00+00:00'"
        ") ORDER BY txn_id;"
    )
    assert list(at_result.to_pandas()["txn_id"]) == [1]

    between_result = engine.execute(
        "SELECT txn_id FROM txns "
        "WHERE CONTEXT IN ("
        "temporal_failed BETWEEN '2026-07-01T00:00:00+00:00' "
        "AND '2026-07-01T02:30:00+00:00'"
        ") ORDER BY txn_id;"
    )
    assert list(between_result.to_pandas()["txn_id"]) == [1, 3]

    version_result = engine.execute(
        "SELECT txn_id FROM txns "
        "WHERE CONTEXT IN (temporal_failed AT VERSION 1) ORDER BY txn_id;"
    )
    assert list(version_result.to_pandas()["txn_id"]) == [1, 3]


def test_temporal_validation_errors(engine):
    _create_materialized(engine, "plain")
    with pytest.raises(ValueError, match="E109"):
        engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN "
            "(plain AT '2026-07-01T00:00:00+00:00');"
        )

    engine.execute(
        """
        CREATE CONTEXT temporal_ctx ON txn_id
        TEMPORAL (event_at, SECOND)
        WITH (materialized = TRUE, storage = 'set', history = TRUE)
        AS SELECT txn_id, event_at FROM txns;
        """
    )
    engine.execute("REFRESH CONTEXT temporal_ctx;")
    with pytest.raises(ValueError, match="E203"):
        engine.execute(
            "SELECT txn_id FROM txns WHERE CONTEXT IN "
            "(temporal_ctx BETWEEN '2026-07-02T00:00:00+00:00' "
            "AND '2026-07-01T00:00:00+00:00');"
        )


def test_temporal_granularity_rounds_in_utc_and_rejects_naive_values():
    rounded = cql.Engine()
    rounded.register_table(
        "events",
        pd.DataFrame(
            {
                "txn_id": [1],
                "event_at": pd.to_datetime(
                    ["2026-07-01T02:00:59+02:00"]
                ),
            }
        ),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    rounded.execute(
        """
        CREATE CONTEXT minute_ctx ON txn_id
        TEMPORAL (event_at, MINUTE)
        WITH (materialized = TRUE, storage = 'set', history = TRUE)
        AS SELECT txn_id, event_at FROM events;
        """
    )
    rounded.execute("REFRESH CONTEXT minute_ctx;")
    result = rounded.execute(
        "SELECT txn_id FROM events WHERE CONTEXT IN "
        "(minute_ctx AT '2026-07-01T00:00:30+00:00');"
    )
    assert list(result.to_pandas()["txn_id"]) == [1]

    naive = cql.Engine()
    naive.register_table(
        "events",
        pd.DataFrame(
            {
                "txn_id": [1],
                "event_at": pd.to_datetime(
                    ["2026-07-01T00:00:59"]
                ),
            }
        ),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    naive.execute(
        """
        CREATE CONTEXT naive_ctx ON txn_id
        TEMPORAL (event_at, MINUTE)
        WITH (materialized = TRUE, storage = 'set', history = TRUE)
        AS SELECT txn_id, event_at FROM events;
        """
    )
    with pytest.raises(ValueError, match="explicit timezone"):
        naive.execute("REFRESH CONTEXT naive_ctx;")


def test_retention_preserves_anchor_and_rejects_older_queries(monkeypatch):
    import contextql.context_ddl as ddl_module

    clock = [
        datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc)
    ]
    monkeypatch.setattr(ddl_module, "_now", lambda: clock[0])
    engine = cql.Engine()

    def register(entity_id, event_at):
        engine.register_table(
            "events",
            pd.DataFrame(
                {
                    "txn_id": [entity_id],
                    "event_at": pd.to_datetime([event_at], utc=True),
                }
            ),
            primary_key="txn_id",
            primary_key_type="INT64",
        )

    register(1, "2026-07-01T00:00:00Z")
    engine.execute(
        """
        CREATE CONTEXT retained ON txn_id
        TEMPORAL (event_at, SECOND)
        WITH (
            materialized = TRUE,
            storage = 'set',
            history = TRUE,
            history_retention = '1 hour'
        )
        AS SELECT txn_id, event_at FROM events;
        """
    )
    engine.execute("REFRESH CONTEXT retained;")

    clock[0] = datetime(2026, 7, 1, 1, 0, tzinfo=timezone.utc)
    register(2, "2026-07-01T01:00:00Z")
    engine.execute("REFRESH CONTEXT retained;")

    clock[0] = datetime(2026, 7, 1, 3, 0, tzinfo=timezone.utc)
    register(3, "2026-07-01T03:00:00Z")
    engine.execute("REFRESH CONTEXT retained;")

    entry = engine._catalog.get_context("retained")
    assert entry.history_available_from == datetime(
        2026, 7, 1, 1, 0, tzinfo=timezone.utc
    )
    assert [
        snapshot.version
        for snapshot in engine.membership.snapshots("retained")
    ] == [2, 3]
    with pytest.raises(ValueError, match="E202"):
        engine.execute(
            "SELECT txn_id FROM events WHERE CONTEXT IN "
            "(retained AT '2026-07-01T00:30:00+00:00');"
        )


class _FilteringRemote:
    def __init__(self):
        self.entity_filter = None
        self.columns = None

    def query(
        self, resource, filters, columns, limit=None, *, entity_filter=None
    ):
        self.entity_filter = entity_filter
        self.columns = columns
        return RemoteResult(
            rows=[
                {"remote_id": int(value), "evidence": "ok"}
                for value in entity_filter.ids()
            ]
        )


class _LegacyRemote:
    def query(self, resource, filters, columns, limit=None):
        return RemoteResult(rows=[])


def test_large_remote_narrowing_uses_roaring_payload():
    rows = 10_001
    engine = cql.Engine()
    engine.register_table(
        "large_txns",
        pd.DataFrame({"txn_id": range(rows)}),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    engine.register_context(
        "all_large",
        "SELECT txn_id FROM large_txns",
        entity_key="txn_id",
    )
    provider = _FilteringRemote()
    engine.register_remote_provider("evidence", provider)
    result = engine.execute(
        """
        SELECT t.txn_id, e.evidence
        FROM large_txns AS t
        JOIN REMOTE(evidence.cases) AS e
          ON t.txn_id = e.remote_id
        WHERE CONTEXT IN (all_large);
        """
    )
    assert len(result.to_pandas()) == rows
    assert provider.entity_filter.entity_ids is None
    assert provider.entity_filter.bitmap_encoding == "roaring64"
    assert set(provider.columns) == {"evidence", "remote_id"}


def test_context_order_honors_secondary_tie_breaker():
    engine = cql.Engine()
    engine.register_table(
        "ranked",
        pd.DataFrame(
            {"txn_id": [4, 1, 3, 2], "score": [1.0] * 4}
        ),
        primary_key="txn_id",
        primary_key_type="INT64",
    )
    engine.execute(
        """
        CREATE CONTEXT tied ON txn_id SCORE score
        WITH (materialized = TRUE, storage = 'set')
        AS SELECT txn_id, score FROM ranked;
        """
    )
    engine.execute("REFRESH CONTEXT tied;")
    result = engine.execute(
        "SELECT txn_id, CONTEXT_SCORE() AS score FROM ranked "
        "WHERE CONTEXT IN (tied) "
        "ORDER BY CONTEXT DESC, txn_id ASC;"
    )
    assert list(result.to_pandas()["txn_id"]) == [1, 2, 3, 4]


def test_unsafe_and_unsupported_remote_joins_fail_before_provider_call(engine):
    provider = _LegacyRemote()
    engine.register_remote_provider("legacy", provider)
    _create_materialized(engine, "plain")
    with pytest.raises(ValueError, match="E301"):
        engine.execute(
            """
            SELECT t.txn_id
            FROM txns AS t
            JOIN REMOTE(legacy.cases) AS e ON t.txn_id > e.remote_id
            WHERE CONTEXT IN (plain);
            """
        )
    with pytest.raises(ValueError, match="E302"):
        engine.execute(
            """
            SELECT t.txn_id
            FROM txns AS t
            JOIN REMOTE(legacy.cases) AS e ON t.txn_id = e.remote_id
            WHERE CONTEXT IN (plain);
            """
        )


def test_materialized_fallback_fails_before_dataframe_execution(monkeypatch):
    engine = cql.Engine()
    rows = 10_001
    engine.register_table(
        "no_pk",
        pd.DataFrame(
            {"txn_id": range(rows), "status": ["bad"] * rows}
        ),
    )
    engine.execute(
        """
        CREATE CONTEXT unsafe_ctx ON txn_id
        WITH (materialized = TRUE, storage = 'roaring')
        AS SELECT txn_id FROM no_pk WHERE status = 'bad';
        """
    )
    engine.execute("REFRESH CONTEXT unsafe_ctx;")
    monkeypatch.setattr(
        engine._adapter,
        "execute_df",
        lambda *_: (_ for _ in ()).throw(
            AssertionError("base DataFrame must not be materialized")
        ),
    )
    with pytest.raises(ValueError, match="E303"):
        engine.execute(
            "SELECT txn_id FROM no_pk WHERE CONTEXT IN (unsafe_ctx);"
        )


def test_intermediate_row_cap_fails_before_dataframe_execution(monkeypatch):
    engine = cql.Engine(max_intermediate_rows=2)
    engine.register_table(
        "bounded",
        pd.DataFrame({"txn_id": [1, 2, 3]}),
        primary_key="txn_id",
    )
    monkeypatch.setattr(
        engine._adapter,
        "execute_df",
        lambda *_: (_ for _ in ()).throw(
            AssertionError("oversized DataFrame must not be materialized")
        ),
    )
    with pytest.raises(ValueError, match="E304"):
        engine.execute("SELECT txn_id FROM bounded;")


def test_temporal_replay_preserves_roaring_membership_and_order():
    from pyroaring import BitMap64

    now = datetime.now(timezone.utc)
    history = MembershipHistoryStore()
    history.append(
        [
            MembershipChange(
                context_id="ctx",
                entity_id=3,
                change_type="added",
                recorded_at=now + timedelta(seconds=2),
                effective_at=now + timedelta(seconds=2),
                context_version=2,
            ),
            MembershipChange(
                context_id="ctx",
                entity_id=1,
                change_type="removed",
                recorded_at=now + timedelta(seconds=1),
                effective_at=now + timedelta(seconds=1),
                context_version=2,
            ),
        ]
    )
    at_members, _ = history.state_at(
        "ctx",
        now + timedelta(seconds=2),
        anchor_members=BitMap64([1, 2]),
        anchor_time=now,
    )
    between_members, _ = history.state_between(
        "ctx",
        now,
        now + timedelta(seconds=2),
        anchor_members=BitMap64([1, 2]),
        anchor_time=now,
    )
    assert isinstance(at_members, BitMap64)
    assert set(at_members) == {2, 3}
    assert isinstance(between_members, BitMap64)
    assert set(between_members) == {1, 2, 3}
    assert [
        event.entity_id
        for event in history.iter_events_between("ctx")
    ] == [1, 3]
