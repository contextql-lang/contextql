"""Membership store abstraction (plan section 7.1, PR 5/6).

Set and Roaring implementations must behave identically; these tests are
parametrized so every implementation runs the same behavioral suite.
Roaring is optional — its parametrization skips when the dependency is
missing.
"""
from datetime import datetime, timezone

import pytest

from contextql.membership import (
    MembershipSnapshot,
    SetMembershipStore,
    make_membership_store,
)


def _stores():
    stores = [pytest.param(SetMembershipStore, id="set")]
    try:
        from contextql.membership_roaring import RoaringMembershipStore
        stores.append(pytest.param(RoaringMembershipStore, id="roaring"))
    except ImportError:
        stores.append(
            pytest.param(
                None,
                id="roaring",
                marks=pytest.mark.skip(reason="roaring extra not installed"),
            )
        )
    return stores


@pytest.fixture(params=_stores())
def store(request):
    return request.param()


NOW = datetime(2026, 7, 23, 12, 0, 0, tzinfo=timezone.utc)


def put(store, context_id="ctx", members=(1, 2, 3), version=None, **kwargs):
    return store.put_snapshot(
        context_id=context_id,
        members=members,
        computed_at=kwargs.pop("computed_at", NOW),
        data_as_of=kwargs.pop("data_as_of", NOW),
        definition_hash=kwargs.pop("definition_hash", "h0"),
        **kwargs,
    )


class TestSnapshots:
    def test_put_returns_versioned_snapshot(self, store):
        snap = put(store)
        assert isinstance(snap, MembershipSnapshot)
        assert snap.version == 1
        assert snap.member_count == 3
        assert snap.state == "current"

    def test_versions_increment_and_previous_closes(self, store):
        put(store, members=[1, 2, 3])
        snap2 = put(store, members=[2, 3, 4])
        assert snap2.version == 2
        prev = store.get_snapshot("ctx", version=1)
        assert prev.state == "superseded"
        assert prev.valid_to is not None
        assert store.get_snapshot("ctx").version == 2

    def test_get_missing_returns_none(self, store):
        assert store.get_snapshot("nope") is None

    def test_contains(self, store):
        put(store, members=[10, 20])
        assert store.contains("ctx", 10) is True
        assert store.contains("ctx", 11) is False

    def test_empty_membership(self, store):
        snap = put(store, members=[])
        assert snap.member_count == 0
        assert store.contains("ctx", 1) is False

    def test_prior_version_remains_queryable(self, store):
        put(store, members=[1])
        put(store, members=[2])
        assert store.members("ctx", version=1) == {1}
        assert store.members("ctx") == {2}

    def test_negative_ids_rejected(self, store):
        with pytest.raises(ValueError):
            put(store, members=[-1, 2])


class TestScores:
    def test_scores_stored_separately_and_joined(self, store):
        put(store, members=[1, 2], scores={1: 0.9, 2: 0.5})
        assert store.scores("ctx") == {1: 0.9, 2: 0.5}

    def test_scores_default_empty(self, store):
        put(store, members=[1])
        assert store.scores("ctx") == {}


class TestDeltas:
    def test_apply_delta_produces_new_version(self, store):
        put(store, members=[1, 2, 3])
        snap = store.apply_delta(
            "ctx",
            additions=[4],
            removals=[1],
            computed_at=NOW,
            data_as_of=NOW,
        )
        assert snap.version == 2
        assert store.members("ctx") == {2, 3, 4}
        # previous version unchanged (immutability)
        assert store.members("ctx", version=1) == {1, 2, 3}

    def test_apply_delta_score_changes(self, store):
        put(store, members=[1, 2], scores={1: 0.2})
        store.apply_delta(
            "ctx",
            additions=[],
            removals=[],
            score_changes={1: 0.8, 2: 0.4},
            computed_at=NOW,
            data_as_of=NOW,
        )
        assert store.scores("ctx") == {1: 0.8, 2: 0.4}

    def test_apply_delta_missing_context_raises(self, store):
        with pytest.raises(ValueError):
            store.apply_delta("nope", additions=[1], removals=[])

    def test_incremental_equals_rebuild(self, store):
        put(store, members=[1, 2, 3, 5, 8])
        store.apply_delta(
            "ctx", additions=[13, 21], removals=[1, 5],
            computed_at=NOW, data_as_of=NOW,
        )
        rebuilt = {2, 3, 8, 13, 21}
        assert store.members("ctx") == rebuilt


class TestAlgebra:
    def test_union(self, store):
        put(store, context_id="a", members=[1, 2])
        put(store, context_id="b", members=[2, 3])
        assert store.union(["a", "b"]) == {1, 2, 3}

    def test_intersect(self, store):
        put(store, context_id="a", members=[1, 2, 3])
        put(store, context_id="b", members=[2, 3, 4])
        assert store.intersect(["a", "b"]) == {2, 3}

    def test_difference(self, store):
        put(store, context_id="a", members=[1, 2, 3])
        put(store, context_id="b", members=[2])
        assert store.difference("a", "b") == {1, 3}

    def test_union_of_single(self, store):
        put(store, context_id="a", members=[7])
        assert store.union(["a"]) == {7}

    def test_algebra_missing_context_raises(self, store):
        with pytest.raises(ValueError):
            store.union(["nope"])


class TestSerialization:
    def test_round_trip(self, store):
        put(store, members=[0, 1, 100, 65536, 2**31])
        payload = store.serialize("ctx")
        assert isinstance(payload, bytes)
        other = type(store)()
        restored = other.deserialize(
            context_id="ctx2",
            payload=payload,
            computed_at=NOW,
            data_as_of=NOW,
            definition_hash="h0",
        )
        assert restored.member_count == 5
        assert other.members("ctx2") == {0, 1, 100, 65536, 2**31}

    def test_sparse_and_dense(self, store):
        dense = list(range(1000))
        sparse = [i * 997 for i in range(1000)]
        put(store, context_id="dense", members=dense)
        put(store, context_id="sparse", members=sparse)
        assert store.members("dense") == set(dense)
        assert store.members("sparse") == set(sparse)


class TestFactory:
    def test_auto_selects_available_store(self):
        store = make_membership_store("set")
        assert isinstance(store, SetMembershipStore)

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError):
            make_membership_store("granite")
