"""Context membership storage abstraction (plan section 7.1).

Current membership of a materialized context is a versioned, immutable
snapshot containing entity IDs only (DECISIONS.md CS-3). Scores are stored
separately and joined by entity ID (CS-6). Refresh is copy-and-promote:
every change produces a new snapshot version and prior versions stay
queryable (CS-7).

``SetMembershipStore`` is the reference implementation. The optional Roaring
Bitmap implementation lives in ``contextql.membership_roaring`` behind the
``[roaring]`` extra so parser/LSP-only installations carry no bitmap
dependency.
"""
from __future__ import annotations

import struct
import threading
from dataclasses import dataclass, replace
from datetime import datetime
from typing import (
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
)

_MAGIC = b"CQLM"
_FORMAT_VERSION = 1


@dataclass(frozen=True)
class MembershipSnapshot:
    """Immutable snapshot metadata (DECISIONS.md CS-4)."""
    context_id: str
    version: int
    storage_kind: str
    member_count: int
    computed_at: datetime
    data_as_of: datetime
    valid_from: datetime
    valid_to: Optional[datetime] = None
    definition_hash: Optional[str] = None
    source_watermark: Optional[str] = None
    state: str = "current"  # current | superseded


@dataclass(frozen=True)
class StagedMembershipSnapshot:
    """Complete immutable payload that has not been published as current."""

    snapshot: MembershipSnapshot
    membership: object
    scores: Dict[int, float]


class ContextMembershipStore(Protocol):
    """Store protocol independent of the membership representation."""

    def put_snapshot(self, context_id: str, members: Iterable[int], *,
                     computed_at: datetime, data_as_of: datetime,
                     definition_hash: Optional[str] = None,
                     source_watermark: Optional[str] = None,
                     scores: Optional[Mapping[int, float]] = None,
                     storage_kind: Optional[str] = None,
                     ) -> MembershipSnapshot: ...

    def get_snapshot(self, context_id: str,
                     version: Optional[int] = None,
                     ) -> Optional[MembershipSnapshot]: ...

    def apply_delta(self, context_id: str, *, additions: Iterable[int],
                    removals: Iterable[int],
                    score_changes: Optional[Mapping[int, float]] = None,
                    computed_at: Optional[datetime] = None,
                    data_as_of: Optional[datetime] = None,
                    source_watermark: Optional[str] = None,
                    ) -> MembershipSnapshot: ...

    def contains(self, context_id: str, entity_id: int,
                 version: Optional[int] = None) -> bool: ...

    def union(self, context_ids: Sequence[str]) -> Set[int]: ...

    def intersect(self, context_ids: Sequence[str]) -> Set[int]: ...

    def difference(self, context_id: str, other_id: str) -> Set[int]: ...

    def serialize(self, context_id: str,
                  version: Optional[int] = None) -> bytes: ...


class SetMembershipStore:
    """Reference membership store backed by frozen Python sets."""

    storage_kind = "set"

    def __init__(self) -> None:
        self._snapshots: Dict[str, List[MembershipSnapshot]] = {}
        self._members: Dict[Tuple[str, int], frozenset] = {}
        self._scores: Dict[Tuple[str, int], Dict[int, float]] = {}
        self._aliases: Dict[str, str] = {}
        self._lock = threading.RLock()

    def register_alias(self, name: str, context_id: str) -> None:
        """Register a compatibility/display-name alias for an immutable ID."""
        with self._lock:
            self._aliases[name.lower()] = context_id

    def unregister_alias(self, name: str) -> None:
        with self._lock:
            self._aliases.pop(name.lower(), None)

    def _resolve_context_id(self, value: str) -> str:
        return self._aliases.get(value.lower(), value)

    # ------------------------------------------------------------------
    # Representation hooks (overridden by the Roaring implementation)
    # ------------------------------------------------------------------

    def _make_membership(self, members: Iterable[int]) -> frozenset:
        validated = frozenset(int(m) for m in members)
        for member in validated:
            if member < 0:
                raise ValueError(
                    f"Entity IDs must be non-negative integers; got {member}."
                )
        return validated

    def _resolve_storage_kind(self, requested: Optional[str]) -> str:
        kind = (requested or self.storage_kind).lower()
        if kind == "auto":
            try:
                from pyroaring import FrozenBitMap64  # noqa: F401
                return "roaring"
            except ImportError:
                return "set"
        if kind == "roaring":
            try:
                from pyroaring import FrozenBitMap64  # noqa: F401
            except ImportError as exc:
                raise ValueError(
                    "[E162] storage = 'roaring' requires the optional "
                    "PyRoaring backend; install contextql[roaring]."
                ) from exc
            return "roaring"
        if kind == "set":
            return "set"
        raise ValueError(f"Unknown membership storage kind {requested!r}.")

    def _make_membership_for_kind(
        self, members: Iterable[int], storage_kind: str
    ):
        if storage_kind == "set":
            # Call the base implementation explicitly: a Roaring subclass
            # must still honor an explicit per-context `storage = 'set'`.
            return SetMembershipStore._make_membership(self, members)
        from pyroaring import FrozenBitMap64
        def validated():
            for member in members:
                value = int(member)
                if value < 0:
                    raise ValueError(
                        "Entity IDs must be non-negative integers; "
                        f"got {value}."
                    )
                yield value
        return FrozenBitMap64(validated())

    def resolve_storage_kind(self, requested: Optional[str]) -> str:
        return self._resolve_storage_kind(requested)

    def _to_set(self, membership) -> Set[int]:
        return set(membership)

    def _encode_members(self, membership) -> bytes:
        ordered = sorted(membership)
        return struct.pack(f"<{len(ordered)}Q", *ordered)

    def _decode_members(self, payload: bytes, count: int) -> frozenset:
        expected = count * 8
        if len(payload) != expected:
            raise ValueError(
                f"Malformed membership payload: expected {expected} body "
                f"bytes for {count} members, got {len(payload)}."
            )
        return frozenset(struct.unpack(f"<{count}Q", payload))

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------

    def put_snapshot(self, context_id: str, members: Iterable[int], *,
                     computed_at: datetime, data_as_of: datetime,
                     definition_hash: Optional[str] = None,
                     source_watermark: Optional[str] = None,
                     scores: Optional[Mapping[int, float]] = None,
                     storage_kind: Optional[str] = None,
                     ) -> MembershipSnapshot:
        staged = self.stage_snapshot(
            context_id,
            members,
            computed_at=computed_at,
            data_as_of=data_as_of,
            definition_hash=definition_hash,
            source_watermark=source_watermark,
            scores=scores,
            storage_kind=storage_kind,
        )
        return self.commit_snapshot(staged)

    def stage_snapshot(
        self,
        context_id: str,
        members: Iterable[int],
        *,
        computed_at: datetime,
        data_as_of: datetime,
        definition_hash: Optional[str] = None,
        source_watermark: Optional[str] = None,
        scores: Optional[Mapping[int, float]] = None,
        storage_kind: Optional[str] = None,
    ) -> StagedMembershipSnapshot:
        """Build a complete snapshot without changing the current pointer."""
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            resolved_storage = self._resolve_storage_kind(storage_kind)
            membership = self._make_membership_for_kind(
                members, resolved_storage
            )
            history = self._snapshots.setdefault(context_id, [])
            version = history[-1].version + 1 if history else 1

            snapshot = MembershipSnapshot(
                context_id=context_id,
                version=version,
                storage_kind=resolved_storage,
                member_count=len(membership),
                computed_at=computed_at,
                data_as_of=data_as_of,
                valid_from=computed_at,
                definition_hash=definition_hash,
                source_watermark=source_watermark,
            )
            return StagedMembershipSnapshot(
                snapshot=snapshot,
                membership=membership,
                scores=dict(scores or {}),
            )

    def commit_snapshot(
        self, staged: StagedMembershipSnapshot
    ) -> MembershipSnapshot:
        """Atomically publish a previously staged immutable payload."""
        with self._lock:
            snapshot = staged.snapshot
            context_id = self._resolve_context_id(snapshot.context_id)
            history = self._snapshots.setdefault(context_id, [])
            expected_version = history[-1].version + 1 if history else 1
            if snapshot.version != expected_version:
                raise ValueError(
                    "Staged snapshot version is no longer current; "
                    "refreshes for one context must serialize."
                )
            # Install the complete immutable payload before publishing its
            # metadata as the new current snapshot.
            self._members[(context_id, snapshot.version)] = (
                staged.membership
            )
            self._scores[(context_id, snapshot.version)] = dict(
                staged.scores
            )
            if history:
                history[-1] = replace(
                    history[-1],
                    state="superseded",
                    valid_to=snapshot.computed_at,
                )
            history.append(snapshot)
            return snapshot

    def serialize_staged(
        self, staged: StagedMembershipSnapshot
    ) -> bytes:
        """Serialize a staged payload for durable promotion."""
        membership = staged.membership
        if staged.snapshot.storage_kind == "roaring":
            body = membership.serialize()
        else:
            body = SetMembershipStore._encode_members(self, membership)
        return _MAGIC + struct.pack(
            "<BQ", _FORMAT_VERSION, len(membership)
        ) + body

    def get_snapshot(self, context_id: str,
                     version: Optional[int] = None,
                     ) -> Optional[MembershipSnapshot]:
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            history = self._snapshots.get(context_id)
            if not history:
                return None
            if version is None:
                return history[-1]
            for snapshot in history:
                if snapshot.version == version:
                    return snapshot
            return None

    def snapshots(self, context_id: str) -> List[MembershipSnapshot]:
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            return list(self._snapshots.get(context_id, ()))

    def get_snapshot_at(
        self, context_id: str, timestamp: datetime
    ) -> Optional[MembershipSnapshot]:
        candidates = [
            snapshot for snapshot in self.snapshots(context_id)
            if snapshot.data_as_of <= timestamp
        ]
        return candidates[-1] if candidates else None

    def members(self, context_id: str,
                version: Optional[int] = None) -> Set[int]:
        snapshot = self._require_snapshot(context_id, version)
        context_id = self._resolve_context_id(context_id)
        with self._lock:
            return self._to_set(
                self._members[(context_id, snapshot.version)]
            )

    def scores(self, context_id: str,
               version: Optional[int] = None) -> Dict[int, float]:
        snapshot = self._require_snapshot(context_id, version)
        context_id = self._resolve_context_id(context_id)
        with self._lock:
            return dict(self._scores[(context_id, snapshot.version)])

    def membership_object(
        self, context_id: str, version: Optional[int] = None
    ):
        """Return the immutable internal membership representation."""
        snapshot = self._require_snapshot(context_id, version)
        context_id = self._resolve_context_id(context_id)
        with self._lock:
            return self._members[(context_id, snapshot.version)]

    def iter_member_batches(
        self,
        context_id: str,
        version: Optional[int] = None,
        batch_size: int = 65_536,
    ):
        """Yield bounded tuples without creating one complete ID array."""
        import itertools

        membership = self.membership_object(context_id, version)
        iterator = iter(membership)
        while True:
            batch = tuple(itertools.islice(iterator, batch_size))
            if not batch:
                break
            yield batch

    def contains(self, context_id: str, entity_id: int,
                 version: Optional[int] = None) -> bool:
        snapshot = self.get_snapshot(context_id, version)
        if snapshot is None:
            return False
        context_id = self._resolve_context_id(context_id)
        with self._lock:
            return entity_id in self._members[
                (context_id, snapshot.version)
            ]

    def discard_snapshot(self, context_id: str, version: int) -> None:
        """Roll back an unpublished latest snapshot after persistence failure."""
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            history = self._snapshots.get(context_id, [])
            if not history or history[-1].version != version:
                raise ValueError(
                    "Only the latest snapshot can be discarded safely."
                )
            history.pop()
            self._members.pop((context_id, version), None)
            self._scores.pop((context_id, version), None)
            if history:
                history[-1] = replace(
                    history[-1], state="current", valid_to=None
                )
            else:
                self._snapshots.pop(context_id, None)

    def prune_before(self, context_id: str, cutoff: datetime):
        """Retain one anchor snapshot at/before cutoff and everything later."""
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            snapshots = self._snapshots.get(context_id, [])
            eligible = [
                snapshot for snapshot in snapshots
                if snapshot.data_as_of <= cutoff
            ]
            if not eligible:
                return None
            anchor = eligible[-1]
            retained = [
                snapshot for snapshot in snapshots
                if snapshot.version >= anchor.version
            ]
            removed_versions = {
                snapshot.version for snapshot in snapshots
                if snapshot.version < anchor.version
            }
            for version in removed_versions:
                self._members.pop((context_id, version), None)
                self._scores.pop((context_id, version), None)
            self._snapshots[context_id] = retained
            return anchor

    # ------------------------------------------------------------------
    # Deltas
    # ------------------------------------------------------------------

    def apply_delta(self, context_id: str, *, additions: Iterable[int],
                    removals: Iterable[int],
                    score_changes: Optional[Mapping[int, float]] = None,
                    computed_at: Optional[datetime] = None,
                    data_as_of: Optional[datetime] = None,
                    source_watermark: Optional[str] = None,
                    ) -> MembershipSnapshot:
        staged = self.stage_delta(
            context_id,
            additions=additions,
            removals=removals,
            score_changes=score_changes,
            computed_at=computed_at,
            data_as_of=data_as_of,
            source_watermark=source_watermark,
        )
        return self.commit_snapshot(staged)

    def stage_delta(
        self,
        context_id: str,
        *,
        additions: Iterable[int],
        removals: Iterable[int],
        score_changes: Optional[Mapping[int, float]] = None,
        computed_at: Optional[datetime] = None,
        data_as_of: Optional[datetime] = None,
        source_watermark: Optional[str] = None,
    ) -> StagedMembershipSnapshot:
        with self._lock:
            context_id = self._resolve_context_id(context_id)
            current = self._require_snapshot(context_id, None)
            membership = self._to_set(
                self._members[(context_id, current.version)]
            )
            membership.update(int(a) for a in additions)
            membership.difference_update(int(r) for r in removals)

            new_scores = dict(self._scores[(context_id, current.version)])
            for entity_id, score in (score_changes or {}).items():
                new_scores[int(entity_id)] = float(score)
            new_scores = {
                entity_id: score
                for entity_id, score in new_scores.items()
                if entity_id in membership
            }

            return self.stage_snapshot(
                context_id,
                membership,
                computed_at=computed_at or current.computed_at,
                data_as_of=data_as_of or current.data_as_of,
                definition_hash=current.definition_hash,
                source_watermark=(
                    source_watermark or current.source_watermark
                ),
                scores=new_scores,
                storage_kind=current.storage_kind,
            )

    # ------------------------------------------------------------------
    # Algebra
    # ------------------------------------------------------------------

    def union(self, context_ids: Sequence[str]) -> Set[int]:
        result: Set[int] = set()
        for context_id in context_ids:
            result |= self.members(context_id)
        return result

    def intersect(self, context_ids: Sequence[str]) -> Set[int]:
        if not context_ids:
            return set()
        result = self.members(context_ids[0])
        for context_id in context_ids[1:]:
            result &= self.members(context_id)
        return result

    def difference(self, context_id: str, other_id: str) -> Set[int]:
        return self.members(context_id) - self.members(other_id)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def serialize(self, context_id: str,
                  version: Optional[int] = None) -> bytes:
        snapshot = self._require_snapshot(context_id, version)
        context_id = self._resolve_context_id(context_id)
        membership = self._members[(context_id, snapshot.version)]
        if snapshot.storage_kind == "roaring":
            body = membership.serialize()
        else:
            body = SetMembershipStore._encode_members(self, membership)
        header = _MAGIC + struct.pack(
            "<BQ", _FORMAT_VERSION, len(membership)
        )
        return header + body

    def deserialize(self, *, context_id: str, payload: bytes,
                    computed_at: datetime, data_as_of: datetime,
                    definition_hash: Optional[str] = None,
                    source_watermark: Optional[str] = None,
                    scores: Optional[Mapping[int, float]] = None,
                    storage_kind: Optional[str] = None,
                    ) -> MembershipSnapshot:
        if len(payload) < len(_MAGIC) + 9 or not payload.startswith(_MAGIC):
            raise ValueError("Malformed membership payload: bad header.")
        offset = len(_MAGIC)
        fmt_version, count = struct.unpack_from("<BQ", payload, offset)
        if fmt_version != _FORMAT_VERSION:
            raise ValueError(
                f"Unsupported membership payload version {fmt_version}."
            )
        body = payload[offset + 9:]
        resolved_storage = self._resolve_storage_kind(storage_kind)
        if resolved_storage == "roaring":
            from pyroaring import BitMap64
            membership = BitMap64.deserialize(body)
            if len(membership) != count:
                raise ValueError(
                    "Malformed bitmap payload: header cardinality does not "
                    "match decoded membership."
                )
        else:
            membership = SetMembershipStore._decode_members(
                self, body, count
            )
        return self.put_snapshot(
            context_id,
            membership,
            computed_at=computed_at,
            data_as_of=data_as_of,
            definition_hash=definition_hash,
            source_watermark=source_watermark,
            scores=scores,
            storage_kind=resolved_storage,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_snapshot(self, context_id: str,
                          version: Optional[int]) -> MembershipSnapshot:
        snapshot = self.get_snapshot(context_id, version)
        if snapshot is None:
            raise ValueError(
                f"No membership snapshot for context '{context_id}'"
                + (f" version {version}." if version is not None else ".")
            )
        return snapshot


def make_membership_store(kind: str = "auto") -> ContextMembershipStore:
    """Create a membership store: 'set', 'roaring', or 'auto' (CS-14)."""
    normalized = kind.lower()
    if normalized == "set":
        return SetMembershipStore()
    if normalized == "roaring":
        from .membership_roaring import RoaringMembershipStore
        return RoaringMembershipStore()
    if normalized == "auto":
        try:
            from .membership_roaring import RoaringMembershipStore
            return RoaringMembershipStore()
        except ImportError:
            return SetMembershipStore()
    raise ValueError(
        f"Unknown membership store kind {kind!r}; "
        "expected 'set', 'roaring', or 'auto'."
    )
