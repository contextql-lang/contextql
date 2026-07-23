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


class ContextMembershipStore(Protocol):
    """Store protocol independent of the membership representation."""

    def put_snapshot(self, context_id: str, members: Iterable[int], *,
                     computed_at: datetime, data_as_of: datetime,
                     definition_hash: Optional[str] = None,
                     source_watermark: Optional[str] = None,
                     scores: Optional[Mapping[int, float]] = None,
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
                     ) -> MembershipSnapshot:
        membership = self._make_membership(members)
        history = self._snapshots.setdefault(context_id, [])
        version = history[-1].version + 1 if history else 1

        if history:
            history[-1] = replace(
                history[-1], state="superseded", valid_to=computed_at
            )

        snapshot = MembershipSnapshot(
            context_id=context_id,
            version=version,
            storage_kind=self.storage_kind,
            member_count=len(membership),
            computed_at=computed_at,
            data_as_of=data_as_of,
            valid_from=computed_at,
            definition_hash=definition_hash,
            source_watermark=source_watermark,
        )
        history.append(snapshot)
        self._members[(context_id, version)] = membership
        self._scores[(context_id, version)] = dict(scores or {})
        return snapshot

    def get_snapshot(self, context_id: str,
                     version: Optional[int] = None,
                     ) -> Optional[MembershipSnapshot]:
        history = self._snapshots.get(context_id)
        if not history:
            return None
        if version is None:
            return history[-1]
        for snapshot in history:
            if snapshot.version == version:
                return snapshot
        return None

    def members(self, context_id: str,
                version: Optional[int] = None) -> Set[int]:
        snapshot = self._require_snapshot(context_id, version)
        return self._to_set(self._members[(context_id, snapshot.version)])

    def scores(self, context_id: str,
               version: Optional[int] = None) -> Dict[int, float]:
        snapshot = self._require_snapshot(context_id, version)
        return dict(self._scores[(context_id, snapshot.version)])

    def contains(self, context_id: str, entity_id: int,
                 version: Optional[int] = None) -> bool:
        snapshot = self.get_snapshot(context_id, version)
        if snapshot is None:
            return False
        return entity_id in self._members[(context_id, snapshot.version)]

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

        return self.put_snapshot(
            context_id,
            membership,
            computed_at=computed_at or current.computed_at,
            data_as_of=data_as_of or current.data_as_of,
            definition_hash=current.definition_hash,
            source_watermark=source_watermark or current.source_watermark,
            scores=new_scores,
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
        membership = self._members[(context_id, snapshot.version)]
        body = self._encode_members(membership)
        header = _MAGIC + struct.pack(
            "<BQ", _FORMAT_VERSION, len(membership)
        )
        return header + body

    def deserialize(self, *, context_id: str, payload: bytes,
                    computed_at: datetime, data_as_of: datetime,
                    definition_hash: Optional[str] = None,
                    source_watermark: Optional[str] = None,
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
        membership = self._decode_members(body, count)
        return self.put_snapshot(
            context_id,
            membership,
            computed_at=computed_at,
            data_as_of=data_as_of,
            definition_hash=definition_hash,
            source_watermark=source_watermark,
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
