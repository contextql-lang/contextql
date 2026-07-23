"""Roaring Bitmap membership store (plan section 7.1, DECISIONS.md CS-3).

Optional implementation behind the ``[roaring]`` extra (``pyroaring``).
Membership is held in 64-bit Roaring Bitmaps supporting non-negative integer
entity IDs; behavior is identical to ``SetMembershipStore`` (verified by the
shared parametrized test suite).
"""
from __future__ import annotations

from typing import Iterable, Mapping, Sequence, Set

try:
    from pyroaring import BitMap64, FrozenBitMap64
except ImportError as exc:  # pragma: no cover - exercised without the extra
    raise ImportError(
        "RoaringMembershipStore requires the 'pyroaring' package; "
        "install with: pip install 'contextql[roaring]'"
    ) from exc

from .membership import SetMembershipStore


class RoaringMembershipStore(SetMembershipStore):
    """Membership store backed by 64-bit Roaring Bitmaps."""

    storage_kind = "roaring"

    # ------------------------------------------------------------------
    # Representation hooks
    # ------------------------------------------------------------------

    def _make_membership(self, members: Iterable[int]) -> FrozenBitMap64:
        validated = []
        for member in members:
            value = int(member)
            if value < 0:
                raise ValueError(
                    f"Entity IDs must be non-negative integers; got {value}."
                )
            validated.append(value)
        return FrozenBitMap64(validated)

    def _to_set(self, membership) -> Set[int]:
        return set(membership)

    def _encode_members(self, membership) -> bytes:
        return membership.serialize()

    def _decode_members(self, payload: bytes, count: int) -> FrozenBitMap64:
        bitmap = BitMap64.deserialize(payload)
        if len(bitmap) != count:
            raise ValueError(
                f"Malformed bitmap payload: header count {count} does not "
                f"match decoded cardinality {len(bitmap)}."
            )
        return FrozenBitMap64(bitmap)

    # ------------------------------------------------------------------
    # Bitmap-native algebra (no Python-set expansion)
    # ------------------------------------------------------------------

    def compose(self, *, union_of: Sequence[str] = (),
                intersect_of: Sequence[str] = (),
                subtract: Sequence[str] = ()) -> BitMap64:
        """Compose memberships bitmap-natively and return the result bitmap.

        The executor uses this path so large memberships are never expanded
        into Python sets (CS-11).
        """
        all_ids = list(union_of) + list(intersect_of) + list(subtract)
        if any(
            self._require_snapshot(context_id, None).storage_kind != "roaring"
            for context_id in all_ids
        ):
            if intersect_of:
                result_set = self.intersect(intersect_of)
            else:
                result_set = self.union(union_of)
            for context_id in subtract:
                result_set -= self.members(context_id)
            return result_set

        result: BitMap64 | None = None
        for context_id in union_of:
            bitmap = self._bitmap(context_id)
            result = BitMap64(bitmap) if result is None else result | bitmap
        for context_id in intersect_of:
            bitmap = self._bitmap(context_id)
            result = BitMap64(bitmap) if result is None else result & bitmap
        if result is None:
            result = BitMap64()
        for context_id in subtract:
            result = result - self._bitmap(context_id)
        return result

    def apply_delta(self, context_id: str, *, additions: Iterable[int],
                    removals: Iterable[int],
                    score_changes: Mapping[int, float] | None = None,
                    computed_at=None, data_as_of=None,
                    source_watermark: str | None = None):
        """Apply Roaring deltas without expanding current membership to set."""
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

    def stage_delta(self, context_id: str, *, additions: Iterable[int],
                    removals: Iterable[int],
                    score_changes: Mapping[int, float] | None = None,
                    computed_at=None, data_as_of=None,
                    source_watermark: str | None = None):
        """Build a Roaring delta snapshot without publishing it."""
        context_id = self._resolve_context_id(context_id)
        with self._lock:
            current = self._require_snapshot(context_id, None)
            if current.storage_kind != "roaring":
                return super().stage_delta(
                    context_id,
                    additions=additions,
                    removals=removals,
                    score_changes=score_changes,
                    computed_at=computed_at,
                    data_as_of=data_as_of,
                    source_watermark=source_watermark,
                )
            membership = BitMap64(
                self._members[(context_id, current.version)]
            )
            membership |= BitMap64(int(value) for value in additions)
            membership -= BitMap64(int(value) for value in removals)
            new_scores = dict(
                self._scores[(context_id, current.version)]
            )
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
                storage_kind="roaring",
            )

    def serialized_size(self, context_id: str) -> int:
        """Serialized byte size of the current membership bitmap."""
        return len(self._bitmap(context_id).serialize())

    def _bitmap(self, context_id: str):
        context_id = self._resolve_context_id(context_id)
        snapshot = self._require_snapshot(context_id, None)
        return self._members[(context_id, snapshot.version)]
