"""Roaring Bitmap membership store (plan section 7.1, DECISIONS.md CS-3).

Optional implementation behind the ``[roaring]`` extra (``pyroaring``).
Membership is held in 64-bit Roaring Bitmaps supporting non-negative integer
entity IDs; behavior is identical to ``SetMembershipStore`` (verified by the
shared parametrized test suite).
"""
from __future__ import annotations

from typing import Iterable, Sequence, Set

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

    def serialized_size(self, context_id: str) -> int:
        """Serialized byte size of the current membership bitmap."""
        return len(self._bitmap(context_id).serialize())

    def _bitmap(self, context_id: str):
        snapshot = self._require_snapshot(context_id, None)
        return self._members[(context_id, snapshot.version)]
