"""Central snapshot compatibility checks used by every execution path."""
from __future__ import annotations

from dataclasses import dataclass

from .membership import MembershipSnapshot
from .semantic import ContextCatalogEntry


@dataclass(frozen=True)
class ResolvedSnapshot:
    entry: ContextCatalogEntry
    snapshot: MembershipSnapshot
    membership_key: str


def resolve_snapshot(
    entry: ContextCatalogEntry,
    membership,
    *,
    version: int | None = None,
) -> ResolvedSnapshot:
    """Resolve one compatible immutable snapshot.

    Ordinary native queries must follow the catalog's explicit current pointer.
    Connector-backed embedded contexts retain compatibility with the public
    ``engine.membership.put_snapshot`` API until a durable synchronizer supplies
    the pointer transactionally.
    """
    membership_key = entry.context_id or entry.qualified_name
    requested_version = version
    if requested_version is None:
        requested_version = entry.current_snapshot_version
        if requested_version is None and entry.source_kind != "connector":
            raise ValueError(
                f"[E200] context '{entry.qualified_name}' is materialized but "
                "has no current compatible snapshot; run REFRESH CONTEXT "
                f"{entry.qualified_name}."
            )

    snapshot = membership.get_snapshot(membership_key, requested_version)
    if snapshot is None:
        raise ValueError(
            f"[E200] context '{entry.qualified_name}' is materialized but "
            "has no current compatible snapshot; run REFRESH CONTEXT "
            f"{entry.qualified_name}."
        )
    if requested_version is not None and snapshot.version != requested_version:
        raise ValueError(
            f"[E201] snapshot version mismatch for context "
            f"'{entry.qualified_name}'."
        )
    if (
        version is None
        and
        entry.definition_hash is not None
        and snapshot.definition_hash != entry.definition_hash
    ):
        raise ValueError(
            f"[E200] context '{entry.qualified_name}' current snapshot was "
            "built for a different definition; run REFRESH CONTEXT "
            f"{entry.qualified_name}."
        )
    if snapshot.state != "current" and version is None:
        raise ValueError(
            f"[E201] snapshot {snapshot.version} for context "
            f"'{entry.qualified_name}' is not current."
        )
    return ResolvedSnapshot(
        entry=entry,
        snapshot=snapshot,
        membership_key=membership_key,
    )
