"""Persistence boundary for context definitions and runtime snapshots.

The core package owns semantics; deployment packages may inject a durable
repository.  The default embedded engine uses this no-op implementation.
"""
from __future__ import annotations

from typing import Iterable, Protocol

from .history import MembershipChange
from .membership import MembershipSnapshot
from .semantic import ContextCatalogEntry


class ContextCatalogRepository(Protocol):
    def load_contexts(self) -> Iterable[ContextCatalogEntry]: ...

    def save_context(
        self,
        entry: ContextCatalogEntry,
        *,
        raw_ddl: str | None = None,
    ) -> None: ...

    def drop_context(self, entry: ContextCatalogEntry) -> None: ...

    def promote_snapshot(
        self,
        entry: ContextCatalogEntry,
        snapshot: MembershipSnapshot,
        *,
        membership_payload: bytes,
        scores: dict[int, float],
        history_events: Iterable[MembershipChange] = (),
    ) -> None: ...

    def hydrate_runtime(self, membership, history) -> None: ...

    def prune_history(
        self, entry: ContextCatalogEntry, cutoff
    ) -> None: ...


class InMemoryCatalogRepository:
    """No-op repository used by the embedded engine."""

    def load_contexts(self):
        return ()

    def save_context(self, entry, *, raw_ddl=None) -> None:
        return None

    def drop_context(self, entry) -> None:
        return None

    def promote_snapshot(
        self,
        entry,
        snapshot,
        *,
        membership_payload,
        scores,
        history_events=(),
    ) -> None:
        return None

    def hydrate_runtime(self, membership, history) -> None:
        return None

    def prune_history(self, entry, cutoff) -> None:
        return None
