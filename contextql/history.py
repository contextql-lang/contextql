"""Shared membership history store (plan section 7.2, DECISIONS.md CS-5).

Membership changes are recorded as an append-oriented event stream — one
event per change, never one row per current member. Entry and exit are
separate events; temporal qualifiers resolve against this history (CS-16).

This module is the in-memory reference implementation. The server persists
the same shape in the ``context_membership_history`` SQLite table.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set


@dataclass(frozen=True)
class MembershipChange:
    """One membership change event."""
    context_id: str
    entity_id: int
    change_type: str  # added | removed | score_changed
    recorded_at: datetime
    effective_at: datetime
    context_version: int
    source: str = "native"
    evidence_ref: Optional[str] = None
    previous_score: Optional[float] = None
    new_score: Optional[float] = None


class MembershipHistoryStore:
    """Append-oriented history store, shared across contexts."""

    def __init__(self) -> None:
        self._events: List[MembershipChange] = []

    def append(self, events: Iterable[MembershipChange]) -> None:
        self._events.extend(events)

    def events(
        self,
        context_id: str,
        *,
        context_version: Optional[int] = None,
        change_type: Optional[str] = None,
    ) -> List[MembershipChange]:
        result = [
            event for event in self._events
            if event.context_id == context_id
            and (context_version is None
                 or event.context_version == context_version)
            and (change_type is None or event.change_type == change_type)
        ]
        return result

    def clear_context(self, context_id: str) -> None:
        self._events = [
            event for event in self._events if event.context_id != context_id
        ]


def derive_changes(
    *,
    context_id: str,
    context_version: int,
    recorded_at: datetime,
    effective_at: datetime,
    previous_members: Set[int],
    new_members: Set[int],
    previous_scores: Dict[int, float],
    new_scores: Dict[int, float],
    source: str = "native",
) -> List[MembershipChange]:
    """Diff two membership states into history events (plan section 7.3)."""
    changes: List[MembershipChange] = []
    for entity_id in sorted(new_members - previous_members):
        changes.append(
            MembershipChange(
                context_id=context_id,
                entity_id=entity_id,
                change_type="added",
                recorded_at=recorded_at,
                effective_at=effective_at,
                context_version=context_version,
                source=source,
                new_score=new_scores.get(entity_id),
            )
        )
    for entity_id in sorted(previous_members - new_members):
        changes.append(
            MembershipChange(
                context_id=context_id,
                entity_id=entity_id,
                change_type="removed",
                recorded_at=recorded_at,
                effective_at=effective_at,
                context_version=context_version,
                source=source,
                previous_score=previous_scores.get(entity_id),
            )
        )
    for entity_id in sorted(new_members & previous_members):
        old_score = previous_scores.get(entity_id)
        new_score = new_scores.get(entity_id)
        if old_score != new_score:
            changes.append(
                MembershipChange(
                    context_id=context_id,
                    entity_id=entity_id,
                    change_type="score_changed",
                    recorded_at=recorded_at,
                    effective_at=effective_at,
                    context_version=context_version,
                    source=source,
                    previous_score=old_score,
                    new_score=new_score,
                )
            )
    return changes
