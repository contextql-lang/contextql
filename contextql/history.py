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
        self._aliases: Dict[str, str] = {}

    def register_alias(self, name: str, context_id: str) -> None:
        self._aliases[name.lower()] = context_id

    def unregister_alias(self, name: str) -> None:
        self._aliases.pop(name.lower(), None)

    def _resolve_context_id(self, value: str) -> str:
        return self._aliases.get(value.lower(), value)

    def append(self, events: Iterable[MembershipChange]) -> None:
        self._events.extend(events)

    def events(
        self,
        context_id: str,
        *,
        context_version: Optional[int] = None,
        change_type: Optional[str] = None,
    ) -> List[MembershipChange]:
        context_id = self._resolve_context_id(context_id)
        result = [
            event for event in self._events
            if event.context_id == context_id
            and (context_version is None
                 or event.context_version == context_version)
            and (change_type is None or event.change_type == change_type)
        ]
        return result

    def clear_context(self, context_id: str) -> None:
        context_id = self._resolve_context_id(context_id)
        self._events = [
            event for event in self._events if event.context_id != context_id
        ]

    def discard_version(self, context_id: str, context_version: int) -> None:
        context_id = self._resolve_context_id(context_id)
        self._events = [
            event for event in self._events
            if not (
                event.context_id == context_id
                and event.context_version == context_version
            )
        ]

    def prune_before(self, context_id: str, cutoff: datetime) -> None:
        context_id = self._resolve_context_id(context_id)
        self._events = [
            event for event in self._events
            if event.context_id != context_id
            or event.effective_at >= cutoff
        ]

    def events_between(
        self,
        context_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[MembershipChange]:
        context_id = self._resolve_context_id(context_id)
        events = [
            event for event in self._events
            if event.context_id == context_id
            and (start is None or event.effective_at >= start)
            and (end is None or event.effective_at <= end)
        ]
        return sorted(
            events,
            key=lambda event: (
                event.effective_at,
                event.recorded_at,
                event.context_version,
                event.entity_id,
                event.change_type,
            ),
        )

    def state_at(
        self,
        context_id: str,
        timestamp: datetime,
        *,
        anchor_members=(),
        anchor_scores: Optional[Dict[int, float]] = None,
        anchor_time: Optional[datetime] = None,
    ) -> tuple[Set[int], Dict[int, float]]:
        members = set(anchor_members)
        scores = dict(anchor_scores or {})
        for event in self.events_between(
            context_id, start=anchor_time, end=timestamp
        ):
            if event.change_type == "added":
                members.add(event.entity_id)
                if event.new_score is not None:
                    scores[event.entity_id] = event.new_score
            elif event.change_type == "removed":
                members.discard(event.entity_id)
                scores.pop(event.entity_id, None)
            elif event.change_type == "score_changed":
                if event.entity_id in members and event.new_score is not None:
                    scores[event.entity_id] = event.new_score
        return members, scores

    def state_between(
        self,
        context_id: str,
        start: datetime,
        end: datetime,
        *,
        anchor_members=(),
        anchor_scores: Optional[Dict[int, float]] = None,
        anchor_time: Optional[datetime] = None,
    ) -> tuple[Set[int], Dict[int, float]]:
        members, scores = self.state_at(
            context_id,
            start,
            anchor_members=anchor_members,
            anchor_scores=anchor_scores,
            anchor_time=anchor_time,
        )
        ever_members = set(members)
        max_scores = dict(scores)
        for event in self.events_between(
            context_id, start=start, end=end
        ):
            if event.change_type == "added":
                members.add(event.entity_id)
                ever_members.add(event.entity_id)
                if event.new_score is not None:
                    max_scores[event.entity_id] = max(
                        max_scores.get(event.entity_id, event.new_score),
                        event.new_score,
                    )
            elif event.change_type == "removed":
                members.discard(event.entity_id)
            elif (
                event.change_type == "score_changed"
                and event.entity_id in members
                and event.new_score is not None
            ):
                max_scores[event.entity_id] = max(
                    max_scores.get(event.entity_id, event.new_score),
                    event.new_score,
                )
        return ever_members, {
            entity_id: max_scores.get(entity_id, 1.0)
            for entity_id in ever_members
        }


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
    effective_times: Optional[Dict[int, datetime]] = None,
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
                effective_at=(effective_times or {}).get(
                    entity_id, effective_at
                ),
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
                    effective_at=(effective_times or {}).get(
                        entity_id, effective_at
                    ),
                    context_version=context_version,
                    source=source,
                    previous_score=old_score,
                    new_score=new_score,
                )
            )
    return changes
