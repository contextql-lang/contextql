"""Mock Jira REMOTE provider.

Reference implementation of :class:`~contextql.providers.RemoteProvider`
that simulates a Jira-like issue tracker data source.
"""

from __future__ import annotations

import random
from typing import Any

from contextql.providers.base import RemoteResult


class JiraRemoteProvider:
    """Simulated Jira issue tracker data source.

    Generates deterministic issue data for demo and testing purposes.
    Can also wrap a static list of rows or a pandas DataFrame.

    Args:
        rows: Static row data.  If ``None``, generates synthetic issues.
        seed: Random seed for reproducible synthetic data.
        num_issues: Number of synthetic issues to generate (default 240).

    Example::

        provider = JiraRemoteProvider()  # 240 synthetic issues
        engine.register_remote_provider("jira", provider)
    """

    def __init__(
        self,
        rows: list[dict] | Any | None = None,
        seed: int = 42,
        num_issues: int = 240,
    ) -> None:
        self._static_rows = rows
        self._seed = seed
        self._num_issues = num_issues

    def query(
        self,
        resource: str,
        filters: dict,
        columns: list[str],
        limit: int | None = None,
    ) -> RemoteResult:
        if self._static_rows is not None:
            rows = (
                self._static_rows
                if isinstance(self._static_rows, list)
                else self._static_rows
            )
            if limit is not None:
                rows = rows[:limit]
            return RemoteResult(rows=rows)

        rng = random.Random(self._seed)
        statuses = ["OPEN", "IN_PROGRESS", "CLOSED", "REOPENED"]
        priorities = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        teams = ["platform", "finops", "vendor-risk", "fulfillment"]

        rows = [
            {
                "issue_id": i,
                "status": rng.choice(statuses),
                "priority": rng.choice(priorities),
                "assigned_team": rng.choice(teams),
                "story_points": rng.randint(1, 13),
            }
            for i in range(1, self._num_issues + 1)
        ]

        if limit is not None:
            rows = rows[:limit]

        return RemoteResult(rows=rows)
