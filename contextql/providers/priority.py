"""Priority scoring MCP provider.

Reference implementation that assigns priority scores to all entities
in a dataset, useful for ranking queries.
"""

from __future__ import annotations

from typing import Any, Callable

from contextql.providers.base import MCPResult


class PriorityMCP:
    """Universal priority scoring provider.

    Assigns a score to every entity — unlike :class:`FraudDetectionMCP`
    which filters by threshold, ``PriorityMCP`` scores all entities (or a
    provided subset) for ranking via ``ORDER BY CONTEXT``.

    Args:
        scores: Dict mapping entity_id → priority_score, or a callable.
            If ``None``, uses a built-in deterministic generator.
        normalize: If ``True``, normalise scores to ``[0.0, 1.0]``.

    Example::

        scores = {1: 100.0, 2: 50.0, 3: 200.0}
        provider = PriorityMCP(scores=scores, normalize=True)
        engine.register_mcp_provider("priority", provider)
    """

    def __init__(
        self,
        scores: dict[Any, float] | Callable[[str], dict[Any, float]] | None = None,
        normalize: bool = True,
    ) -> None:
        self._scores = scores
        self._normalize = normalize

    def resolve(
        self,
        entity_type: str,
        params: dict,
        limit: int | None = None,
    ) -> MCPResult:
        if callable(self._scores):
            score_map = self._scores(entity_type)
        elif self._scores is not None:
            score_map = dict(self._scores)
        else:
            score_map = self._default_scores(entity_type)

        if self._normalize and score_map:
            max_val = max(score_map.values()) or 1.0
            score_map = {k: v / max_val for k, v in score_map.items()}

        sorted_items = sorted(score_map.items(), key=lambda x: -x[1])
        if limit is not None:
            sorted_items = sorted_items[:limit]

        entity_ids = [eid for eid, _ in sorted_items]
        scores = [s for _, s in sorted_items]
        return MCPResult(entity_type=entity_type, entity_ids=entity_ids, scores=scores)

    @staticmethod
    def _default_scores(entity_type: str) -> dict[int, float]:
        """Generate deterministic demo scores for 240 entities."""
        return {i: float(250 + ((i * 137) % 24000)) for i in range(1, 241)}
