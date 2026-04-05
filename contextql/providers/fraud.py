"""Fraud detection MCP provider.

Reference implementation of :class:`~contextql.providers.MCPProvider` that
scores entities against a configurable threshold.
"""

from __future__ import annotations

from typing import Any, Callable

from contextql.providers.base import MCPResult


class FraudDetectionMCP:
    """Threshold-based fraud scoring provider.

    Flags entities whose score exceeds a threshold.  Can be initialised
    with a static score map or a callable that generates scores on demand.

    Args:
        scores: Dict mapping entity_id → fraud_score (0.0–1.0),
            or a callable ``(entity_type) → dict[Any, float]``.
            If ``None``, uses a built-in deterministic generator.
        threshold: Minimum score to include an entity in the result.
            Can be overridden per-query via ``params["threshold"]``.
        max_score: Normalisation ceiling for scores (default 1.0).

    Example::

        scores = {1: 0.95, 2: 0.20, 3: 0.87, 4: 0.55}
        provider = FraudDetectionMCP(scores=scores, threshold=0.7)
        engine.register_mcp_provider("fraud", provider)
    """

    def __init__(
        self,
        scores: dict[Any, float] | Callable[[str], dict[Any, float]] | None = None,
        threshold: float = 0.5,
        max_score: float = 1.0,
    ) -> None:
        self._scores = scores
        self._threshold = threshold
        self._max_score = max_score

    def resolve(
        self,
        entity_type: str,
        params: dict,
        limit: int | None = None,
    ) -> MCPResult:
        threshold = float(params.get("threshold", self._threshold))

        if callable(self._scores):
            score_map = self._scores(entity_type)
        elif self._scores is not None:
            score_map = dict(self._scores)
        else:
            score_map = self._default_scores(entity_type)

        flagged = {
            eid: min(score / self._max_score, 1.0)
            for eid, score in score_map.items()
            if score >= threshold
        }

        sorted_items = sorted(flagged.items(), key=lambda x: -x[1])
        if limit is not None:
            sorted_items = sorted_items[:limit]

        entity_ids = [eid for eid, _ in sorted_items]
        scores = [s for _, s in sorted_items]
        return MCPResult(entity_type=entity_type, entity_ids=entity_ids, scores=scores)

    @staticmethod
    def _default_scores(entity_type: str) -> dict[int, float]:
        """Generate deterministic demo scores for 240 entities."""
        return {i: (250 + ((i * 137) % 24000)) / 24250.0 for i in range(1, 241)}
