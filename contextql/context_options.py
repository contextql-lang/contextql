"""Context WITH-option validation and resolution (SPEC.md section 6).

Validates the standardized options of a lowered ``ContextDefinitionModel``
(E150-E158, E160) and resolves them into ``MaterializationSettings``.
"""
from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, List, Optional

from .errors import Severity

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from .semantic import (
        ContextDefinitionModel,
        MaterializationSettings,
        SemanticDiagnostic,
    )

BOOLEAN_OPTIONS = frozenset({"materialized", "history"})
DURATION_OPTIONS = frozenset(
    {"refresh_interval", "stale_after", "history_retention"}
)
ENUM_OPTIONS = {
    "storage": frozenset({"set", "roaring", "auto"}),
    "refresh_mode": frozenset({"manual", "scheduled", "incremental"}),
}
IDENTIFIER_OPTIONS = frozenset({"source_watermark"})
KNOWN_OPTIONS = (
    BOOLEAN_OPTIONS
    | DURATION_OPTIONS
    | frozenset(ENUM_OPTIONS)
    | IDENTIFIER_OPTIONS
)

_DURATION_RE = re.compile(
    r"^\s*(\d+)\s+(second|minute|hour|day)s?\s*$", re.IGNORECASE
)
_DURATION_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
}

INTEGER_KEY_TYPES = frozenset(
    {"INT", "INT8", "INT16", "INT32", "INT64", "INTEGER", "BIGINT", "SMALLINT"}
)


def parse_duration_seconds(value: Any) -> Optional[int]:
    """Parse a SPEC section 6 duration string; ``None`` when malformed."""
    if not isinstance(value, str):
        return None
    match = _DURATION_RE.match(value)
    if not match:
        return None
    quantity = int(match.group(1))
    if quantity <= 0:
        return None
    return quantity * _DURATION_SECONDS[match.group(2).lower()]


def validate_context_options(ctx: "ContextDefinitionModel") -> List["SemanticDiagnostic"]:
    """Validate WITH options against SPEC.md section 6 rules."""
    from .semantic import SemanticDiagnostic

    diags: List[SemanticDiagnostic] = []

    def err(code: str, message: str, hint: Optional[str] = None) -> None:
        diags.append(
            SemanticDiagnostic(
                code=code, severity=Severity.ERROR, message=message, hint=hint
            )
        )

    options = ctx.options

    for name in sorted(options):
        if name not in KNOWN_OPTIONS:
            err(
                "E150",
                f"unknown context option '{name}' on context '{ctx.name}'.",
                hint=f"Known options: {', '.join(sorted(KNOWN_OPTIONS))}.",
            )

    for name in ctx.duplicate_options:
        err(
            "E151",
            f"context option '{name}' specified more than once on "
            f"context '{ctx.name}'.",
        )

    for name in sorted(BOOLEAN_OPTIONS & options.keys()):
        if not isinstance(options[name], bool):
            err(
                "E160",
                f"invalid value {options[name]!r} for context option "
                f"'{name}'; expected TRUE or FALSE.",
            )

    for name, allowed in ENUM_OPTIONS.items():
        if name in options and options[name] not in allowed:
            err(
                "E160",
                f"invalid value {options[name]!r} for context option "
                f"'{name}'; expected one of {', '.join(sorted(allowed))}.",
            )

    durations: dict[str, int] = {}
    for name in sorted(DURATION_OPTIONS & options.keys()):
        seconds = parse_duration_seconds(options[name])
        if seconds is None:
            err(
                "E152",
                f"invalid duration {options[name]!r} for context option "
                f"'{name}'.",
                hint="Use '<positive integer> <second|minute|hour|day>[s]'.",
            )
        else:
            durations[name] = seconds

    materialized = options.get("materialized", False)
    refresh_mode = options.get("refresh_mode", "manual")
    history = options.get("history", False)

    if "refresh_interval" in options and refresh_mode != "scheduled":
        err(
            "E153",
            "refresh_interval requires refresh_mode = 'scheduled' "
            f"(context '{ctx.name}' has refresh_mode = '{refresh_mode}').",
        )
    if refresh_mode == "incremental" and "source_watermark" not in options:
        err(
            "E154",
            f"refresh_mode = 'incremental' on context '{ctx.name}' "
            "requires source_watermark.",
        )
    elif refresh_mode == "incremental" and ctx.definition_sql is not None:
        err(
            "E161",
            f"incremental refresh is unsupported for native SQL context "
            f"'{ctx.name}'; arbitrary SQL cannot infer removals safely.",
            hint=(
                "Use refresh_mode = 'manual' or 'scheduled', or supply "
                "membership through a connector change feed."
            ),
        )
    if "history_retention" in options and history is not True:
        err(
            "E155",
            f"history_retention on context '{ctx.name}' requires "
            "history = TRUE.",
        )
    if refresh_mode in ("scheduled", "incremental") and materialized is not True:
        err(
            "E156",
            f"refresh_mode = '{refresh_mode}' on context '{ctx.name}' "
            "requires materialized = TRUE.",
        )
    if (
        options.get("storage") == "roaring"
        and ctx.entity_key_type is not None
        and ctx.entity_key_type.upper() not in INTEGER_KEY_TYPES
    ):
        err(
            "E157",
            f"storage = 'roaring' on context '{ctx.name}' requires an "
            f"integer entity key; '{ctx.entity_key_name}' is "
            f"{ctx.entity_key_type}.",
        )
    if (
        "stale_after" in durations
        and "refresh_interval" in durations
        and durations["stale_after"] < durations["refresh_interval"]
    ):
        err(
            "E158",
            f"stale_after must not be less than refresh_interval on "
            f"context '{ctx.name}'.",
        )

    return diags


def resolve_materialization(ctx: "ContextDefinitionModel") -> "MaterializationSettings":
    """Resolve validated options into settings with SPEC defaults."""
    from .semantic import MaterializationSettings

    options = ctx.options
    return MaterializationSettings(
        materialized=bool(options.get("materialized", False)),
        storage=options.get("storage", "auto"),
        refresh_mode=options.get("refresh_mode", "manual"),
        refresh_interval=options.get("refresh_interval"),
        stale_after=options.get("stale_after"),
        history=bool(options.get("history", False)),
        history_retention=options.get("history_retention"),
        source_watermark=options.get("source_watermark"),
    )
