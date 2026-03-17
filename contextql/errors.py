"""ContextQL error code registry.

Error code ranges (from WHITEPAPER Section 35):
  E001-E099  Syntax errors (parser failures, invalid grammar)
  E100-E199  Semantic errors (type mismatches, undefined references)
  E200-E299  Runtime errors (execution failures, timeouts)
  E300-E399  Federation errors (MCP/REMOTE provider failures)
  E400-E499  Lifecycle errors (state violations, dependency failures)

Warning code ranges:
  W001-W099  Parser warnings
  W100-W199  Semantic warnings
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class ErrorCode:
    code: str
    severity: Severity
    name: str
    template: str

    def format(self, **kwargs: object) -> str:
        return self.template.format(**kwargs)


# ── Syntax errors (E001-E099) ───────────────────────────────────────────

E001 = ErrorCode("E001", Severity.ERROR, "SYNTAX_ERROR",
                 "syntax error")
E002 = ErrorCode("E002", Severity.ERROR, "UNEXPECTED_TOKEN",
                 "unexpected token {token!r}")
E003 = ErrorCode("E003", Severity.ERROR, "UNTERMINATED_STRING",
                 "unterminated string literal")
E004 = ErrorCode("E004", Severity.ERROR, "INVALID_NUMBER",
                 "invalid numeric literal {value!r}")

# ── Semantic errors (E100-E199) ─────────────────────────────────────────

E100 = ErrorCode("E100", Severity.ERROR, "UNDEFINED_CONTEXT",
                 "context {name!r} is not defined")
E102 = ErrorCode("E102", Severity.ERROR, "ENTITY_KEY_TYPE_MISMATCH",
                 "entity key type mismatch: context {context!r} has key "
                 "{ctx_key!r} ({ctx_type}) but table {table!r} has key "
                 "{tbl_key!r} ({tbl_type})")
E103 = ErrorCode("E103", Severity.ERROR, "CIRCULAR_DEPENDENCY",
                 "circular dependency detected: {cycle}")
E105 = ErrorCode("E105", Severity.ERROR, "AMBIGUOUS_EVENT_LOG",
                 "multiple event logs match; use USING EVENT LOG to disambiguate")
E107 = ErrorCode("E107", Severity.ERROR, "MISSING_WHERE_CONTEXT",
                 "ORDER BY CONTEXT requires WHERE CONTEXT IN")
E108 = ErrorCode("E108", Severity.ERROR, "CONTEXT_SCORE_SCOPE",
                 "CONTEXT_SCORE() may only appear in queries with WHERE CONTEXT IN")
E109 = ErrorCode("E109", Severity.ERROR, "TEMPORAL_ON_NON_TEMPORAL",
                 "temporal qualifier used on non-temporal context {name!r}")
E110 = ErrorCode("E110", Severity.ERROR, "NEGATIVE_WEIGHT",
                 "context weights must be non-negative")
E111 = ErrorCode("E111", Severity.ERROR, "SCORE_TYPE_ERROR",
                 "score expression must evaluate to a numeric type")
E112 = ErrorCode("E112", Severity.ERROR, "CONTEXT_RETIRED",
                 "context {name!r} is in RETIRED state and cannot be queried")
E113 = ErrorCode("E113", Severity.ERROR, "DUPLICATE_CONTEXT_NAME",
                 "context {name!r} already exists in namespace")
E114 = ErrorCode("E114", Severity.ERROR, "MISSING_ENTITY_KEY",
                 "entity key column {key!r} not found in SELECT list")
E115 = ErrorCode("E115", Severity.ERROR, "UNDEFINED_PARAMETER",
                 "parameter {name!r} referenced but not declared")
E116 = ErrorCode("E116", Severity.ERROR, "DUPLICATE_PARAMETER",
                 "duplicate parameter name {name!r}")
E117 = ErrorCode("E117", Severity.ERROR, "INCOMPATIBLE_COMPOSITE_KEYS",
                 "composite context children must have type-compatible entity keys")
E118 = ErrorCode("E118", Severity.ERROR, "ORDERBY_IN_CONTEXT_DEF",
                 "context definition SELECT must not contain ORDER BY")
E120 = ErrorCode("E120", Severity.ERROR, "MISSING_ENTITY_KEY_DECL",
                 "CREATE CONTEXT {name!r} is missing an ON entity key declaration")
E130 = ErrorCode("E130", Severity.ERROR, "EVENT_LOG_MISSING_SOURCE",
                 "CREATE EVENT LOG {name!r} is missing a FROM source table")
E131 = ErrorCode("E131", Severity.ERROR, "EVENT_LOG_MISSING_CASE_KEY",
                 "CREATE EVENT LOG {name!r} is missing ON <case_column>")
E132 = ErrorCode("E132", Severity.ERROR, "EVENT_LOG_MISSING_ACTIVITY",
                 "CREATE EVENT LOG {name!r} is missing ACTIVITY <column>")
E133 = ErrorCode("E133", Severity.ERROR, "EVENT_LOG_MISSING_TIMESTAMP",
                 "CREATE EVENT LOG {name!r} is missing TIMESTAMP <column>")
E140 = ErrorCode("E140", Severity.ERROR, "PROCESS_MODEL_NO_PATHS",
                 "CREATE PROCESS MODEL {name!r} has no EXPECTED PATH clauses")
E141 = ErrorCode("E141", Severity.ERROR, "UNDEFINED_EVENT_LOG",
                 "undefined event log {log!r} in CREATE PROCESS MODEL {name!r}")

# ── Semantic warnings (W100-W199) ───────────────────────────────────────

W001 = ErrorCode("W001", Severity.WARNING, "WINDOW_WITHOUT_SCORE",
                 "CONTEXT WINDOW applied to contexts with no scores; "
                 "truncation order is by entity key")
W002 = ErrorCode("W002", Severity.WARNING, "MISSING_CONTEXT_ON",
                 "joined query uses CONTEXT without explicit CONTEXT ON binding")
W003 = ErrorCode("W003", Severity.WARNING, "SCORE_OUT_OF_RANGE",
                 "score expression may produce values outside [0.0, 1.0]")
W004 = ErrorCode("W004", Severity.WARNING, "WEIGHT_ZERO",
                 "weight 0.0 means membership-only with no score contribution")

# ── Registry ────────────────────────────────────────────────────────────

_ALL_CODES: dict[str, ErrorCode] = {
    v.code: v
    for k, v in globals().items()
    if isinstance(v, ErrorCode)
}


def get_error(code: str) -> ErrorCode | None:
    return _ALL_CODES.get(code)


def all_errors() -> dict[str, ErrorCode]:
    return dict(_ALL_CODES)
