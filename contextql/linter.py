"""ContextQL semantic analyzer / linter.

Separates lint rules from the parser. Walks the Lark parse tree and
emits diagnostics using the error code registry (errors.py).

Lint rules implemented:
  E100  Undefined context
  E102  Entity key type mismatch
  E103  Circular dependency (in CREATE CONTEXT)
  E107  ORDER BY CONTEXT without WHERE CONTEXT IN
  E108  CONTEXT_SCORE() outside context query
  E109  Temporal qualifier on non-temporal context
  E110  Negative weight
  E118  ORDER BY in context definition SELECT
  W001  CONTEXT WINDOW without scores
  W002  Joined query missing explicit CONTEXT ON
  W004  Weight of zero (membership-only)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Optional

from lark import Token, Tree

from .errors import Severity
from .parser import ContextQLParser, ContextQLSyntaxError


# ── Catalog data classes ────────────────────────────────────────────────


@dataclass(slots=True)
class CatalogContext:
    name: str
    entity_key: str
    entity_key_type: str
    has_score: bool = False
    parameters: list[str] = field(default_factory=list)
    is_temporal: bool = False
    dependencies: list[str] = field(default_factory=list)
    lifecycle_state: str = "materialized"


@dataclass(slots=True)
class CatalogTable:
    name: str
    primary_key: Optional[str] = None
    primary_key_type: Optional[str] = None
    columns: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class CatalogEventLog:
    name: str
    source_table: str
    case_column: str
    activity_column: str
    timestamp_column: str
    resource_column: Optional[str] = None


# ── Diagnostic ──────────────────────────────────────────────────────────


@dataclass(slots=True)
class LintDiagnostic:
    rule_id: str
    severity: str  # error | warning | info
    message: str
    line: int = 1
    column: int = 1
    suggestion: Optional[str] = None


# ── Catalog ─────────────────────────────────────────────────────────────


class Catalog:
    def __init__(self) -> None:
        self.contexts: dict[str, CatalogContext] = {}
        self.tables: dict[str, CatalogTable] = {}
        self.event_logs: dict[str, CatalogEventLog] = {}

    def add_context(self, ctx: CatalogContext) -> None:
        self.contexts[ctx.name.lower()] = ctx

    def add_table(self, table: CatalogTable) -> None:
        self.tables[table.name.lower()] = table

    def add_event_log(self, log: CatalogEventLog) -> None:
        self.event_logs[log.name.lower()] = log

    def get_context(self, name: str) -> Optional[CatalogContext]:
        return self.contexts.get(name.lower())

    def get_table(self, name: str) -> Optional[CatalogTable]:
        return self.tables.get(name.lower())

    def context_names(self) -> list[str]:
        return list(self.contexts.keys())


# ── Linter ──────────────────────────────────────────────────────────────


class ContextQLLinter:
    def __init__(self, catalog: Optional[Catalog] = None) -> None:
        self.catalog = catalog or Catalog()
        self.parser = ContextQLParser()

    def lint(self, text: str) -> list[LintDiagnostic]:
        try:
            tree = self.parser.parse(text)
        except ContextQLSyntaxError as exc:
            return [
                LintDiagnostic(
                    rule_id=exc.detail.code,
                    severity="error",
                    message=exc.detail.message,
                    line=exc.detail.line,
                    column=exc.detail.column,
                    suggestion=(
                        f"Expected one of: {', '.join(exc.detail.expected)}"
                        if exc.detail.expected else None
                    ),
                )
            ]

        diagnostics: list[LintDiagnostic] = []
        diagnostics.extend(self._rule_e100_undefined_context(tree))
        diagnostics.extend(self._rule_e102_entity_key_mismatch(tree))
        diagnostics.extend(self._rule_e103_circular_dependency(tree))
        diagnostics.extend(self._rule_e107_order_by_requires_where(tree))
        diagnostics.extend(self._rule_e108_context_score_scope(tree))
        diagnostics.extend(self._rule_e109_temporal_on_non_temporal(tree))
        diagnostics.extend(self._rule_e110_weight_negative(tree))
        diagnostics.extend(self._rule_e118_orderby_in_context_def(tree))
        diagnostics.extend(self._rule_w001_window_without_score(tree))
        diagnostics.extend(self._rule_w002_missing_context_on_hint(tree))
        diagnostics.extend(self._rule_w004_weight_zero(tree))
        return diagnostics

    # ── Tree helpers ────────────────────────────────────────────────────

    def _iter_subtrees(self, tree: Tree, data: str) -> Iterable[Tree]:
        for sub in tree.iter_subtrees_topdown():
            if sub.data == data:
                yield sub

    def _qualified_name_text(self, node: Tree) -> str:
        parts: list[str] = []
        for child in node.children:
            if isinstance(child, Token) and child.type in {"IDENTIFIER", "QUOTED_IDENTIFIER"}:
                parts.append(child.value.strip('"'))
            elif isinstance(child, Tree) and child.data == "identifier":
                for tok in child.children:
                    if isinstance(tok, Token) and tok.type in {"IDENTIFIER", "QUOTED_IDENTIFIER"}:
                        parts.append(tok.value.strip('"'))
        return ".".join(parts)

    def _has_where_context(self, tree: Tree) -> bool:
        return any(True for _ in self._iter_subtrees(tree, "context_predicate"))

    def _get_pos(self, node: Tree) -> tuple[int, int]:
        return (getattr(node.meta, "line", 1), getattr(node.meta, "column", 1))

    def _did_you_mean(self, name: str) -> Optional[str]:
        """Simple fuzzy match: suggest catalog contexts with shared prefix."""
        candidates = self.catalog.context_names()
        name_lower = name.lower()
        for c in candidates:
            if c.startswith(name_lower[:3]) or name_lower.startswith(c[:3]):
                return c
        return None

    # ── E100: Undefined context ─────────────────────────────────────────

    def _rule_e100_undefined_context(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for sub in self._iter_subtrees(tree, "context_invocation"):
            if not sub.children:
                continue
            first = sub.children[0]
            if isinstance(first, Tree) and first.data == "qualified_name":
                name = self._qualified_name_text(first)
                if self.catalog.get_context(name) is None:
                    line, col = self._get_pos(sub)
                    suggestion = "Create the context first or correct the name."
                    similar = self._did_you_mean(name)
                    if similar:
                        suggestion = f"Did you mean '{similar}'?"
                    out.append(LintDiagnostic(
                        rule_id="E100",
                        severity="error",
                        message=f"Context '{name}' is not defined.",
                        line=line, column=col,
                        suggestion=suggestion,
                    ))
        return out

    # ── E102: Entity key type mismatch ──────────────────────────────────

    def _rule_e102_entity_key_mismatch(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        # Collect FROM tables and their aliases
        table_map: dict[str, CatalogTable] = {}
        for ref in self._iter_subtrees(tree, "table_ref"):
            children = list(ref.children)
            if not children:
                continue
            first = children[0]
            if isinstance(first, Tree) and first.data == "qualified_name":
                tname = self._qualified_name_text(first)
                tbl = self.catalog.get_table(tname)
                if tbl:
                    table_map[tname] = tbl
                    # Also register under alias if present
                    for ch in children:
                        if isinstance(ch, Tree) and ch.data == "alias":
                            for ac in ch.children:
                                if isinstance(ac, Tree) and ac.data == "identifier":
                                    alias = self._qualified_name_text(ac) if ac.children else ""
                                    if alias:
                                        table_map[alias] = tbl

        if not table_map:
            return []

        # Check context predicates for key compatibility
        for cp in self._iter_subtrees(tree, "context_predicate"):
            # Determine which table this context predicate targets
            binding_table: Optional[CatalogTable] = None
            for ch in cp.children:
                if isinstance(ch, Tree) and ch.data == "context_on_binding":
                    for bch in ch.children:
                        if isinstance(bch, Tree) and bch.data == "identifier":
                            alias = self._qualified_name_text(bch) if bch.children else ""
                            binding_table = table_map.get(alias)

            if binding_table is None and len(table_map) == 1:
                binding_table = next(iter(table_map.values()))

            if binding_table is None or binding_table.primary_key_type is None:
                continue

            # Check each referenced context
            for inv in self._iter_subtrees(cp, "context_invocation"):
                if not inv.children:
                    continue
                first = inv.children[0]
                if isinstance(first, Tree) and first.data == "qualified_name":
                    cname = self._qualified_name_text(first)
                    ctx = self.catalog.get_context(cname)
                    if ctx and ctx.entity_key_type.upper() != binding_table.primary_key_type.upper():
                        line, col = self._get_pos(inv)
                        out.append(LintDiagnostic(
                            rule_id="E102",
                            severity="error",
                            message=(
                                f"Entity key type mismatch: context '{cname}' "
                                f"has key '{ctx.entity_key}' ({ctx.entity_key_type}) "
                                f"but table '{binding_table.name}' has key "
                                f"'{binding_table.primary_key}' ({binding_table.primary_key_type})."
                            ),
                            line=line, column=col,
                            suggestion="Use CONTEXT ON to bind to the correct table.",
                        ))
        return out

    # ── E103: Circular dependency ───────────────────────────────────────

    def _rule_e103_circular_dependency(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for stmt in self._iter_subtrees(tree, "create_context_stmt"):
            children = list(stmt.children)
            # Find context name
            ctx_name = None
            for ch in children:
                if isinstance(ch, Tree) and ch.data == "qualified_name":
                    ctx_name = self._qualified_name_text(ch)
                    break
            if not ctx_name:
                continue

            # Find all context references in the body
            for inv in self._iter_subtrees(stmt, "context_invocation"):
                if not inv.children:
                    continue
                first = inv.children[0]
                if isinstance(first, Tree) and first.data == "qualified_name":
                    ref_name = self._qualified_name_text(first)
                    if ref_name.lower() == ctx_name.lower():
                        line, col = self._get_pos(inv)
                        out.append(LintDiagnostic(
                            rule_id="E103",
                            severity="error",
                            message=f"Circular dependency: context '{ctx_name}' references itself.",
                            line=line, column=col,
                        ))
                    # Check transitive deps via catalog
                    elif self.catalog.get_context(ref_name):
                        dep_ctx = self.catalog.get_context(ref_name)
                        if dep_ctx and ctx_name.lower() in [d.lower() for d in dep_ctx.dependencies]:
                            line, col = self._get_pos(inv)
                            out.append(LintDiagnostic(
                                rule_id="E103",
                                severity="error",
                                message=f"Circular dependency: '{ctx_name}' -> '{ref_name}' -> '{ctx_name}'.",
                                line=line, column=col,
                            ))
        return out

    # ── E107: ORDER BY CONTEXT without WHERE CONTEXT IN ─────────────────

    def _rule_e107_order_by_requires_where(self, tree: Tree) -> list[LintDiagnostic]:
        has_context_order = any(
            isinstance(sub, Tree) and sub.data == "order_item"
            and sub.children and isinstance(sub.children[0], Token)
            and sub.children[0].type == "CONTEXT"
            for sub in tree.iter_subtrees_topdown()
        )
        if has_context_order and not self._has_where_context(tree):
            return [LintDiagnostic(
                rule_id="E107",
                severity="error",
                message="ORDER BY CONTEXT requires WHERE CONTEXT IN.",
                suggestion="Add WHERE CONTEXT IN (...) or use CONTEXT_SCORE() for standalone ranking.",
            )]
        return []

    # ── E108: CONTEXT_SCORE() outside context query ─────────────────────

    def _rule_e108_context_score_scope(self, tree: Tree) -> list[LintDiagnostic]:
        has_score_call = any(True for _ in self._iter_subtrees(tree, "context_score_call"))
        # Also check function_call nodes for CONTEXT_SCORE (Earley ambiguity)
        if not has_score_call:
            has_score_call = self._has_function_named(tree, "CONTEXT_SCORE")
        if has_score_call and not self._has_where_context(tree):
            return [LintDiagnostic(
                rule_id="E108",
                severity="error",
                message="CONTEXT_SCORE() may only appear in queries with WHERE CONTEXT IN.",
                suggestion="Add WHERE CONTEXT IN (...) or remove CONTEXT_SCORE().",
            )]
        return []

    # ── E109: Temporal qualifier on non-temporal context ─────────────────

    def _rule_e109_temporal_on_non_temporal(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for sub in self._iter_subtrees(tree, "context_ref"):
            inv = None
            temporal_seen = False
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "context_invocation":
                    inv = child
                elif isinstance(child, Tree) and child.data == "temporal_qualifier":
                    temporal_seen = True
            if inv is None or not temporal_seen:
                continue
            first = inv.children[0] if inv.children else None
            if isinstance(first, Tree) and first.data == "qualified_name":
                name = self._qualified_name_text(first)
                ctx = self.catalog.get_context(name)
                if ctx and not ctx.is_temporal:
                    line, col = self._get_pos(sub)
                    out.append(LintDiagnostic(
                        rule_id="E109",
                        severity="error",
                        message=f"Temporal qualifier used on non-temporal context '{name}'.",
                        line=line, column=col,
                        suggestion="Declare the context with TEMPORAL (...) or remove AT/BETWEEN.",
                    ))
        return out

    # ── E110: Negative weight ───────────────────────────────────────────

    def _rule_e110_weight_negative(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for sub in self._iter_subtrees(tree, "weight_clause"):
            value = self._extract_weight_value(sub)
            if value is not None and value < 0:
                line, col = self._get_pos(sub)
                out.append(LintDiagnostic(
                    rule_id="E110",
                    severity="error",
                    message="Context weights must be non-negative.",
                    line=line, column=col,
                ))
        return out

    # ── E118: ORDER BY in context definition ────────────────────────────

    def _rule_e118_orderby_in_context_def(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for stmt in self._iter_subtrees(tree, "create_context_stmt"):
            # Look for order_by_clause inside the context's defining SELECT
            for sel in self._iter_subtrees(stmt, "select_stmt"):
                for obc in self._iter_subtrees(sel, "order_by_clause"):
                    line, col = self._get_pos(obc)
                    out.append(LintDiagnostic(
                        rule_id="E118",
                        severity="error",
                        message="Context definition SELECT must not contain ORDER BY.",
                        line=line, column=col,
                        suggestion="Remove ORDER BY from the context definition; "
                                   "ordering is applied at query time.",
                    ))
        return out

    # ── W001: CONTEXT WINDOW without scores ─────────────────────────────

    def _rule_w001_window_without_score(self, tree: Tree) -> list[LintDiagnostic]:
        has_window = any(True for _ in self._iter_subtrees(tree, "context_window_clause"))
        if not has_window:
            return []
        referenced: list[CatalogContext] = []
        for sub in self._iter_subtrees(tree, "context_invocation"):
            first = sub.children[0] if sub.children else None
            if isinstance(first, Tree) and first.data == "qualified_name":
                name = self._qualified_name_text(first)
                ctx = self.catalog.get_context(name)
                if ctx:
                    referenced.append(ctx)
        if referenced and not any(ctx.has_score for ctx in referenced):
            return [LintDiagnostic(
                rule_id="W001",
                severity="warning",
                message="CONTEXT WINDOW applied to contexts with no scores.",
                suggestion="Add scoring to at least one context or accept entity-key truncation order.",
            )]
        return []

    # ── W002: Joined query without CONTEXT ON ───────────────────────────

    def _rule_w002_missing_context_on_hint(self, tree: Tree) -> list[LintDiagnostic]:
        has_join = any(True for _ in self._iter_subtrees(tree, "join_clause"))
        if not has_join:
            return []
        for sub in self._iter_subtrees(tree, "context_predicate"):
            has_on = any(
                isinstance(ch, Tree) and ch.data == "context_on_binding"
                for ch in sub.children
            )
            if not has_on:
                return [LintDiagnostic(
                    rule_id="W002",
                    severity="info",
                    message="Joined query uses CONTEXT without explicit CONTEXT ON binding.",
                    suggestion="Use CONTEXT ON table_alias IN (...) for clarity in multi-table queries.",
                )]
        return []

    # ── W004: Weight of zero ────────────────────────────────────────────

    def _rule_w004_weight_zero(self, tree: Tree) -> list[LintDiagnostic]:
        out: list[LintDiagnostic] = []
        for sub in self._iter_subtrees(tree, "weight_clause"):
            value = self._extract_weight_value(sub)
            if value is not None and value == 0.0:
                line, col = self._get_pos(sub)
                out.append(LintDiagnostic(
                    rule_id="W004",
                    severity="warning",
                    message="Weight 0.0 means membership-only with no score contribution.",
                    line=line, column=col,
                ))
        return out

    # ── Helpers ─────────────────────────────────────────────────────────

    def _has_function_named(self, tree: Tree, name: str) -> bool:
        """Check if any function_call node calls a function with the given name."""
        for sub in self._iter_subtrees(tree, "function_call"):
            if not sub.children:
                continue
            first = sub.children[0]
            if isinstance(first, Tree) and first.data == "identifier":
                fname = self._qualified_name_text(first) if first.data == "qualified_name" else ""
                if not fname:
                    for tok in first.children:
                        if isinstance(tok, Token):
                            fname = tok.value
                            break
                if fname.upper() == name.upper():
                    return True
        return False

    def _extract_weight_value(self, weight_node: Tree) -> Optional[float]:
        """Extract numeric value from a weight_clause tree node."""
        if len(weight_node.children) < 2:
            return None
        lit = weight_node.children[1]
        raw = None
        if isinstance(lit, Token):
            raw = lit.value
        elif isinstance(lit, Tree) and lit.children and isinstance(lit.children[0], Token):
            raw = lit.children[0].value
        if raw is not None:
            try:
                return float(raw)
            except ValueError:
                pass
        return None
