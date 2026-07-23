"""ContextQL semantic analysis layer.

Sits between the parser/linter and a future executor. Provides:
  1. A stable semantic model (typed IR for ContextQL statements)
  2. A lowering pass from Lark CST → semantic model
  3. Semantic diagnostics for checks the linter does not cover

Architecture:
  SQL text → ContextQLParser.parse() → Lark Tree
    → SemanticLowerer.lower(tree) → List[SemanticStatement]
      → SemanticAnalyzer.analyze(stmts) → AnalysisResult

The linter (linter.py) handles fast tree-based diagnostics (E100-E118, W001-W004).
This module handles structured lowering and deeper semantic checks (E120, E130+).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Sequence

from lark import Token, Tree

from .context_options import validate_context_options
from .errors import Severity
from .parser import ContextQLParser


# ============================================================
# Semantic model
# ============================================================


class EntityKeyType(str, Enum):
    UNKNOWN   = "UNKNOWN"
    INT64     = "INT64"
    VARCHAR   = "VARCHAR"
    UUID      = "UUID"
    COMPOSITE = "COMPOSITE"


class StatementKind:
    SELECT = "SELECT"
    CREATE_CONTEXT = "CREATE_CONTEXT"
    ALTER_CONTEXT = "ALTER_CONTEXT"
    DROP_CONTEXT = "DROP_CONTEXT"
    SHOW_CONTEXTS = "SHOW_CONTEXTS"
    DESCRIBE_CONTEXT = "DESCRIBE_CONTEXT"
    REFRESH_CONTEXT = "REFRESH_CONTEXT"
    VALIDATE_CONTEXT = "VALIDATE_CONTEXT"
    CREATE_EVENT_LOG = "CREATE_EVENT_LOG"
    CREATE_PROCESS_MODEL = "CREATE_PROCESS_MODEL"
    UNKNOWN = "UNKNOWN"


@dataclass
class SemanticStatement:
    """Base for all lowered statements."""
    kind: str
    raw_sql: str


@dataclass
class ParameterBinding:
    name: str
    value: str


@dataclass
class TemporalQualifier:
    mode: str  # "AT" | "BETWEEN"
    at_value: Optional[str] = None
    start_value: Optional[str] = None
    end_value: Optional[str] = None


@dataclass
class ContextReference:
    name: str
    source_kind: str = "CONTEXT"  # CONTEXT | MCP
    weight: Optional[float] = None
    temporal: Optional[TemporalQualifier] = None
    parameters: List[ParameterBinding] = field(default_factory=list)


@dataclass
class ContextPredicate:
    binding_alias: Optional[str]
    negated: bool
    all_mode: bool
    sequence_mode: bool
    refs: List[ContextReference]


@dataclass
class TableRef:
    name: str
    alias: Optional[str] = None
    source_kind: str = "TABLE"  # TABLE | REMOTE | SUBQUERY


@dataclass
class JoinRef:
    join_type: str
    table: TableRef
    condition: Optional[str] = None


@dataclass
class OrderItem:
    is_context_order: bool = False
    strategy: Optional[str] = None
    direction: Optional[str] = None
    column_name: Optional[str] = None


@dataclass
class QueryModel(SemanticStatement):
    projections: List[str] = field(default_factory=list)
    from_table: Optional[TableRef] = None
    joins: List[JoinRef] = field(default_factory=list)
    context_predicates: List[ContextPredicate] = field(default_factory=list)
    order_items: List[OrderItem] = field(default_factory=list)
    context_window: Optional[int] = None
    uses_context_score: bool = False
    uses_context_count: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None
    where_text: Optional[str] = None
    group_by: Optional[str] = None
    having: Optional[str] = None


@dataclass(frozen=True)
class ContextParameter:
    """A declared context parameter with an optional typed default."""
    name: str
    type_name: str
    default: Optional[Any] = None


@dataclass(frozen=True)
class ContextCompositionItem:
    """One member of a COMPOSE body, with an optional weight."""
    name: str
    weight: Optional[float] = None


@dataclass(frozen=True)
class ContextComposition:
    """A COMPOSE (...) WITH STRATEGY body."""
    items: tuple[ContextCompositionItem, ...] = ()
    strategy: str = "UNION"


@dataclass(frozen=True)
class MaterializationSettings:
    """Materialization/storage settings resolved from WITH options.

    Defaults follow SPEC.md section 6 (Options).
    """
    materialized: bool = False
    storage: str = "auto"
    refresh_mode: str = "manual"
    refresh_interval: Optional[str] = None
    stale_after: Optional[str] = None
    history: bool = False
    history_retention: Optional[str] = None
    source_watermark: Optional[str] = None


@dataclass
class ContextDefinitionModel(SemanticStatement):
    name: str = ""
    namespace: Optional[str] = None
    parameters: List[ContextParameter] = field(default_factory=list)
    entity_key_name: Optional[str] = None
    entity_key_type: Optional[str] = None
    definition_sql: Optional[str] = None
    composition: Optional[ContextComposition] = None
    score_expression: Optional[str] = None
    temporal_column: Optional[str] = None
    temporal_granularity: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    classification: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    options: Dict[str, Any] = field(default_factory=dict)

    or_replace: bool = False
    duplicate_options: List[str] = field(default_factory=list)

    @property
    def composition_strategy(self) -> Optional[str]:
        return self.composition.strategy if self.composition else None

    def definition_hash(self) -> str:
        """Stable hash of the semantic definition (SPEC section 7).

        Covers every field that changes membership or scoring semantics;
        excludes presentation metadata (description, tags).
        """
        canonical = json.dumps(
            {
                "name": self.name.lower(),
                "namespace": (self.namespace or "").lower(),
                "parameters": [
                    (p.name, p.type_name, repr(p.default)) for p in self.parameters
                ],
                "entity_key_name": self.entity_key_name,
                "definition_sql": self.definition_sql,
                "composition": (
                    [(i.name, i.weight) for i in self.composition.items]
                    + [self.composition.strategy]
                    if self.composition
                    else None
                ),
                "score_expression": self.score_expression,
                "temporal": [self.temporal_column, self.temporal_granularity],
                "options": sorted(
                    (k, repr(v)) for k, v in self.options.items()
                ),
            },
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class AlterContextModel(SemanticStatement):
    """ALTER CONTEXT <name> <action>."""
    name: str = ""
    action: str = ""  # rename_to | set_definition | set_score | drop_score
    #                 # | set_description | set_tags | set_state
    new_name: Optional[str] = None
    definition_sql: Optional[str] = None
    score_expression: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    state: Optional[str] = None


@dataclass
class DropContextModel(SemanticStatement):
    """DROP CONTEXT [IF EXISTS] <name> [CASCADE | RESTRICT]."""
    name: str = ""
    if_exists: bool = False
    cascade: bool = False  # default mode is RESTRICT (SPEC section 8)


@dataclass
class ShowContextsModel(SemanticStatement):
    """SHOW CONTEXTS [LIKE 'pattern']."""
    like_pattern: Optional[str] = None


@dataclass
class DescribeContextModel(SemanticStatement):
    """DESCRIBE CONTEXT <name>."""
    name: str = ""


@dataclass
class RefreshContextModel(SemanticStatement):
    """REFRESH CONTEXT <name> | REFRESH ALL CONTEXTS."""
    name: Optional[str] = None
    refresh_all: bool = False
    parameters: List[ParameterBinding] = field(default_factory=list)


@dataclass
class ValidateContextModel(SemanticStatement):
    """VALIDATE CONTEXT <name>."""
    name: str = ""


@dataclass
class EventLogDefinitionModel(SemanticStatement):
    name: str = ""
    source_table: Optional[str] = None
    case_column: Optional[str] = None
    activity_column: Optional[str] = None
    timestamp_column: Optional[str] = None
    resource_column: Optional[str] = None


@dataclass
class ProcessModelDefinitionModel(SemanticStatement):
    name: str = ""
    event_log_name: Optional[str] = None
    expected_paths: List[List[str]] = field(default_factory=list)


# ============================================================
# Catalog protocol
# ============================================================


@dataclass
class ContextCatalogEntry:
    name: str
    entity_key_name: str
    entity_key_type: EntityKeyType = EntityKeyType.UNKNOWN
    is_temporal: bool = False
    has_score: bool = False
    dependencies: List[str] = field(default_factory=list)
    # Plan section 6.2 metadata
    context_id: Optional[str] = None
    namespace: Optional[str] = None
    version: int = 1
    definition_hash: Optional[str] = None
    lifecycle_state: str = "draft"
    materialization: MaterializationSettings = field(
        default_factory=MaterializationSettings
    )
    current_snapshot_version: Optional[int] = None
    last_validated_at: Optional[datetime] = None
    last_refreshed_at: Optional[datetime] = None
    data_as_of: Optional[datetime] = None
    # Descriptive/definition fields used by DESCRIBE CONTEXT and ALTER
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    definition_sql: Optional[str] = None
    composition: Optional[ContextComposition] = None
    score_expression: Optional[str] = None


@dataclass
class EventLogCatalogEntry:
    name: str
    case_key_name: str = ""
    case_key_type: str = "UNKNOWN"


@dataclass
class ProcessModelCatalogEntry:
    name: str
    event_log_name: Optional[str] = None


@dataclass
class TableCatalogEntry:
    name: str
    alias: Optional[str] = None
    primary_key_name: Optional[str] = None
    primary_key_type: str = "UNKNOWN"


class CatalogProtocol(Protocol):
    def get_context(self, name: str) -> Optional[ContextCatalogEntry]: ...
    def list_contexts(self) -> Sequence[ContextCatalogEntry]: ...
    def get_event_log(self, name: str) -> Optional[EventLogCatalogEntry]: ...
    def list_event_logs(self) -> Sequence[EventLogCatalogEntry]: ...
    def get_process_model(self, name: str) -> Optional[ProcessModelCatalogEntry]: ...
    def get_table(self, name_or_alias: str) -> Optional[TableCatalogEntry]: ...


@dataclass
class InMemoryCatalog:
    """Simple catalog with case-insensitive lookups."""
    contexts: Dict[str, ContextCatalogEntry] = field(default_factory=dict)
    event_logs: Dict[str, EventLogCatalogEntry] = field(default_factory=dict)
    process_models: Dict[str, ProcessModelCatalogEntry] = field(default_factory=dict)
    tables: Dict[str, TableCatalogEntry] = field(default_factory=dict)

    def get_context(self, name: str) -> Optional[ContextCatalogEntry]:
        return self.contexts.get(name.lower())

    def list_contexts(self) -> Sequence[ContextCatalogEntry]:
        return list(self.contexts.values())

    def get_event_log(self, name: str) -> Optional[EventLogCatalogEntry]:
        return self.event_logs.get(name.lower())

    def list_event_logs(self) -> Sequence[EventLogCatalogEntry]:
        return list(self.event_logs.values())

    def get_process_model(self, name: str) -> Optional[ProcessModelCatalogEntry]:
        return self.process_models.get(name.lower())

    def get_table(self, name_or_alias: str) -> Optional[TableCatalogEntry]:
        key = name_or_alias.lower()
        if key in self.tables:
            return self.tables[key]
        for table in self.tables.values():
            if table.alias and table.alias.lower() == key:
                return table
        return None


# ============================================================
# Diagnostic model
# ============================================================


@dataclass
class SemanticDiagnostic:
    code: str
    severity: Severity
    message: str
    hint: Optional[str] = None

    def __str__(self) -> str:
        suffix = f" Hint: {self.hint}" if self.hint else ""
        return f"{self.severity.value.upper()}[{self.code}] {self.message}{suffix}"


@dataclass
class AnalysisResult:
    statements: List[SemanticStatement]
    diagnostics: List[SemanticDiagnostic]

    @property
    def ok(self) -> bool:
        return not any(d.severity == Severity.ERROR for d in self.diagnostics)


# ============================================================
# Tree-walking lowerer
# ============================================================


class SemanticLowerer:
    """Lowers a Lark CST into typed semantic model objects."""

    def lower(self, tree: Tree) -> List[SemanticStatement]:
        statements: List[SemanticStatement] = []
        for node in self._iter_statements(tree):
            stmt = self._lower_node(node)
            if stmt is not None:
                statements.append(stmt)
        return statements

    def _iter_statements(self, tree: Tree) -> List[Tree]:
        """Yield top-level statement nodes from the parse tree."""
        if tree.data == "start":
            return [
                child for child in tree.children
                if isinstance(child, Tree) and child.data != "SEMICOLON"
            ]
        return [tree]

    def _lower_node(self, node: Tree) -> Optional[SemanticStatement]:
        if not isinstance(node, Tree):
            return None
        lowerers = {
            "select_stmt": self._lower_select,
            "create_context_stmt": self._lower_create_context,
            "alter_context_stmt": self._lower_alter_context,
            "drop_context_stmt": self._lower_drop_context,
            "show_contexts_stmt": self._lower_show_contexts,
            "describe_context_stmt": self._lower_describe_context,
            "refresh_context_stmt": self._lower_refresh_context,
            "validate_context_stmt": self._lower_validate_context,
            "create_event_log_stmt": self._lower_create_event_log,
            "create_process_model_stmt": self._lower_create_process_model,
        }
        handler = lowerers.get(node.data)
        if handler:
            return handler(node)
        return SemanticStatement(kind=StatementKind.UNKNOWN, raw_sql=self._tree_text(node))

    # ── SELECT ─────────────────────────────────────────────

    def _lower_select(self, node: Tree) -> QueryModel:
        model = QueryModel(kind=StatementKind.SELECT, raw_sql=self._tree_text(node))

        # Context window
        for sub in self._iter_subtrees(node, "context_window_clause"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "int_literal":
                    model.context_window = self._int_value(child)

        # Projections
        for sub in self._iter_subtrees(node, "select_item"):
            alias_nodes = self._iter_subtrees(sub, "alias")
            alias = self._alias_text(alias_nodes[0]) if alias_nodes else None
            is_score = (
                bool(self._iter_subtrees(sub, "context_score_call"))
                or self._has_function_named(sub, "CONTEXT_SCORE")
            )
            is_count = (
                bool(self._iter_subtrees(sub, "context_count_call"))
                or self._has_function_named(sub, "CONTEXT_COUNT")
            )
            if is_score:
                text = f"CONTEXT_SCORE() AS {alias}" if alias else "CONTEXT_SCORE()"
                model.projections.append(text)
            elif is_count:
                text = f"CONTEXT_COUNT() AS {alias}" if alias else "CONTEXT_COUNT()"
                model.projections.append(text)
            else:
                model.projections.append(self._tree_text(sub))
        if not model.projections:
            # project_all case (SELECT *)
            for sub in node.children:
                if isinstance(sub, Tree) and sub.data == "project_all":
                    model.projections.append("*")

        # FROM + JOINs
        for from_clause in self._iter_subtrees(node, "from_clause"):
            children = list(from_clause.children)
            for child in children:
                if isinstance(child, Tree) and child.data == "table_ref":
                    if model.from_table is None:
                        model.from_table = self._lower_table_ref(child)
                elif isinstance(child, Tree) and child.data == "join_clause":
                    model.joins.append(self._lower_join(child))

        # Context predicates
        for cp in self._iter_subtrees(node, "context_predicate"):
            model.context_predicates.append(self._lower_context_predicate(cp))

        # ORDER BY
        for item in self._iter_subtrees(node, "order_item"):
            model.order_items.append(self._lower_order_item(item))

        # CONTEXT_SCORE / CONTEXT_COUNT
        model.uses_context_score = any(
            True for _ in self._iter_subtrees(node, "context_score_call")
        ) or self._has_function_named(node, "CONTEXT_SCORE")
        model.uses_context_count = any(
            True for _ in self._iter_subtrees(node, "context_count_call")
        ) or self._has_function_named(node, "CONTEXT_COUNT")

        # WHERE text (full predicate including CONTEXT predicates)
        for sub in self._iter_subtrees(node, "where_clause"):
            for child in sub.children:
                if isinstance(child, Tree):
                    # capture any Tree after WHERE — the predicate (any variant)
                    model.where_text = self._tree_text(child)
                    break

        # GROUP BY
        for sub in self._iter_subtrees(node, "group_by_clause"):
            parts = [self._tree_text(c) for c in sub.children if isinstance(c, Tree)]
            if parts:
                model.group_by = ", ".join(parts)

        # HAVING
        for sub in self._iter_subtrees(node, "having_clause"):
            for child in sub.children:
                if isinstance(child, Tree):
                    model.having = self._tree_text(child)
                    break

        # LIMIT / OFFSET
        for sub in self._iter_subtrees(node, "limit_clause"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "int_literal":
                    model.limit = self._int_value(child)
        for sub in self._iter_subtrees(node, "offset_clause"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "int_literal":
                    model.offset = self._int_value(child)

        return model

    def _lower_table_ref(self, node: Tree) -> TableRef:
        name = ""
        alias = None
        source_kind = "TABLE"

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "qualified_name":
                    name = self._qualified_name_text(child)
                elif child.data == "alias":
                    alias = self._alias_text(child)
                elif child.data == "remote_source":
                    source_kind = "REMOTE"
                    for rc in child.children:
                        if isinstance(rc, Tree) and rc.data == "qualified_name":
                            name = self._qualified_name_text(rc)
                elif child.data == "select_stmt":
                    source_kind = "SUBQUERY"
                    name = "(subquery)"

        return TableRef(name=name, alias=alias, source_kind=source_kind)

    def _lower_join(self, node: Tree) -> JoinRef:
        join_type = "JOIN"
        table = TableRef(name="")
        condition = None
        found_on = False

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "join_type":
                    parts = []
                    for tok in child.children:
                        if isinstance(tok, Token):
                            parts.append(tok.value.upper())
                    join_type = " ".join(parts) + " JOIN" if parts else "JOIN"
                elif child.data == "table_ref":
                    table = self._lower_table_ref(child)
                elif found_on:
                    # capture the first tree after ON — the predicate (any variant)
                    condition = self._tree_text(child)
                    found_on = False
            elif isinstance(child, Token):
                if child.type == "CROSS":
                    join_type = "CROSS JOIN"
                elif child.type == "ON":
                    found_on = True

        return JoinRef(join_type=join_type, table=table, condition=condition)

    def _lower_context_predicate(self, node: Tree) -> ContextPredicate:
        binding_alias = None
        negated = False
        all_mode = False
        sequence_mode = False
        refs: List[ContextReference] = []

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "context_on_binding":
                    binding_alias = self._identifier_text(child)
                elif child.data == "context_mode":
                    all_mode = any(
                        isinstance(t, Token) and t.type == "ALL"
                        for t in child.children
                    )
                elif child.data == "context_ref_chain":
                    refs, sequence_mode = self._lower_context_ref_chain(child)
                elif child.data == "context_ref_list":
                    # NOT IN case — refs are direct children
                    refs = self._lower_context_ref_list(child)
            elif isinstance(child, Token) and child.type == "NOT":
                negated = True

        return ContextPredicate(
            binding_alias=binding_alias,
            negated=negated,
            all_mode=all_mode,
            sequence_mode=sequence_mode,
            refs=refs,
        )

    def _lower_context_ref_chain(self, node: Tree) -> tuple[List[ContextReference], bool]:
        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "then_chain":
                    refs = self._lower_context_ref_list(child)
                    return refs, True
                elif child.data == "context_ref_list":
                    refs = self._lower_context_ref_list(child)
                    return refs, False
        return [], False

    def _lower_context_ref_list(self, node: Tree) -> List[ContextReference]:
        refs: List[ContextReference] = []
        for child in node.children:
            if isinstance(child, Tree) and child.data == "context_ref":
                refs.append(self._lower_context_ref(child))
        return refs

    def _lower_context_ref(self, node: Tree) -> ContextReference:
        name = ""
        source_kind = "CONTEXT"
        weight = None
        temporal = None
        parameters: List[ParameterBinding] = []

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "context_invocation":
                    name, source_kind, parameters = self._lower_context_invocation(child)
                elif child.data == "weight_clause":
                    weight = self._extract_weight(child)
                elif child.data == "temporal_qualifier":
                    temporal = self._lower_temporal(child)

        return ContextReference(
            name=name,
            source_kind=source_kind,
            weight=weight,
            temporal=temporal,
            parameters=parameters,
        )

    def _lower_context_invocation(self, node: Tree) -> tuple[str, str, List[ParameterBinding]]:
        name = ""
        source_kind = "CONTEXT"
        parameters: List[ParameterBinding] = []

        has_mcp = any(isinstance(c, Token) and c.type == "MCP" for c in node.children)
        if has_mcp:
            source_kind = "MCP"

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "qualified_name":
                    name = self._qualified_name_text(child)
                elif child.data == "context_call_args":
                    for arg in self._iter_subtrees(child, "named_arg"):
                        pname, pvalue = self._lower_named_arg(arg)
                        parameters.append(ParameterBinding(name=pname, value=pvalue))

        return name, source_kind, parameters

    def _lower_temporal(self, node: Tree) -> TemporalQualifier:
        mode = "AT"
        at_value = None
        start_value = None
        end_value = None

        has_between = any(
            isinstance(c, Token) and c.type == "BETWEEN" for c in node.children
        )
        if has_between:
            mode = "BETWEEN"
            literals = [c for c in node.children if isinstance(c, Tree) and c.data == "literal"]
            if len(literals) >= 2:
                start_value = self._tree_text(literals[0])
                end_value = self._tree_text(literals[1])
        else:
            for child in node.children:
                if isinstance(child, Tree) and child.data == "literal":
                    at_value = self._tree_text(child)

        return TemporalQualifier(mode=mode, at_value=at_value,
                                 start_value=start_value, end_value=end_value)

    def _lower_order_item(self, node: Tree) -> OrderItem:
        is_context = any(
            isinstance(c, Token) and c.type == "CONTEXT" for c in node.children
        )
        strategy = None
        direction = None
        column_name = None

        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "scoring_strategy":
                    for tok in child.children:
                        if isinstance(tok, Token):
                            strategy = tok.value.upper()
                elif child.data == "sort_dir":
                    for tok in child.children:
                        if isinstance(tok, Token):
                            direction = tok.value.upper()
                elif child.data in ("column_ref", "qualified_name"):
                    column_name = self._tree_text(child)
                elif not is_context:
                    # Expression-based order item
                    column_name = self._tree_text(child)
            elif isinstance(child, Token) and child.type in ("ASC", "DESC"):
                direction = child.value.upper()

        return OrderItem(
            is_context_order=is_context,
            strategy=strategy,
            direction=direction,
            column_name=column_name,
        )

    # ── CREATE CONTEXT ──────────────────────────────────────

    def _lower_create_context(self, node: Tree) -> ContextDefinitionModel:
        model = ContextDefinitionModel(
            kind=StatementKind.CREATE_CONTEXT,
            raw_sql=self._tree_text(node),
        )

        # OR REPLACE flag
        model.or_replace = any(
            isinstance(t, Token) and t.type == "REPLACE" for t in node.children
        )

        # Name: first qualified_name child; a dotted prefix is the namespace
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                full_name = self._qualified_name_text(child)
                if "." in full_name:
                    model.namespace, model.name = full_name.rsplit(".", 1)
                else:
                    model.name = full_name
                break

        # Parameters
        for sub in self._iter_subtrees(node, "context_params"):
            for pdef in self._iter_subtrees(sub, "param_def"):
                param = self._lower_param_def(pdef)
                if param is not None:
                    model.parameters.append(param)

        # ON entity key: identifier after ON token
        found_on = False
        for child in node.children:
            if isinstance(child, Token) and child.type == "ON":
                found_on = True
            elif found_on and isinstance(child, Tree) and child.data == "identifier":
                model.entity_key_name = self._identifier_text_direct(child)
                break

        # SCORE expression
        for sub in self._iter_subtrees(node, "score_clause"):
            parts = []
            for child in sub.children:
                if isinstance(child, Tree) and child.data != "score_clause":
                    parts.append(self._tree_text(child))
            if parts:
                model.score_expression = " ".join(parts)

        # TEMPORAL declaration
        for sub in self._iter_subtrees(node, "temporal_decl"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "identifier":
                    model.temporal_column = self._identifier_text_direct(child)
                elif isinstance(child, Tree) and child.data == "temporal_granularity":
                    for tok in child.children:
                        if isinstance(tok, Token):
                            model.temporal_granularity = tok.value.upper()

        # DESCRIPTION
        for sub in self._iter_subtrees(node, "description_clause"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "string_literal":
                    model.description = self._string_value(child)

        # TAGS
        for sub in self._iter_subtrees(node, "tags_clause"):
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "string_literal":
                    model.tags.append(self._string_value(child))

        # CLASSIFICATION
        for sub in self._iter_subtrees(node, "classification_clause"):
            text = self._identifier_text(sub)
            if text:
                model.classification = text

        # WITH options: names are case-insensitive (SPEC section 6 / CS-13);
        # duplicates are recorded for E151 rather than last-writer-wins
        for sub in self._iter_subtrees(node, "context_with_options"):
            for opt in self._iter_subtrees(sub, "context_option"):
                name, value = self._lower_context_option(opt)
                if not name:
                    continue
                if name in model.options:
                    model.duplicate_options.append(name)
                    continue
                model.options[name] = value

        # Definition body: AS select_stmt or AS compose_clause
        for child in node.children:
            if isinstance(child, Tree) and child.data == "select_stmt":
                model.definition_sql = self._tree_text(child)
            elif isinstance(child, Tree) and child.data == "compose_clause":
                model.composition = self._lower_compose_clause(child)

        # Dependencies: context references in the body
        own_names = {model.name.lower()}
        if model.namespace:
            own_names.add(f"{model.namespace}.{model.name}".lower())
        for inv in self._iter_subtrees(node, "context_invocation"):
            # Skip the context's own name
            if not inv.children:
                continue
            first = inv.children[0]
            if isinstance(first, Tree) and first.data == "qualified_name":
                ref_name = self._qualified_name_text(first)
                if ref_name.lower() not in own_names:
                    model.dependencies.append(ref_name)
        if model.composition:
            for item in model.composition.items:
                if (
                    item.name.lower() not in own_names
                    and item.name not in model.dependencies
                ):
                    model.dependencies.append(item.name)

        return model

    def _lower_alter_context(self, node: Tree) -> AlterContextModel:
        model = AlterContextModel(
            kind=StatementKind.ALTER_CONTEXT,
            raw_sql=self._tree_text(node),
        )
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                model.name = self._qualified_name_text(child)
                break
        for action in self._iter_subtrees(node, "alter_context_action"):
            token_types = [
                t.type for t in action.children if isinstance(t, Token)
            ]
            if "RENAME" in token_types:
                model.action = "rename_to"
                for child in action.children:
                    if isinstance(child, Tree) and child.data == "qualified_name":
                        model.new_name = self._qualified_name_text(child)
            elif "DEFINITION" in token_types:
                model.action = "set_definition"
                for child in action.children:
                    if isinstance(child, Tree) and child.data == "select_stmt":
                        model.definition_sql = self._tree_text(child)
            elif "SCORE" in token_types and "DROP" in token_types:
                model.action = "drop_score"
            elif "SCORE" in token_types:
                model.action = "set_score"
                for child in action.children:
                    if isinstance(child, Tree):
                        model.score_expression = self._tree_text(child)
            elif "DESCRIPTION" in token_types:
                model.action = "set_description"
                for child in action.children:
                    if isinstance(child, Tree) and child.data == "string_literal":
                        model.description = self._string_value(child)
            elif "TAGS" in token_types:
                model.action = "set_tags"
                for child in action.children:
                    if isinstance(child, Tree) and child.data == "string_literal":
                        model.tags.append(self._string_value(child))
            elif "STATE" in token_types:
                model.action = "set_state"
                for child in action.children:
                    if isinstance(child, Tree) and child.data == "string_literal":
                        model.state = self._string_value(child)
        return model

    def _lower_drop_context(self, node: Tree) -> DropContextModel:
        model = DropContextModel(
            kind=StatementKind.DROP_CONTEXT,
            raw_sql=self._tree_text(node),
        )
        token_types = [t.type for t in node.children if isinstance(t, Token)]
        model.if_exists = "IF" in token_types and "EXISTS" in token_types
        model.cascade = "CASCADE" in token_types
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                model.name = self._qualified_name_text(child)
        return model

    def _lower_show_contexts(self, node: Tree) -> ShowContextsModel:
        model = ShowContextsModel(
            kind=StatementKind.SHOW_CONTEXTS,
            raw_sql=self._tree_text(node),
        )
        for child in node.children:
            if isinstance(child, Tree) and child.data == "string_literal":
                model.like_pattern = self._string_value(child)
        return model

    def _lower_describe_context(self, node: Tree) -> DescribeContextModel:
        model = DescribeContextModel(
            kind=StatementKind.DESCRIBE_CONTEXT,
            raw_sql=self._tree_text(node),
        )
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                model.name = self._qualified_name_text(child)
        return model

    def _lower_refresh_context(self, node: Tree) -> RefreshContextModel:
        model = RefreshContextModel(
            kind=StatementKind.REFRESH_CONTEXT,
            raw_sql=self._tree_text(node),
        )
        token_types = [t.type for t in node.children if isinstance(t, Token)]
        model.refresh_all = "ALL" in token_types
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                model.name = self._qualified_name_text(child)
        for arg in self._iter_subtrees(node, "named_arg"):
            name, value = self._lower_named_arg(arg)
            model.parameters.append(ParameterBinding(name=name, value=value))
        return model

    def _lower_validate_context(self, node: Tree) -> ValidateContextModel:
        model = ValidateContextModel(
            kind=StatementKind.VALIDATE_CONTEXT,
            raw_sql=self._tree_text(node),
        )
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                model.name = self._qualified_name_text(child)
        return model

    def _lower_param_def(self, node: Tree) -> Optional[ContextParameter]:
        """Lower ``identifier type_name (DEFAULT literal)?``."""
        name: Optional[str] = None
        type_name: Optional[str] = None
        default: Optional[Any] = None
        for child in node.children:
            if isinstance(child, Tree) and child.data == "identifier" and name is None:
                name = self._identifier_text_direct(child)
            elif isinstance(child, Tree) and child.data == "type_name":
                type_name = self._tree_text(child).replace(" ", "")
            elif isinstance(child, Tree) and child.data == "literal":
                default = self._literal_value(child)
        if name is None or type_name is None:
            return None
        return ContextParameter(name=name, type_name=type_name, default=default)

    def _lower_context_option(self, node: Tree) -> tuple[str, Any]:
        """Lower ``identifier EQ expr`` to a (lowercased name, value) pair."""
        name = ""
        value: Any = None
        seen_name = False
        for child in node.children:
            if (
                isinstance(child, Tree)
                and child.data == "identifier"
                and not seen_name
            ):
                name = self._identifier_text_direct(child).lower()
                seen_name = True
            elif isinstance(child, Tree):
                value = self._option_value(child)
        return name, value

    def _option_value(self, node: Tree) -> Any:
        """Lower an option value expression to a Python-native value.

        Single literals and identifiers become native values; anything more
        complex is retained as raw expression text.
        """
        tokens = [t for t in node.scan_values(lambda v: isinstance(v, Token))]
        if len(tokens) == 1:
            return self._token_value(tokens[0])
        return self._tree_text(node)

    def _literal_value(self, node: Tree) -> Any:
        """Lower a literal node to a Python-native value."""
        tokens = [t for t in node.scan_values(lambda v: isinstance(v, Token))]
        if len(tokens) == 1:
            return self._token_value(tokens[0])
        return self._tree_text(node)

    def _token_value(self, tok: Token) -> Any:
        if tok.type == "TRUE":
            return True
        if tok.type == "FALSE":
            return False
        if tok.type == "NULL":
            return None
        if tok.type == "STRING":
            raw = tok.value
            if raw.startswith("'") and raw.endswith("'"):
                return raw[1:-1].replace("''", "'")
            return raw
        if tok.type == "INT":
            return int(tok.value)
        if tok.type == "NUMBER":
            return float(tok.value)
        if tok.type in ("IDENTIFIER", "QUOTED_IDENTIFIER"):
            return tok.value.strip('"')
        return tok.value

    def _lower_compose_clause(self, node: Tree) -> ContextComposition:
        items: List[ContextCompositionItem] = []
        for item_node in self._iter_subtrees(node, "compose_item"):
            name: Optional[str] = None
            weight: Optional[float] = None
            for child in item_node.children:
                if isinstance(child, Tree) and child.data == "qualified_name":
                    name = self._qualified_name_text(child)
                elif isinstance(child, Tree) and child.data == "weight_clause":
                    weight = self._extract_weight(child)
            if name:
                items.append(ContextCompositionItem(name=name, weight=weight))
        strategy = "UNION"
        for strat_node in self._iter_subtrees(node, "compose_strategy"):
            for tok in strat_node.children:
                if isinstance(tok, Token):
                    strategy = tok.value.upper()
        return ContextComposition(items=tuple(items), strategy=strategy)

    # ── CREATE EVENT LOG ────────────────────────────────────

    def _lower_create_event_log(self, node: Tree) -> EventLogDefinitionModel:
        model = EventLogDefinitionModel(
            kind=StatementKind.CREATE_EVENT_LOG,
            raw_sql=self._tree_text(node),
        )

        # Walk children in order to extract positional identifiers.
        # Grammar: CREATE EVENT LOG name FROM source ON case ACTIVITY act TIMESTAMP ts (RESOURCE res)?
        prev_token_type = None
        for child in node.children:
            if isinstance(child, Token):
                prev_token_type = child.type
            elif isinstance(child, Tree):
                if child.data == "qualified_name" and not model.name:
                    model.name = self._qualified_name_text(child)
                elif child.data == "event_source":
                    for sc in child.children:
                        if isinstance(sc, Tree) and sc.data == "qualified_name":
                            model.source_table = self._qualified_name_text(sc)
                elif child.data == "identifier":
                    ident = self._identifier_text_direct(child)
                    if prev_token_type == "ON":
                        model.case_column = ident
                    elif prev_token_type == "ACTIVITY":
                        model.activity_column = ident
                    elif prev_token_type == "TIMESTAMP":
                        model.timestamp_column = ident
                    elif prev_token_type == "RESOURCE":
                        model.resource_column = ident

        return model

    # ── CREATE PROCESS MODEL ────────────────────────────────

    def _lower_create_process_model(self, node: Tree) -> ProcessModelDefinitionModel:
        model = ProcessModelDefinitionModel(
            kind=StatementKind.CREATE_PROCESS_MODEL,
            raw_sql=self._tree_text(node),
        )

        # Name: first qualified_name
        for child in node.children:
            if isinstance(child, Tree) and child.data == "qualified_name":
                if not model.name:
                    model.name = self._qualified_name_text(child)
                elif not model.event_log_name:
                    model.event_log_name = self._qualified_name_text(child)

        # Expected paths
        for sub in self._iter_subtrees(node, "expected_path_clause"):
            path: List[str] = []
            for child in sub.children:
                if isinstance(child, Tree) and child.data == "string_literal":
                    path.append(self._string_value(child))
            if path:
                model.expected_paths.append(path)

        return model

    # ── Tree helpers ────────────────────────────────────────

    def _iter_subtrees(self, tree: Tree, data: str) -> List[Tree]:
        return [
            sub for sub in tree.iter_subtrees_topdown()
            if sub.data == data
        ]

    def _qualified_name_text(self, node: Tree) -> str:
        parts: List[str] = []
        for child in node.children:
            if isinstance(child, Token) and child.type in ("IDENTIFIER", "QUOTED_IDENTIFIER"):
                parts.append(child.value.strip('"'))
            elif isinstance(child, Tree) and child.data == "identifier":
                for tok in child.children:
                    if isinstance(tok, Token) and tok.type in ("IDENTIFIER", "QUOTED_IDENTIFIER"):
                        parts.append(tok.value.strip('"'))
        return ".".join(parts)

    def _identifier_text(self, node: Tree) -> Optional[str]:
        """Extract identifier text from a node that contains an identifier child."""
        for child in node.children:
            if isinstance(child, Tree) and child.data == "identifier":
                return self._identifier_text_direct(child)
            elif isinstance(child, Token) and child.type in ("IDENTIFIER", "QUOTED_IDENTIFIER"):
                return child.value.strip('"')
        return None

    def _identifier_text_direct(self, node: Tree) -> str:
        """Extract text from an identifier node directly."""
        for tok in node.children:
            if isinstance(tok, Token) and tok.type in ("IDENTIFIER", "QUOTED_IDENTIFIER"):
                return tok.value.strip('"')
        return ""

    def _alias_text(self, node: Tree) -> Optional[str]:
        """Extract alias name from an alias node."""
        for child in node.children:
            if isinstance(child, Tree) and child.data == "identifier":
                return self._identifier_text_direct(child)
        return None

    def _string_value(self, node: Tree) -> str:
        """Extract string content from a string_literal node (strips quotes)."""
        for tok in node.children:
            if isinstance(tok, Token) and tok.type == "STRING":
                raw = tok.value
                if raw.startswith("'") and raw.endswith("'"):
                    return raw[1:-1].replace("''", "'")
                return raw
        return ""

    def _int_value(self, node: Tree) -> Optional[int]:
        """Extract integer from an int_literal node."""
        for tok in node.children:
            if isinstance(tok, Token) and tok.type == "INT":
                try:
                    return int(tok.value)
                except ValueError:
                    pass
        return None

    def _extract_weight(self, node: Tree) -> Optional[float]:
        """Extract numeric weight from a weight_clause node."""
        for child in node.children:
            if isinstance(child, Tree) and child.data == "numeric_literal":
                for tok in child.children:
                    if isinstance(tok, Token):
                        try:
                            return float(tok.value)
                        except ValueError:
                            pass
            elif isinstance(child, Tree) and child.data == "int_literal":
                val = self._int_value(child)
                if val is not None:
                    return float(val)
            elif isinstance(child, Token) and child.type in ("NUMBER", "INT"):
                try:
                    return float(child.value)
                except ValueError:
                    pass
        return None

    def _lower_named_arg(self, node: Tree) -> tuple[str, str]:
        """Extract name and value from a named_arg node."""
        name = ""
        value = ""
        for child in node.children:
            if isinstance(child, Tree):
                if child.data == "identifier" and not name:
                    name = self._identifier_text_direct(child)
                else:
                    value = self._tree_text(child)
        return name, value

    def _has_function_named(self, tree: Tree, name: str) -> bool:
        """Check for function_call node matching a name (handles Earley ambiguity)."""
        for sub in self._iter_subtrees(tree, "function_call"):
            if not sub.children:
                continue
            first = sub.children[0]
            if isinstance(first, Tree) and first.data == "identifier":
                for tok in first.children:
                    if isinstance(tok, Token) and tok.value.upper() == name.upper():
                        return True
        return False

    def _tree_text(self, node: Tree) -> str:
        """Reconstruct text from a tree node by joining all tokens.

        Special-cases ``global_call`` and ``zscore_call`` to emit valid SQL
        instead of the ContextQL keyword form.
        """
        if node.data == "global_call":
            return self._expand_global_call(node)
        if node.data == "zscore_call":
            return self._expand_zscore_call(node)
        parts: List[str] = []
        for child in node.children:
            if isinstance(child, Token):
                parts.append(child.value)
            elif isinstance(child, Tree):
                parts.append(self._tree_text(child))
        return " ".join(parts)

    def _expand_global_call(self, node: Tree) -> str:
        """Expand GLOBAL(agg_expr) → 'agg_expr OVER ()'."""
        for child in node.children:
            if isinstance(child, Tree) and child.data == "function_call":
                inner = self._tree_text(child)
                return f"{inner} OVER ()"
        return "NULL"  # malformed node fallback

    def _expand_zscore_call(self, node: Tree) -> str:
        """Expand ZSCORE(expr) → standard-score window expression."""
        for child in node.children:
            if isinstance(child, Tree):
                e = self._tree_text(child)
                return (
                    f"({e} - AVG({e}) OVER ())"
                    f" / NULLIF(STDDEV_SAMP({e}) OVER (), 0.0)"
                )
        return "NULL"  # malformed node fallback


# ============================================================
# Analyzer (new checks only — linter handles E100-E118, W001-W004)
# ============================================================


class SemanticAnalyzer:
    # TODO(v0.3): Only 5 semantic checks (E120, E130-133, E140-141).
    # Needs type checking per Whitepaper Section 5, scope validation,
    # and cross-statement consistency analysis.

    def __init__(self, catalog: Optional[CatalogProtocol] = None) -> None:
        self.catalog = catalog or InMemoryCatalog()

    def analyze(self, statements: List[SemanticStatement]) -> AnalysisResult:
        diagnostics: List[SemanticDiagnostic] = []
        for stmt in statements:
            if isinstance(stmt, ContextDefinitionModel):
                diagnostics.extend(self._check_context_definition(stmt))
            elif isinstance(stmt, EventLogDefinitionModel):
                diagnostics.extend(self._check_event_log_definition(stmt))
            elif isinstance(stmt, ProcessModelDefinitionModel):
                diagnostics.extend(self._check_process_model_definition(stmt))
        return AnalysisResult(statements=statements, diagnostics=diagnostics)

    def _check_context_definition(self, ctx: ContextDefinitionModel) -> List[SemanticDiagnostic]:
        diags: List[SemanticDiagnostic] = []
        if not ctx.entity_key_name:
            diags.append(SemanticDiagnostic(
                code="E120",
                severity=Severity.ERROR,
                message=f"CREATE CONTEXT '{ctx.name}' is missing an ON entity key declaration.",
                hint="Add ON <entity_key_column> to the context definition.",
            ))
        diags.extend(validate_context_options(ctx))
        return diags

    def _check_event_log_definition(self, log: EventLogDefinitionModel) -> List[SemanticDiagnostic]:
        diags: List[SemanticDiagnostic] = []
        if not log.source_table:
            diags.append(SemanticDiagnostic(
                code="E130",
                severity=Severity.ERROR,
                message=f"CREATE EVENT LOG '{log.name}' is missing a FROM source table.",
                hint="Add FROM <source_table>.",
            ))
        if not log.case_column:
            diags.append(SemanticDiagnostic(
                code="E131",
                severity=Severity.ERROR,
                message=f"CREATE EVENT LOG '{log.name}' is missing ON <case_column>.",
                hint="Add ON <case_id_column>.",
            ))
        if not log.activity_column:
            diags.append(SemanticDiagnostic(
                code="E132",
                severity=Severity.ERROR,
                message=f"CREATE EVENT LOG '{log.name}' is missing ACTIVITY <column>.",
                hint="Add ACTIVITY <activity_column>.",
            ))
        if not log.timestamp_column:
            diags.append(SemanticDiagnostic(
                code="E133",
                severity=Severity.ERROR,
                message=f"CREATE EVENT LOG '{log.name}' is missing TIMESTAMP <column>.",
                hint="Add TIMESTAMP <timestamp_column>.",
            ))
        return diags

    def _check_process_model_definition(self, model: ProcessModelDefinitionModel) -> List[SemanticDiagnostic]:
        diags: List[SemanticDiagnostic] = []
        if not model.expected_paths:
            diags.append(SemanticDiagnostic(
                code="E140",
                severity=Severity.ERROR,
                message=f"CREATE PROCESS MODEL '{model.name}' has no EXPECTED PATH clauses.",
                hint="Add at least one EXPECTED PATH ('step1', 'step2', ...).",
            ))
        if model.event_log_name:
            if self.catalog.get_event_log(model.event_log_name) is None:
                diags.append(SemanticDiagnostic(
                    code="E141",
                    severity=Severity.ERROR,
                    message=f"Undefined event log '{model.event_log_name}' in CREATE PROCESS MODEL '{model.name}'.",
                    hint="Create or register the event log first.",
                ))
        return diags


# ============================================================
# Convenience API
# ============================================================


def analyze_sql(
    sql: str,
    catalog: Optional[CatalogProtocol] = None,
) -> AnalysisResult:
    """Parse, lower, and analyze a ContextQL SQL string in one call."""
    parser = ContextQLParser()
    tree = parser.parse(sql)
    lowerer = SemanticLowerer()
    statements = lowerer.lower(tree)
    analyzer = SemanticAnalyzer(catalog=catalog)
    return analyzer.analyze(statements)


# ============================================================
# Demo
# ============================================================


if __name__ == "__main__":
    demo_catalog = InMemoryCatalog(
        contexts={
            "late_invoice": ContextCatalogEntry(
                name="late_invoice",
                entity_key_name="invoice_id",
                entity_key_type=EntityKeyType.INT64,
                has_score=False,
            ),
            "high_value": ContextCatalogEntry(
                name="high_value",
                entity_key_name="invoice_id",
                entity_key_type=EntityKeyType.INT64,
                has_score=True,
            ),
        },
        tables={
            "invoices": TableCatalogEntry(
                name="invoices",
                alias="i",
                primary_key_name="invoice_id",
                primary_key_type="INT64",
            ),
        },
    )

    demo_sql = """
    SELECT invoice_id, vendor_name, CONTEXT_SCORE() AS risk_score
    FROM invoices i
    WHERE CONTEXT ON i IN (late_invoice, high_value WEIGHT 0.8)
    ORDER BY CONTEXT DESC
    LIMIT 20;
    """

    result = analyze_sql(demo_sql, demo_catalog)

    print("=== Statements ===")
    for stmt in result.statements:
        print(f"  {stmt.kind}: {type(stmt).__name__}")
        if isinstance(stmt, QueryModel):
            print(f"    projections: {stmt.projections}")
            print(f"    from: {stmt.from_table}")
            print(f"    context_predicates: {len(stmt.context_predicates)}")
            print(f"    order_items: {stmt.order_items}")
            print(f"    uses_context_score: {stmt.uses_context_score}")
            print(f"    limit: {stmt.limit}")

    print(f"\n=== Diagnostics ({len(result.diagnostics)}) ===")
    if not result.diagnostics:
        print("  No diagnostics (clean).")
    else:
        for diag in result.diagnostics:
            print(f"  {diag}")
