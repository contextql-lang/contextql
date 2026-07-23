"""Executable context DDL (plan section 6.3).

Executes CREATE / ALTER / DROP / SHOW / DESCRIBE / VALIDATE / REFRESH CONTEXT
statements against the engine catalog and DuckDB adapter, following the
execution sequence in docs/plans/post-trade-roaring-contexts.md section 6.3.

Snapshot materialization (steps 9-10) is resolved into catalog settings here;
actual snapshot building arrives with the membership store (plan sections
7.2-7.3).
"""
from __future__ import annotations

import logging
import re
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

from .context_options import resolve_materialization
from .errors import E159
from .history import MembershipHistoryStore, derive_changes
from .membership import make_membership_store
from .semantic import (
    AlterContextModel,
    ContextCatalogEntry,
    ContextDefinitionModel,
    DescribeContextModel,
    DropContextModel,
    EntityKeyType,
    InMemoryCatalog,
    RefreshContextModel,
    ShowContextsModel,
    ValidateContextModel,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

LIFECYCLE_STATES = frozenset({"draft", "validated", "active", "retired"})

INTEGER_ENTITY_KEY_TYPES = frozenset({EntityKeyType.INT64})

AuditCallback = Callable[[Dict[str, Any]], None]


def _like_to_regex(pattern: str) -> "re.Pattern[str]":
    """Translate a SQL LIKE pattern to a compiled regex."""
    escaped = re.escape(pattern)
    regex = escaped.replace("%", ".*").replace("_", ".")
    return re.compile(f"^{regex}$", re.IGNORECASE)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ContextDDLExecutor:
    """Executes context DDL statements against catalog and adapter."""

    def __init__(
        self,
        catalog: InMemoryCatalog,
        adapter,
        membership=None,
        history: Optional[MembershipHistoryStore] = None,
    ) -> None:
        self.catalog = catalog
        self.adapter = adapter
        # One shared store selected automatically (Roaring when available).
        # Per-context 'storage' preferences refine representation in the
        # bitmap-aware execution PR; behavior is identical either way.
        self.membership = membership if membership is not None \
            else make_membership_store("auto")
        self.history = history if history is not None \
            else MembershipHistoryStore()
        self.audit_callbacks: List[AuditCallback] = []

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def execute(self, stmt) -> pd.DataFrame:
        if isinstance(stmt, ContextDefinitionModel):
            return self._create(stmt)
        if isinstance(stmt, AlterContextModel):
            return self._alter(stmt)
        if isinstance(stmt, DropContextModel):
            return self._drop(stmt)
        if isinstance(stmt, ShowContextsModel):
            return self._show(stmt)
        if isinstance(stmt, DescribeContextModel):
            return self._describe(stmt)
        if isinstance(stmt, RefreshContextModel):
            return self._refresh(stmt)
        if isinstance(stmt, ValidateContextModel):
            return self._validate(stmt)
        raise ValueError(f"Unsupported DDL statement: {stmt.kind}")

    # ------------------------------------------------------------------
    # CREATE
    # ------------------------------------------------------------------

    def _create(self, stmt: ContextDefinitionModel) -> pd.DataFrame:
        key = stmt.name.lower()
        existing = self.catalog.contexts.get(key)
        if existing is not None and not stmt.or_replace:
            raise ValueError(
                f"Context '{stmt.name}' already exists; "
                "use CREATE OR REPLACE CONTEXT to replace it."
            )

        self._check_dependencies(stmt)

        entity_key_type = self._resolve_key_type(stmt.entity_key_name)
        materialization = resolve_materialization(stmt)
        if (
            materialization.storage == "roaring"
            and entity_key_type is not EntityKeyType.UNKNOWN
            and entity_key_type not in INTEGER_ENTITY_KEY_TYPES
        ):
            raise ValueError(
                f"[E157] storage = 'roaring' on context '{stmt.name}' "
                f"requires an integer entity key; "
                f"'{stmt.entity_key_name}' is {entity_key_type.value}."
            )
        if (
            stmt.score_expression is not None
            and not _IDENTIFIER_RE.match(stmt.score_expression)
        ):
            raise ValueError(
                f"SCORE expression {stmt.score_expression!r} on context "
                f"'{stmt.name}' must name a column of the definition result "
                "in v0.3; compute the score inside the SELECT and reference "
                "it by name."
            )

        version = existing.version + 1 if existing is not None else 1
        entry = ContextCatalogEntry(
            name=stmt.name,
            entity_key_name=stmt.entity_key_name or "",
            entity_key_type=entity_key_type,
            is_temporal=stmt.temporal_column is not None,
            has_score=stmt.score_expression is not None,
            dependencies=list(stmt.dependencies),
            context_id=(
                existing.context_id if existing is not None else str(uuid.uuid4())
            ),
            namespace=stmt.namespace,
            version=version,
            definition_hash=stmt.definition_hash(),
            lifecycle_state="active",
            materialization=materialization,
            current_snapshot_version=None,
            description=stmt.description,
            tags=list(stmt.tags),
            definition_sql=stmt.definition_sql,
            composition=stmt.composition,
            score_expression=stmt.score_expression,
        )

        if stmt.definition_sql is not None:
            self._register_with_adapter(entry)
        self.catalog.contexts[key] = entry
        self._audit("create_context", stmt.name, version=version)
        return self._status_frame("create_context", stmt.name)

    def _check_dependencies(self, stmt: ContextDefinitionModel) -> None:
        own = stmt.name.lower()
        referenced: List[str] = []
        if stmt.composition is not None:
            referenced = [item.name for item in stmt.composition.items]
        else:
            referenced = list(stmt.dependencies)

        for ref in referenced:
            if ref.lower() == own:
                raise ValueError(
                    f"Context '{stmt.name}' references itself; "
                    "self-referential contexts are a dependency cycle."
                )
            if self.catalog.contexts.get(ref.lower()) is None:
                raise ValueError(
                    f"Context '{stmt.name}' depends on undefined context "
                    f"'{ref}'."
                )

        # Cycle check across the catalog graph with this definition applied.
        self._check_entry_cycles(
            ContextCatalogEntry(
                name=stmt.name,
                entity_key_name=stmt.entity_key_name or "",
                dependencies=list(referenced),
            )
        )

    def _resolve_key_type(self, entity_key_name: Optional[str]) -> EntityKeyType:
        if entity_key_name is None:
            return EntityKeyType.UNKNOWN
        for table in self.catalog.tables.values():
            pk = getattr(table, "primary_key_name", None)
            if pk is not None and pk.lower() == entity_key_name.lower():
                declared = getattr(table, "primary_key_type", None)
                if declared:
                    try:
                        return EntityKeyType(declared)
                    except ValueError:
                        return EntityKeyType.UNKNOWN
        return EntityKeyType.UNKNOWN

    def _register_with_adapter(self, entry: ContextCatalogEntry) -> None:
        score_column: Optional[str] = None
        if entry.score_expression and _IDENTIFIER_RE.match(entry.score_expression):
            score_column = entry.score_expression
        self.adapter.register_context(
            name=entry.name,
            sql=entry.definition_sql or "",
            entity_key_name=entry.entity_key_name,
            has_score=entry.has_score,
            score_column_name=score_column,
            replace=True,
        )

    # ------------------------------------------------------------------
    # ALTER
    # ------------------------------------------------------------------

    def _alter(self, stmt: AlterContextModel) -> pd.DataFrame:
        entry = self._require(stmt.name)
        key = stmt.name.lower()

        if stmt.action == "rename_to" and stmt.new_name:
            new_key = stmt.new_name.lower()
            if new_key != key and new_key in self.catalog.contexts:
                raise ValueError(
                    f"Cannot rename context '{stmt.name}' to "
                    f"'{stmt.new_name}': a context with that name already "
                    "exists."
                )
            dependents = self._transitive_dependents(key)
            if dependents:
                names = ", ".join(sorted(dependents))
                raise ValueError(
                    f"Cannot rename context '{stmt.name}': depended on by "
                    f"{names}. Drop or redefine the dependents first."
                )
            new_entry = replace(entry, name=stmt.new_name)
            del self.catalog.contexts[key]
            self.catalog.contexts[new_key] = new_entry
            if new_entry.definition_sql is not None:
                self.adapter.unregister_context(entry.name)
                self._register_with_adapter(new_entry)
        elif stmt.action == "set_definition" and stmt.definition_sql:
            new_dependencies = self._extract_sql_dependencies(
                stmt.definition_sql, own_name=entry.name
            )
            for dep in new_dependencies:
                if self.catalog.contexts.get(dep.lower()) is None:
                    raise ValueError(
                        f"Context '{entry.name}' new definition depends on "
                        f"undefined context '{dep}'."
                    )
            new_entry = replace(
                entry,
                definition_sql=stmt.definition_sql,
                dependencies=new_dependencies,
                version=entry.version + 1,
                current_snapshot_version=None,
            )
            self._check_entry_cycles(new_entry)
            new_entry.definition_hash = self._entry_hash(new_entry)
            self.catalog.contexts[key] = new_entry
            self._register_with_adapter(new_entry)
        elif stmt.action == "set_score":
            new_entry = replace(
                entry,
                score_expression=stmt.score_expression,
                has_score=stmt.score_expression is not None,
            )
            self.catalog.contexts[key] = new_entry
            if new_entry.definition_sql is not None:
                self._register_with_adapter(new_entry)
        elif stmt.action == "drop_score":
            new_entry = replace(entry, score_expression=None, has_score=False)
            self.catalog.contexts[key] = new_entry
            if new_entry.definition_sql is not None:
                self._register_with_adapter(new_entry)
        elif stmt.action == "set_description":
            self.catalog.contexts[key] = replace(
                entry, description=stmt.description
            )
        elif stmt.action == "set_tags":
            self.catalog.contexts[key] = replace(entry, tags=list(stmt.tags))
        elif stmt.action == "set_state" and stmt.state:
            state = stmt.state.lower()
            if state not in LIFECYCLE_STATES:
                raise ValueError(
                    f"Invalid lifecycle state {stmt.state!r}; expected one "
                    f"of {', '.join(sorted(LIFECYCLE_STATES))}."
                )
            self.catalog.contexts[key] = replace(
                entry, lifecycle_state=state
            )
        else:
            raise ValueError(
                f"Unsupported ALTER CONTEXT action: {stmt.action!r}"
            )

        self._audit("alter_context", stmt.name, alter_action=stmt.action)
        return self._status_frame("alter_context", stmt.name)

    def _extract_sql_dependencies(
        self, definition_sql: str, *, own_name: str
    ) -> List[str]:
        """Extract context references from a definition SELECT body."""
        from .parser import ContextQLParser
        from .semantic import SemanticLowerer

        text = definition_sql.strip()
        if not text.endswith(";"):
            text += ";"
        try:
            tree = ContextQLParser().parse(text)
        except Exception:
            return []
        lowerer = SemanticLowerer()
        dependencies: List[str] = []
        for inv in lowerer._iter_subtrees(tree, "context_invocation"):
            if not inv.children:
                continue
            first = inv.children[0]
            if getattr(first, "data", None) == "qualified_name":
                ref = lowerer._qualified_name_text(first)
                if (
                    ref.lower() != own_name.lower()
                    and ref not in dependencies
                ):
                    dependencies.append(ref)
        return dependencies

    def _check_entry_cycles(self, entry: ContextCatalogEntry) -> None:
        """Reject dependency cycles for an updated catalog entry."""
        graph = {
            name: [d.lower() for d in existing.dependencies]
            for name, existing in self.catalog.contexts.items()
        }
        graph[entry.name.lower()] = [d.lower() for d in entry.dependencies]
        seen: set = set()
        stack: List[str] = []

        def visit(node: str) -> None:
            if node in stack:
                cycle = " -> ".join(stack + [node])
                raise ValueError(
                    f"Dependency cycle detected involving context "
                    f"'{entry.name}': {cycle}"
                )
            if node in seen:
                return
            stack.append(node)
            for dep in graph.get(node, []):
                visit(dep)
            stack.pop()
            seen.add(node)

        visit(entry.name.lower())

    def _entry_hash(self, entry: ContextCatalogEntry) -> str:
        model = ContextDefinitionModel(
            kind="CREATE_CONTEXT",
            raw_sql="",
            name=entry.name,
            namespace=entry.namespace,
            entity_key_name=entry.entity_key_name,
            definition_sql=entry.definition_sql,
            composition=entry.composition,
            score_expression=entry.score_expression,
        )
        return model.definition_hash()

    # ------------------------------------------------------------------
    # DROP
    # ------------------------------------------------------------------

    def _drop(self, stmt: DropContextModel) -> pd.DataFrame:
        key = stmt.name.lower()
        entry = self.catalog.contexts.get(key)
        if entry is None:
            if stmt.if_exists:
                return self._status_frame("drop_context", stmt.name)
            raise ValueError(f"Context '{stmt.name}' does not exist.")

        dependents = self._transitive_dependents(key)
        if dependents and not stmt.cascade:
            names = ", ".join(sorted(dependents))
            raise ValueError(
                f"[{E159.code}] "
                + E159.format(name=stmt.name, dependents=names)
            )

        for dep_key in sorted(dependents):
            dep_entry = self.catalog.contexts.pop(dep_key, None)
            if dep_entry is not None:
                self.adapter.unregister_context(dep_entry.name)
                self._audit("drop_context", dep_entry.name, cascaded=True)
        self.catalog.contexts.pop(key, None)
        self.adapter.unregister_context(entry.name)
        self._audit("drop_context", stmt.name)
        return self._status_frame("drop_context", stmt.name)

    def _transitive_dependents(self, key: str) -> set:
        dependents: set = set()
        changed = True
        while changed:
            changed = False
            for name, entry in self.catalog.contexts.items():
                if name == key or name in dependents:
                    continue
                deps = {d.lower() for d in entry.dependencies}
                if key in deps or (deps & dependents):
                    dependents.add(name)
                    changed = True
        return dependents

    # ------------------------------------------------------------------
    # SHOW / DESCRIBE
    # ------------------------------------------------------------------

    def _show(self, stmt: ShowContextsModel) -> pd.DataFrame:
        rows = []
        for entry in sorted(
            self.catalog.contexts.values(), key=lambda e: e.name.lower()
        ):
            if stmt.like_pattern is not None:
                if not _like_to_regex(stmt.like_pattern).match(entry.name):
                    continue
            rows.append(
                {
                    "name": entry.name,
                    "namespace": entry.namespace,
                    "entity_key": entry.entity_key_name,
                    "lifecycle_state": entry.lifecycle_state,
                    "version": entry.version,
                    "materialized": entry.materialization.materialized,
                    "storage": entry.materialization.storage,
                    "has_score": entry.has_score,
                    "is_temporal": entry.is_temporal,
                }
            )
        columns = [
            "name", "namespace", "entity_key", "lifecycle_state", "version",
            "materialized", "storage", "has_score", "is_temporal",
        ]
        return pd.DataFrame(rows, columns=columns)

    def _describe(self, stmt: DescribeContextModel) -> pd.DataFrame:
        entry = self._require(stmt.name)
        return pd.DataFrame(
            [
                {
                    "name": entry.name,
                    "namespace": entry.namespace,
                    "entity_key": entry.entity_key_name,
                    "entity_key_type": str(entry.entity_key_type.value),
                    "lifecycle_state": entry.lifecycle_state,
                    "version": entry.version,
                    "definition_hash": entry.definition_hash,
                    "materialized": entry.materialization.materialized,
                    "storage": entry.materialization.storage,
                    "refresh_mode": entry.materialization.refresh_mode,
                    "description": entry.description,
                    "tags": ", ".join(entry.tags),
                    "score_expression": entry.score_expression,
                    "is_temporal": entry.is_temporal,
                    "dependencies": ", ".join(entry.dependencies),
                    "current_snapshot_version": entry.current_snapshot_version,
                    "last_validated_at": entry.last_validated_at,
                    "last_refreshed_at": entry.last_refreshed_at,
                }
            ]
        )

    # ------------------------------------------------------------------
    # VALIDATE / REFRESH
    # ------------------------------------------------------------------

    def _validate(self, stmt: ValidateContextModel) -> pd.DataFrame:
        entry = self._require(stmt.name)
        if entry.definition_sql is not None:
            try:
                cursor = self.adapter.conn.execute(
                    f"SELECT * FROM ({entry.definition_sql.rstrip(';')}) "
                    "AS _v LIMIT 0"
                )
                result_columns = {desc[0] for desc in cursor.description}
            except Exception as exc:
                raise ValueError(
                    f"Context '{stmt.name}' definition is invalid: {exc}"
                ) from exc
            if entry.entity_key_name and (
                entry.entity_key_name not in result_columns
            ):
                raise ValueError(
                    f"Context '{stmt.name}' definition does not return its "
                    f"entity key column '{entry.entity_key_name}'; "
                    f"result columns: {sorted(result_columns)}."
                )
            if entry.has_score and entry.score_expression and (
                entry.score_expression not in result_columns
            ):
                raise ValueError(
                    f"Context '{stmt.name}' SCORE column "
                    f"'{entry.score_expression}' is not returned by the "
                    f"definition; result columns: {sorted(result_columns)}."
                )
        elif entry.composition is not None:
            for item in entry.composition.items:
                if self.catalog.contexts.get(item.name.lower()) is None:
                    raise ValueError(
                        f"Context '{stmt.name}' composition references "
                        f"undefined context '{item.name}'."
                    )
        self.catalog.contexts[stmt.name.lower()] = replace(
            entry, last_validated_at=_now()
        )
        self._audit("validate_context", stmt.name)
        return self._status_frame("validate_context", stmt.name)

    def _refresh(self, stmt: RefreshContextModel) -> pd.DataFrame:
        if stmt.refresh_all:
            names = [entry.name for entry in self.catalog.contexts.values()]
        else:
            self._require(stmt.name or "")
            names = [stmt.name or ""]

        rows = []
        for name in names:
            try:
                self._refresh_one(name)
                rows.append(
                    {"status": "ok", "action": "refresh_context",
                     "context": name, "error": None}
                )
            except Exception as exc:
                # A single failing context must not abort REFRESH ALL;
                # per-context outcomes are reported instead (W101 model).
                if not stmt.refresh_all:
                    raise
                self._audit("refresh_context_failed", name, error=str(exc))
                rows.append(
                    {"status": "failed", "action": "refresh_context",
                     "context": name, "error": str(exc)}
                )
        return pd.DataFrame(
            rows, columns=["status", "action", "context", "error"]
        )

    def _refresh_one(self, name: str) -> None:
        entry = self._require(name)
        now = _now()
        if entry.materialization.materialized and entry.definition_sql:
            snapshot = self._build_snapshot(entry, now)
            self.catalog.contexts[name.lower()] = replace(
                entry,
                last_refreshed_at=now,
                data_as_of=now,
                current_snapshot_version=snapshot.version,
            )
        else:
            if entry.definition_sql is not None:
                # Force one evaluation so definition errors surface now.
                self.adapter.resolve_context_df(entry.name)
            self.catalog.contexts[name.lower()] = replace(
                entry, last_refreshed_at=now
            )
        self._audit("refresh_context", name)

    def _build_snapshot(self, entry: ContextCatalogEntry, now: datetime):
        """Full refresh: evaluate, build, atomically promote (plan 7.3).

        The store is only touched after successful evaluation, so a failing
        definition leaves the previous snapshot current (CS-7).
        """
        df = self.adapter.resolve_context_df(entry.name)
        key_column = entry.entity_key_name
        try:
            member_ids = [int(v) for v in df[key_column]]
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Context '{entry.name}' cannot be materialized: snapshot "
                f"membership requires integer entity keys (CS-9); column "
                f"'{key_column}' is not integer-valued."
            ) from exc

        scores: Dict[int, float] = {}
        score_column = entry.score_expression
        if entry.has_score and score_column and score_column in df.columns:
            scores = {
                int(k): float(v)
                for k, v in zip(df[key_column], df[score_column])
            }

        previous = self.membership.get_snapshot(entry.name)
        previous_members = (
            self.membership.members(entry.name) if previous else set()
        )
        previous_scores = (
            self.membership.scores(entry.name) if previous else {}
        )

        snapshot = self.membership.put_snapshot(
            entry.name,
            member_ids,
            computed_at=now,
            data_as_of=now,
            definition_hash=entry.definition_hash,
            source_watermark=entry.materialization.source_watermark,
            scores=scores,
        )

        if entry.materialization.history:
            self.history.append(
                derive_changes(
                    context_id=entry.name,
                    context_version=snapshot.version,
                    recorded_at=now,
                    effective_at=now,
                    previous_members=previous_members,
                    new_members=set(member_ids),
                    previous_scores=previous_scores,
                    new_scores=scores,
                )
            )
        return snapshot

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require(self, name: str) -> ContextCatalogEntry:
        entry = self.catalog.contexts.get(name.lower())
        if entry is None:
            raise ValueError(f"Context '{name}' does not exist.")
        return entry

    def _audit(self, action: str, context: str, **detail: Any) -> None:
        event = {
            "action": action,
            "context": context,
            "timestamp": _now().isoformat(),
            **detail,
        }
        for callback in self.audit_callbacks:
            # A failing audit callback must never make a committed DDL
            # operation look failed to the caller.
            try:
                callback(event)
            except Exception:
                logger.exception(
                    "DDL audit callback failed for %s on context '%s'",
                    action, context,
                )

    def _status_frame(self, action: str, context: str) -> pd.DataFrame:
        return pd.DataFrame(
            [{"status": "ok", "action": action, "context": context}]
        )
