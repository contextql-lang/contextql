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
import threading
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

from .context_options import resolve_materialization
from .context_options import parse_duration_seconds
from .catalog_repository import InMemoryCatalogRepository
from .context_names import (
    DEFAULT_NAMESPACE,
    context_catalog_key,
    qualify_context_name,
)
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
        repository=None,
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
        self.repository = (
            repository if repository is not None
            else InMemoryCatalogRepository()
        )
        self.audit_callbacks: List[AuditCallback] = []
        self.last_refresh_max_batch_size = 0
        self._refresh_locks: Dict[str, threading.Lock] = {}

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
        namespace = stmt.namespace or DEFAULT_NAMESPACE
        key = context_catalog_key(stmt.name, namespace)
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
            namespace=namespace,
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
            temporal_column=stmt.temporal_column,
            temporal_granularity=stmt.temporal_granularity,
        )

        if stmt.definition_sql is not None:
            self._register_with_adapter(entry)
        membership_key = entry.context_id or entry.qualified_name
        if hasattr(self.membership, "register_alias"):
            self.membership.register_alias(
                entry.qualified_name, membership_key
            )
        if hasattr(self.history, "register_alias"):
            self.history.register_alias(
                entry.qualified_name, membership_key
            )
        self.catalog.put_context(entry)
        self.repository.save_context(entry, raw_ddl=stmt.raw_sql)
        self._audit(
            "create_context",
            entry.qualified_name,
            context_id=entry.context_id,
            version=version,
            definition_hash=entry.definition_hash,
        )
        return self._status_frame("create_context", entry.qualified_name)

    def _check_dependencies(self, stmt: ContextDefinitionModel) -> None:
        namespace = stmt.namespace or DEFAULT_NAMESPACE
        own = context_catalog_key(stmt.name, namespace)
        referenced: List[str] = []
        if stmt.composition is not None:
            referenced = [item.name for item in stmt.composition.items]
        else:
            referenced = list(stmt.dependencies)

        for ref in referenced:
            ref_key = context_catalog_key(
                ref, default_namespace=self.catalog.default_namespace
            )
            if ref_key == own:
                raise ValueError(
                    f"Context '{stmt.name}' references itself; "
                    "self-referential contexts are a dependency cycle."
                )
            if self.catalog.contexts.get(ref_key) is None:
                raise ValueError(
                    f"Context '{stmt.name}' depends on undefined context "
                    f"'{ref}'."
                )

        # Cycle check across the catalog graph with this definition applied.
        self._check_entry_cycles(
            ContextCatalogEntry(
                name=stmt.name,
                namespace=namespace,
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
            name=entry.qualified_name,
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
        key = entry.catalog_key

        if stmt.action == "rename_to" and stmt.new_name:
            new_qualified = qualify_context_name(
                stmt.new_name, default_namespace=entry.namespace
            )
            new_key = new_qualified.key
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
            new_entry = replace(
                entry,
                name=new_qualified.name,
                namespace=new_qualified.namespace,
                version=entry.version + 1,
            )
            del self.catalog.contexts[key]
            self.catalog.contexts[new_key] = new_entry
            if hasattr(self.membership, "unregister_alias"):
                self.membership.unregister_alias(entry.qualified_name)
                self.membership.register_alias(
                    new_entry.qualified_name,
                    new_entry.context_id or new_entry.qualified_name,
                )
            if hasattr(self.history, "unregister_alias"):
                self.history.unregister_alias(entry.qualified_name)
                self.history.register_alias(
                    new_entry.qualified_name,
                    new_entry.context_id or new_entry.qualified_name,
                )
            if new_entry.definition_sql is not None:
                self.adapter.unregister_context(entry.qualified_name)
                self._register_with_adapter(new_entry)
        elif stmt.action == "set_definition" and stmt.definition_sql:
            new_dependencies = self._extract_sql_dependencies(
                stmt.definition_sql, own_name=entry.name
            )
            for dep in new_dependencies:
                if self.catalog.get_context(dep) is None:
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
                version=entry.version + 1,
                current_snapshot_version=None,
            )
            new_entry.definition_hash = self._entry_hash(new_entry)
            self.catalog.contexts[key] = new_entry
            if new_entry.definition_sql is not None:
                self._register_with_adapter(new_entry)
        elif stmt.action == "drop_score":
            new_entry = replace(
                entry,
                score_expression=None,
                has_score=False,
                version=entry.version + 1,
                current_snapshot_version=None,
            )
            new_entry.definition_hash = self._entry_hash(new_entry)
            self.catalog.contexts[key] = new_entry
            if new_entry.definition_sql is not None:
                self._register_with_adapter(new_entry)
        elif stmt.action == "set_description":
            self.catalog.contexts[key] = replace(
                entry,
                description=stmt.description,
                version=entry.version + 1,
            )
        elif stmt.action == "set_tags":
            self.catalog.contexts[key] = replace(
                entry,
                tags=list(stmt.tags),
                version=entry.version + 1,
            )
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

        updated = self.catalog.contexts.get(new_key if (
            stmt.action == "rename_to" and stmt.new_name
        ) else key)
        if updated is not None:
            self.repository.save_context(
                updated,
                raw_ddl=(
                    None if stmt.action == "set_state" else stmt.raw_sql
                ),
            )
        self._audit(
            "alter_context",
            updated.qualified_name if updated is not None else stmt.name,
            context_id=entry.context_id,
            alter_action=stmt.action,
            version=updated.version if updated is not None else entry.version,
            definition_hash=(
                updated.definition_hash if updated is not None
                else entry.definition_hash
            ),
        )
        return self._status_frame(
            "alter_context",
            updated.qualified_name if updated is not None else stmt.name,
        )

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
            name: [
                context_catalog_key(
                    d, default_namespace=self.catalog.default_namespace
                )
                for d in existing.dependencies
            ]
            for name, existing in self.catalog.contexts.items()
        }
        graph[entry.catalog_key] = [
            context_catalog_key(
                d, default_namespace=self.catalog.default_namespace
            )
            for d in entry.dependencies
        ]
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

        visit(entry.catalog_key)

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
        entry = self.catalog.get_context(stmt.name)
        if entry is None:
            if stmt.if_exists:
                return self._status_frame("drop_context", stmt.name)
            raise ValueError(f"Context '{stmt.name}' does not exist.")

        key = entry.catalog_key
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
                if hasattr(self.membership, "unregister_alias"):
                    self.membership.unregister_alias(dep_entry.qualified_name)
                if hasattr(self.history, "unregister_alias"):
                    self.history.unregister_alias(dep_entry.qualified_name)
                self.adapter.unregister_context(dep_entry.qualified_name)
                self._audit(
                    "drop_context",
                    dep_entry.qualified_name,
                    context_id=dep_entry.context_id,
                    cascaded=True,
                )
                self.repository.drop_context(dep_entry)
        self.catalog.contexts.pop(key, None)
        if hasattr(self.membership, "unregister_alias"):
            self.membership.unregister_alias(entry.qualified_name)
        if hasattr(self.history, "unregister_alias"):
            self.history.unregister_alias(entry.qualified_name)
        self.adapter.unregister_context(entry.qualified_name)
        self._audit(
            "drop_context",
            entry.qualified_name,
            context_id=entry.context_id,
        )
        self.repository.drop_context(entry)
        return self._status_frame("drop_context", stmt.name)

    def _transitive_dependents(self, key: str) -> set:
        dependents: set = set()
        changed = True
        while changed:
            changed = False
            for name, entry in self.catalog.contexts.items():
                if name == key or name in dependents:
                    continue
                deps = {
                    context_catalog_key(
                        d, default_namespace=self.catalog.default_namespace
                    )
                    for d in entry.dependencies
                }
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
                if self.catalog.get_context(item.name) is None:
                    raise ValueError(
                        f"Context '{stmt.name}' composition references "
                        f"undefined context '{item.name}'."
                    )
        updated_entry = replace(
            entry, last_validated_at=_now()
        )
        self.catalog.contexts[entry.catalog_key] = updated_entry
        self.repository.save_context(updated_entry)
        self._audit("validate_context", stmt.name)
        return self._status_frame("validate_context", stmt.name)

    def _refresh(self, stmt: RefreshContextModel) -> pd.DataFrame:
        if stmt.refresh_all:
            names = [
                entry.qualified_name
                for entry in self.catalog.contexts.values()
            ]
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
        lock = self._refresh_locks.setdefault(
            entry.context_id, threading.Lock()
        )
        with lock:
            self._refresh_one_locked(name)

    def _refresh_one_locked(self, name: str) -> None:
        entry = self._require(name)
        now = _now()
        if entry.materialization.materialized and entry.definition_sql:
            staged, history_events = self._build_snapshot(entry, now)
            snapshot = staged.snapshot
            updated_entry = replace(
                entry,
                last_refreshed_at=now,
                data_as_of=now,
                current_snapshot_version=snapshot.version,
                last_refresh_error=None,
            )
            try:
                self.repository.promote_snapshot(
                    updated_entry,
                    snapshot,
                    membership_payload=self.membership.serialize_staged(
                        staged
                    ),
                    scores=staged.scores,
                    history_events=history_events,
                )
            except Exception:
                raise
            self.membership.commit_snapshot(staged)
            self.history.append(history_events)
            self.catalog.contexts[entry.catalog_key] = updated_entry
            retention = parse_duration_seconds(
                entry.materialization.history_retention
            )
            if entry.materialization.history and retention is not None:
                from datetime import timedelta
                cutoff = now - timedelta(seconds=retention)
                anchor = self.membership.prune_before(
                    entry.context_id, cutoff
                )
                if anchor is not None:
                    self.history.prune_before(
                        entry.context_id, anchor.data_as_of
                    )
                    retained_entry = replace(
                        updated_entry,
                        history_available_from=anchor.data_as_of,
                    )
                    self.catalog.contexts[
                        entry.catalog_key
                    ] = retained_entry
                    self.repository.prune_history(
                        retained_entry, anchor.data_as_of
                    )
                    self.repository.save_context(retained_entry)
        else:
            if entry.definition_sql is not None:
                # Force one evaluation so definition errors surface now.
                self.adapter.resolve_context_df(entry.qualified_name)
            updated_entry = replace(
                entry, last_refreshed_at=now, last_refresh_error=None
            )
            self.catalog.contexts[entry.catalog_key] = updated_entry
            self.repository.save_context(updated_entry)
        self._audit("refresh_context", name)

    def record_refresh_failure(self, name: str, detail: str) -> None:
        entry = self._require(name)
        updated = replace(entry, last_refresh_error=detail)
        self.catalog.contexts[entry.catalog_key] = updated
        self.repository.save_context(updated)
        self._audit(
            "refresh_context_failed",
            entry.qualified_name,
            context_id=entry.context_id,
            error=detail,
        )

    def _build_snapshot(self, entry: ContextCatalogEntry, now: datetime):
        """Full refresh: evaluate, build, atomically promote (plan 7.3).

        The store is only touched after successful evaluation, so a failing
        definition leaves the previous snapshot current (CS-7).
        """
        key_column = entry.entity_key_name
        resolved_storage = self.membership.resolve_storage_kind(
            entry.materialization.storage
        )
        if resolved_storage == "roaring":
            from pyroaring import BitMap64
            member_ids = BitMap64()
        else:
            member_ids = set()
        scores: Dict[int, float] = {}
        effective_times: Dict[int, datetime] = {}
        score_column = entry.score_expression
        watermark_column = entry.materialization.source_watermark
        observed_values = []
        definition_sql = (entry.definition_sql or "").rstrip(";")
        try:
            for batch in self.adapter.execute_batches(definition_sql):
                self.last_refresh_max_batch_size = max(
                    self.last_refresh_max_batch_size, len(batch)
                )
                if key_column not in batch.columns:
                    raise ValueError(
                        f"definition does not return entity key '{key_column}'"
                    )
                batch_ids = [int(value) for value in batch[key_column]]
                if any(value < 0 for value in batch_ids):
                    raise ValueError("entity IDs must be non-negative")
                if resolved_storage == "roaring":
                    member_ids |= BitMap64(batch_ids)
                else:
                    member_ids.update(batch_ids)
                if (
                    entry.has_score and score_column
                    and score_column in batch.columns
                ):
                    scores.update(
                        (int(key), float(value))
                        for key, value in zip(
                            batch[key_column], batch[score_column]
                        )
                    )
                if entry.is_temporal and entry.temporal_column:
                    if entry.temporal_column not in batch.columns:
                        raise ValueError(
                            f"TEMPORAL column '{entry.temporal_column}' "
                            "is not returned by the definition"
                        )
                    from contextql.temporal import floor_utc

                    temporal_values = []
                    for raw_value in batch[entry.temporal_column]:
                        timestamp = pd.Timestamp(raw_value)
                        if (
                            timestamp.tzinfo is None
                            or timestamp.utcoffset() is None
                        ):
                            raise ValueError(
                                "TEMPORAL values must include an explicit "
                                "timezone"
                            )
                        temporal_values.append(
                            floor_utc(
                                timestamp.to_pydatetime(),
                                entry.temporal_granularity,
                            )
                        )
                    effective_times.update(
                        (
                            int(key),
                            value,
                        )
                        for key, value in zip(
                            batch[key_column], temporal_values
                        )
                    )
                if watermark_column:
                    if watermark_column not in batch.columns:
                        raise ValueError(
                            f"source_watermark column '{watermark_column}' "
                            "is not returned by the definition"
                        )
                    non_null = batch[watermark_column].dropna()
                    if not non_null.empty:
                        observed_values.append(non_null.max())
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"Context '{entry.qualified_name}' cannot be materialized: "
                f"{exc}."
            ) from exc

        membership_key = entry.context_id or entry.qualified_name
        previous = self.membership.get_snapshot(membership_key)
        previous_members = set()
        previous_scores: Dict[int, float] = {}
        if entry.materialization.history and previous is not None:
            previous_members = self.membership.membership_object(
                membership_key, previous.version
            )
            previous_scores = self.membership.scores(
                membership_key, previous.version
            )

        observed_watermark = (
            previous.source_watermark if previous is not None else None
        )
        if watermark_column:
            if observed_values:
                value = max(observed_values)
                observed_watermark = (
                    value.isoformat() if hasattr(value, "isoformat")
                    else str(value)
                )

        staged = self.membership.stage_snapshot(
            membership_key,
            member_ids,
            computed_at=now,
            data_as_of=now,
            definition_hash=entry.definition_hash,
            source_watermark=observed_watermark,
            scores=scores,
            storage_kind=resolved_storage,
        )

        history_events = []
        if entry.materialization.history:
            history_events = derive_changes(
                context_id=membership_key,
                context_version=staged.snapshot.version,
                recorded_at=now,
                effective_at=now,
                previous_members=previous_members,
                new_members=staged.membership,
                previous_scores=previous_scores,
                new_scores=scores,
                effective_times=effective_times,
            )
        return staged, history_events

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require(self, name: str) -> ContextCatalogEntry:
        entry = self.catalog.get_context(name)
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
