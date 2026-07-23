# contextql/executor.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import pandas as pd

import numpy as np

from contextql.adapters.duckdb_adapter import DuckDBAdapter
from contextql.context_ddl import ContextDDLExecutor
from contextql.context_options import parse_duration_seconds
from contextql.errors import Severity
from contextql.semantic import (
    AlterContextModel,
    AnalysisResult,
    ContextDefinitionModel,
    ContextPredicate,
    ContextReference,
    DescribeContextModel,
    DropContextModel,
    InMemoryCatalog,
    QueryModel,
    RefreshContextModel,
    ShowContextsModel,
    TableRef,
    ValidateContextModel,
    analyze_sql,
)

DDL_STATEMENT_TYPES = (
    ContextDefinitionModel,
    AlterContextModel,
    DropContextModel,
    ShowContextsModel,
    DescribeContextModel,
    RefreshContextModel,
    ValidateContextModel,
)


@dataclass
class ProviderCall:
    """Record of a single provider invocation during execution."""
    provider_name: str
    provider_type: str  # "MCP" or "REMOTE"
    entity_count: int
    elapsed_ms: float
    data_as_of: Optional[str] = None


@dataclass
class ContextTrace:
    """Execution trace capturing provenance of context resolution."""
    contexts_resolved: List[str] = field(default_factory=list)
    provider_calls: List[ProviderCall] = field(default_factory=list)
    identity_maps_used: List[str] = field(default_factory=list)
    score_breakdown: Dict = field(default_factory=dict)


@dataclass
class ExecutionResult:
    dataframe: pd.DataFrame
    generated_sql: str
    analysis: AnalysisResult
    trace: Optional[ContextTrace] = None


class ContextQLExecutor:
    """
    ContextQL executor backed by DuckDB.

    Execution strategy for v0.3:
    1. Parse + semantically analyze the query
    2. Lower the base query (FROM/JOIN/non-context WHERE) into SQL
    3. Execute base SQL in DuckDB
    4. Resolve contexts through the adapter
    5. Apply context membership + scoring in Python over the result DataFrame
    6. Apply ORDER BY CONTEXT / LIMIT

    This is intentionally hybrid:
    - SQL work is done in DuckDB
    - Context algebra is done in ContextQL space
    """

    def __init__(
        self,
        catalog: InMemoryCatalog,
        adapter: DuckDBAdapter,
        mcp_providers: Optional[Dict] = None,
        mcp_entity_keys: Optional[Dict[str, str]] = None,
        remote_providers: Optional[Dict] = None,
        identity_maps: Optional[Dict] = None,
        mcp_timeout_ms: int = 30000,
        remote_timeout_ms: int = 30000,
        mcp_timeout_behavior: str = "warn",
    ):
        self.catalog = catalog
        self.adapter = adapter
        self._mcp_providers: Dict = mcp_providers if mcp_providers is not None else {}
        self._mcp_entity_keys: Dict[str, str] = mcp_entity_keys if mcp_entity_keys is not None else {}
        self._remote_providers: Dict = remote_providers if remote_providers is not None else {}
        self._identity_maps: Dict = identity_maps if identity_maps is not None else {}
        self._mcp_timeout_ms = mcp_timeout_ms
        self._remote_timeout_ms = remote_timeout_ms
        self._mcp_timeout_behavior = mcp_timeout_behavior
        self._mcp_result_cache: Dict = {}
        self.ddl = ContextDDLExecutor(catalog=catalog, adapter=adapter)
        self.membership = self.ddl.membership
        self.history = self.ddl.history

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------

    def execute_sql(self, sql: str) -> ExecutionResult:
        analysis = analyze_sql(sql, self.catalog)

        if not analysis.ok:
            raise ValueError("\n".join(str(d) for d in analysis.diagnostics))

        if not analysis.statements:
            raise ValueError("No statements found.")

        stmt = analysis.statements[0]
        self._pending_warnings: List = []
        if isinstance(stmt, DDL_STATEMENT_TYPES):
            df = self.ddl.execute(stmt)
            return ExecutionResult(
                dataframe=df,
                generated_sql="",
                analysis=analysis,
                trace=None,
            )
        if not isinstance(stmt, QueryModel):
            raise ValueError(
                "Executor supports SELECT queries and context DDL statements."
            )

        df, generated_sql = self._execute_query(stmt)

        if self._pending_warnings:
            analysis.diagnostics.extend(self._pending_warnings)

        trace = getattr(self, '_trace', None)
        return ExecutionResult(
            dataframe=df,
            generated_sql=generated_sql,
            analysis=analysis,
            trace=trace,
        )

    # ---------------------------------------------------------
    # Main query execution
    # ---------------------------------------------------------

    def _execute_query(self, query: QueryModel) -> Tuple[pd.DataFrame, str]:
        # Clear per-query MCP result cache and init trace
        self._mcp_result_cache = {}
        self._trace = ContextTrace()

        # Snapshot-state gate: E200 for materialized-without-snapshot,
        # W100 for stale snapshots (SPEC section 6).
        self._check_snapshot_states(query)

        # Materialise REMOTE sources before building SQL
        temp_tables = self._materialize_remote_sources(query)
        member_tables: List[str] = []
        try:
            extra_key_cols = self._collect_extra_key_cols(query)
            context_where = self._prepare_bitmap_pushdown(query, member_tables)
            base_sql = self._build_base_sql(
                query, extra_key_cols, context_where=context_where
            )
            df = self.adapter.execute_df(base_sql)

            if query.context_predicates and context_where is None:
                df = self._apply_context_filters(df, query)

            if query.uses_context_score or any(item.is_context_order for item in query.order_items):
                df = self._apply_context_scoring(df, query)

            # ORDER BY before renaming so __context_score is still available
            df = self._apply_order(df, query)

            if query.limit is not None:
                df = df.head(query.limit)

            if query.offset is not None:
                df = df.iloc[query.offset :]

            # Drop key columns that were added for context resolution but not in user's SELECT
            drop_cols = [c for c in extra_key_cols if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

            # Rename internal score/count columns to user aliases and drop unused internals
            df = self._apply_projection_aliases(df, query)

            return df.reset_index(drop=True), base_sql
        finally:
            for t in temp_tables + member_tables:
                try:
                    self.adapter.conn.unregister(t)
                except Exception:
                    pass

    # ---------------------------------------------------------
    # Bitmap snapshot pushdown (plan 7.4, CS-11)
    # ---------------------------------------------------------

    def _snapshot_entry(self, name: str):
        """Catalog entry if *name* is a materialized plain context."""
        entry = self.catalog.contexts.get(name.lower())
        if entry is None:
            return None
        materialization = getattr(entry, "materialization", None)
        if materialization is None or not materialization.materialized:
            return None
        return entry

    def _check_snapshot_states(self, query: QueryModel) -> None:
        """Enforce SPEC section 6 snapshot-state behavior (E200 / W100)."""
        from contextql.semantic import SemanticDiagnostic
        from datetime import datetime, timezone

        for pred in query.context_predicates:
            for ref in pred.refs:
                if ref.source_kind in ("MCP", "REMOTE"):
                    continue
                entry = self._snapshot_entry(ref.name)
                if entry is None:
                    continue
                snapshot = self.membership.get_snapshot(ref.name)
                if snapshot is None:
                    raise ValueError(
                        f"[E200] context '{ref.name}' is materialized but "
                        f"has no current snapshot; run REFRESH CONTEXT "
                        f"{ref.name}."
                    )
                stale_after = entry.materialization.stale_after
                threshold = parse_duration_seconds(stale_after)
                if threshold is not None:
                    age = (
                        datetime.now(timezone.utc) - snapshot.computed_at
                    ).total_seconds()
                    if age > threshold:
                        self._pending_warnings.append(
                            SemanticDiagnostic(
                                code="W100",
                                severity=Severity.WARNING,
                                message=(
                                    f"context '{ref.name}' snapshot is "
                                    f"stale (age {int(age)}s, stale_after "
                                    f"{stale_after})."
                                ),
                                hint=f"Run REFRESH CONTEXT {ref.name}.",
                            )
                        )

    def _prepare_bitmap_pushdown(
        self, query: QueryModel, member_tables: List[str]
    ) -> Optional[str]:
        """Build a SQL membership predicate from context snapshots.

        Returns a WHERE fragment when every referenced context is a
        materialized plain context with a current snapshot sharing the FROM
        table's primary key; otherwise ``None`` (legacy in-Python path).
        Registers per-predicate member relations and appends their names to
        *member_tables* for cleanup.
        """
        if not query.context_predicates or not query.from_table:
            return None

        table_entry = self.catalog.tables.get(query.from_table.name.lower())
        if table_entry is None or not table_entry.primary_key_name:
            return None
        primary_key = table_entry.primary_key_name

        entries: Dict[str, object] = {}
        for pred in query.context_predicates:
            for ref in pred.refs:
                if ref.source_kind in ("MCP", "REMOTE"):
                    return None
                entry = self._snapshot_entry(ref.name)
                if entry is None:
                    return None
                if entry.entity_key_name.lower() != primary_key.lower():
                    return None
                entries[ref.name] = entry

        key_qualifier = query.from_table.alias or query.from_table.name
        clauses: List[str] = []
        for index, pred in enumerate(query.context_predicates):
            names = [ref.name for ref in pred.refs]
            intersect = bool(pred.all_mode or pred.sequence_mode)
            member_array = self._compose_member_array(names, intersect)
            table_name = f"__cql_members_{index}"
            self.adapter.conn.register(
                table_name, pd.DataFrame({"entity_id": member_array})
            )
            member_tables.append(table_name)
            operator = "NOT IN" if pred.negated else "IN"
            clauses.append(
                f"{key_qualifier}.{primary_key} {operator} "
                f"(SELECT entity_id FROM {table_name})"
            )
            for name in names:
                version = self.membership.get_snapshot(name).version
                label = f"{name}@v{version}"
                if label not in self._trace.contexts_resolved:
                    self._trace.contexts_resolved.append(label)

        return " AND ".join(clauses)

    def _compose_member_array(
        self, names: List[str], intersect: bool
    ) -> "np.ndarray":
        """Compose snapshot memberships without Python-set expansion when
        the store supports bitmap-native algebra."""
        if hasattr(self.membership, "compose"):
            if intersect:
                bitmap = self.membership.compose(intersect_of=names)
            else:
                bitmap = self.membership.compose(union_of=names)
            return np.asarray(bitmap.to_array(), dtype="int64")
        members = (
            self.membership.intersect(names)
            if intersect
            else self.membership.union(names)
        )
        return np.fromiter(members, dtype="int64", count=len(members))

    # ---------------------------------------------------------
    # REMOTE source materialisation
    # ---------------------------------------------------------

    def _materialize_remote_sources(self, query: QueryModel) -> List[str]:
        """Fetch REMOTE table refs and register them as DuckDB views.

        Returns the list of registered temp names so the caller can unregister
        them in a ``finally`` block after execution.
        """
        temp_names: List[str] = []

        all_refs: List[TableRef] = []
        if query.from_table is not None:
            all_refs.append(query.from_table)
        for j in query.joins or []:
            all_refs.append(j.table)

        for ref in all_refs:
            if ref.source_kind != "REMOTE":
                continue

            provider_name, _, resource = ref.name.partition(".")
            if not resource:
                raise ValueError(
                    f"REMOTE source '{ref.name}' must be qualified as "
                    "provider.resource"
                )

            provider = self._remote_providers.get(provider_name)
            if provider is None:
                raise ValueError(
                    f"REMOTE provider '{provider_name}' is not registered. "
                    "Call register_remote_provider() before executing queries "
                    "that reference it."
                )

            from concurrent.futures import ThreadPoolExecutor
            from concurrent.futures import TimeoutError as FuturesTimeoutError

            _remote_t0 = __import__('time').perf_counter()
            with ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    provider.query,
                    resource=resource,
                    filters={},
                    columns=[],
                    limit=None,
                )
                try:
                    remote_result = future.result(
                        timeout=self._remote_timeout_ms / 1000
                    )
                except FuturesTimeoutError:
                    raise RuntimeError(
                        f"REMOTE provider '{provider_name}' timed out after "
                        f"{self._remote_timeout_ms}ms"
                    )

            remote_df = remote_result.to_dataframe()
            _remote_elapsed = (__import__('time').perf_counter() - _remote_t0) * 1000
            if hasattr(self, '_trace'):
                self._trace.provider_calls.append(ProviderCall(
                    provider_name=provider_name,
                    provider_type="REMOTE",
                    entity_count=len(remote_df),
                    elapsed_ms=_remote_elapsed,
                ))
            temp_name = f"__remote_{provider_name}_{resource.replace('.', '_')}"
            self.adapter.conn.register(temp_name, remote_df)

            # Mutate the ref in-place so _build_base_sql uses the temp name
            ref.name = temp_name
            ref.source_kind = "TABLE"
            temp_names.append(temp_name)

        return temp_names

    # ---------------------------------------------------------
    # SQL lowering
    # ---------------------------------------------------------

    def _collect_extra_key_cols(self, query: QueryModel) -> Dict[str, str]:
        """Return {key_col_name: select_expr} for context key columns not already projected."""
        # SELECT * already includes all columns — nothing to inject
        if query.projections == ["*"]:
            return {}
        proj_text = " ".join(query.projections).lower()
        # Determine what columns the FROM table actually has (for identity map lookup)
        from_table_name = query.from_table.name.lower() if query.from_table else ""
        from_table_df = self.adapter._tables.get(from_table_name)
        from_cols: set = set(from_table_df.columns.str.lower()) if from_table_df is not None else set()
        extra: Dict[str, str] = {}
        for pred in query.context_predicates:
            for ref in pred.refs:
                if ref.source_kind == "MCP":
                    # MCP key resolution uses the registered entity_key first,
                    # then the catalog PK, then identity maps.  Inject the
                    # resolved column if not already projected.
                    registered_key = self._mcp_entity_keys.get(ref.name)
                    if registered_key is not None:
                        key_name = registered_key
                    else:
                        if not query.from_table:
                            continue
                        tbl = self.catalog.tables.get(query.from_table.name.lower())
                        if not tbl or not tbl.primary_key_name:
                            continue
                        key_name = tbl.primary_key_name
                    if key_name.lower() in proj_text or key_name in extra:
                        continue
                    if key_name.lower() in from_cols:
                        effective_key = key_name
                    else:
                        mapped = self._find_identity_mapped_col(key_name, from_cols)
                        if mapped is None:
                            continue
                        effective_key = mapped
                    if effective_key.lower() in proj_text or effective_key in extra:
                        continue
                    if pred.binding_alias:
                        extra[effective_key] = f"{pred.binding_alias}.{effective_key}"
                    else:
                        extra[effective_key] = effective_key
                    continue
                try:
                    ctx = self.adapter.get_context(ref.name)
                except KeyError:
                    continue
                key_name = ctx.entity_key_name
                if key_name.lower() in proj_text or key_name in extra:
                    continue
                # If the context entity key exists in the FROM table, inject it directly.
                # If not, check identity maps for a column that maps to this key.
                # If neither exists, skip — _resolve_dataframe_key_column will raise a
                # helpful ValueError mentioning register_identity_map.
                if key_name.lower() in from_cols:
                    effective_key = key_name
                else:
                    mapped = self._find_identity_mapped_col(key_name, from_cols)
                    if mapped is None:
                        continue
                    effective_key = mapped
                    # Record identity map usage in trace
                    if hasattr(self, '_trace'):
                        map_desc = f"{key_name} -> {mapped}"
                        if map_desc not in self._trace.identity_maps_used:
                            self._trace.identity_maps_used.append(map_desc)
                if effective_key.lower() in proj_text or effective_key in extra:
                    continue
                if pred.binding_alias:
                    extra[effective_key] = f"{pred.binding_alias}.{effective_key}"
                else:
                    extra[effective_key] = effective_key
        return extra

    def _find_identity_mapped_col(self, ctx_entity_key: str, available_cols: set) -> Optional[str]:
        """Return a column from available_cols that maps to ctx_entity_key via identity maps.

        # TODO(v0.3): Identity maps use string matching only — no type
        # coercion or schema validation (Whitepaper Section 23).
        """
        for mapping in self._identity_maps.values():
            for col_a, col_b in mapping.items():
                _, _, a_col = col_a.rpartition(".")
                _, _, b_col = col_b.rpartition(".")
                if b_col == ctx_entity_key and a_col.lower() in available_cols:
                    return a_col
                if a_col == ctx_entity_key and b_col.lower() in available_cols:
                    return b_col
        return None

    def _build_base_sql(
        self,
        query: QueryModel,
        extra_key_cols: Optional[Dict[str, str]] = None,
        context_where: Optional[str] = None,
    ) -> str:
        if not query.from_table:
            raise ValueError("Query has no FROM table.")

        projections = self._projection_sql(query)
        if extra_key_cols:
            projections += ", " + ", ".join(extra_key_cols.values())
        from_sql = self._from_sql(query.from_table)
        joins_sql = self._joins_sql(query)

        non_context_where = self._strip_context_predicates(query.where_text or "")

        parts = [
            f"SELECT {projections}",
            f"FROM {from_sql}",
        ]

        if joins_sql:
            parts.append(joins_sql)

        where_clauses = [
            clause for clause in (non_context_where, context_where) if clause
        ]
        if where_clauses:
            parts.append(
                "WHERE " + " AND ".join(f"({c})" for c in where_clauses)
            )

        if query.group_by:
            parts.append(f"GROUP BY {query.group_by}")

        if query.having:
            parts.append(f"HAVING {query.having}")

        # Intentionally do NOT push ORDER BY CONTEXT here
        # Non-context ORDER BY pushdown can be added later.
        return "\n".join(parts)

    def _projection_sql(self, query: QueryModel) -> str:
        projections: List[str] = []

        for raw in query.projections:
            proj = raw.strip()

            if "CONTEXT_SCORE()" in proj.upper():
                projections.append("NULL AS __context_score_placeholder")
            elif "CONTEXT_COUNT()" in proj.upper():
                projections.append("NULL AS __context_count_placeholder")
            else:
                projections.append(proj)

        return ", ".join(projections) if projections else "*"

    def _from_sql(self, table: TableRef) -> str:
        if table.alias:
            return f"{table.name} AS {table.alias}"
        return table.name

    def _joins_sql(self, query: QueryModel) -> str:
        if not query.joins:
            return ""

        chunks: List[str] = []
        for join in query.joins:
            table_sql = join.table.name
            if join.table.alias:
                table_sql += f" AS {join.table.alias}"

            if join.condition:
                chunks.append(f"{join.join_type} {table_sql} ON {join.condition}")
            else:
                chunks.append(f"{join.join_type} {table_sql}")

        return "\n".join(chunks)

    def _strip_context_predicates(self, where_text: str) -> str:
        """
        Remove CONTEXT predicates from the WHERE clause so the remaining SQL
        can be executed directly in DuckDB.

        This is intentionally simple for v0.3 and assumes:
        - context predicates appear as top-level AND terms
        - non-context predicates remain valid SQL
        """
        if not where_text.strip():
            return ""

        import re
        _context_pred_re = re.compile(
            r'\bCONTEXT\s+(?:ON\s+\w+\s+)?(?:NOT\s+)?IN\b', re.IGNORECASE)
        parts = self._split_top_level_and(where_text)
        kept = [p for p in parts if not _context_pred_re.search(p)]
        return " AND ".join(kept).strip()

    def _split_top_level_and(self, text: str) -> List[str]:
        parts: List[str] = []
        current: List[str] = []
        depth = 0
        in_single = False
        in_double = False
        i = 0

        while i < len(text):
            ch = text[i]

            if ch == "'" and not in_double:
                in_single = not in_single
                current.append(ch)
                i += 1
                continue

            if ch == '"' and not in_single:
                in_double = not in_double
                current.append(ch)
                i += 1
                continue

            if in_single or in_double:
                current.append(ch)
                i += 1
                continue

            if ch == "(":
                depth += 1
                current.append(ch)
                i += 1
                continue

            if ch == ")":
                depth = max(0, depth - 1)
                current.append(ch)
                i += 1
                continue

            if depth == 0 and text[i : i + 3].upper() == "AND":
                before_ok = i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")
                after_idx = i + 3
                after_ok = after_idx >= len(text) or not (
                    text[after_idx].isalnum() or text[after_idx] == "_"
                )
                if before_ok and after_ok:
                    part = "".join(current).strip()
                    if part:
                        parts.append(part)
                    current = []
                    i += 3
                    continue

            current.append(ch)
            i += 1

        tail = "".join(current).strip()
        if tail:
            parts.append(tail)

        return parts

    # ---------------------------------------------------------
    # Context membership
    # ---------------------------------------------------------

    def _apply_context_filters(self, df: pd.DataFrame, query: QueryModel) -> pd.DataFrame:
        if df.empty:
            return df

        overall_mask = pd.Series([True] * len(df), index=df.index)

        for pred in query.context_predicates:
            pred_mask = self._evaluate_context_predicate(df, query, pred)
            overall_mask = overall_mask & pred_mask

        return df.loc[overall_mask].copy()

    def _evaluate_context_predicate(
        self,
        df: pd.DataFrame,
        query: QueryModel,
        pred: ContextPredicate,
    ) -> pd.Series:
        if not pred.refs:
            return pd.Series([True] * len(df), index=df.index)

        ref_masks = [
            self._evaluate_single_context(df, query, pred, ref)
            for ref in pred.refs
        ]

        if pred.all_mode or pred.sequence_mode:
            combined = ref_masks[0]
            for mask in ref_masks[1:]:
                combined = combined & mask
        else:
            combined = ref_masks[0]
            for mask in ref_masks[1:]:
                combined = combined | mask

        if pred.negated:
            combined = ~combined

        return combined

    def _resolve_mcp_key_column(
        self,
        df: pd.DataFrame,
        query: QueryModel,
        pred: ContextPredicate,
        ref_name: str,
    ) -> str:
        """Return the DataFrame column for MCP entity-key matching.

        Resolution strategy:
        1. Registered ``entity_key`` for this provider (set via
           ``register_mcp_provider(..., entity_key=)``), resolved through
           ``_resolve_dataframe_key_column`` (which handles identity maps).
        2. Catalog primary key for the FROM table, resolved through the same
           chain.
        3. First adapter-registered context entity key found in the DataFrame.
        4. Fails with a helpful ``ValueError`` — never falls back to an
           arbitrary column.
        """
        # 1. Registered entity key for this specific provider
        registered_key = self._mcp_entity_keys.get(ref_name)
        if registered_key is not None:
            return self._resolve_dataframe_key_column(df, pred, registered_key)

        # 2. Catalog primary key
        key_name: Optional[str] = None
        if query.from_table:
            tbl = self.catalog.tables.get(query.from_table.name.lower())
            if tbl and tbl.primary_key_name:
                key_name = tbl.primary_key_name

        if key_name is not None:
            try:
                return self._resolve_dataframe_key_column(df, pred, key_name)
            except ValueError:
                pass  # fall through to adapter context scan

        # 3. Adapter-registered context entity keys
        for ctx in self.adapter._contexts.values():
            if ctx.entity_key_name in df.columns:
                return ctx.entity_key_name

        # 4. Fail fast with guidance
        from_name = query.from_table.name if query.from_table else "<unknown>"
        raise ValueError(
            f"Cannot determine entity key column for MCP context on table "
            f"'{from_name}'. Register the table with a primary_key, pass "
            f"entity_key= to register_mcp_provider(), or register an "
            f"identity map with Engine.register_identity_map()."
        )

    def _evaluate_single_context(
        self,
        df: pd.DataFrame,
        query: QueryModel,
        pred: ContextPredicate,
        ref: ContextReference,
    ) -> pd.Series:
        if ref.source_kind == "MCP":
            import warnings
            from concurrent.futures import ThreadPoolExecutor
            from concurrent.futures import TimeoutError as FuturesTimeoutError

            provider = self._mcp_providers.get(ref.name)
            if provider is None:
                raise ValueError(
                    f"MCP provider '{ref.name}' is not registered. "
                    "Call register_mcp_provider() before executing queries "
                    "that reference it."
                )

            entity_type = pred.binding_alias or (
                query.from_table.name if query.from_table else ""
            )
            params = {p.name: p.value for p in ref.parameters}
            cache_key = (ref.name, entity_type, frozenset(params.items()))

            if cache_key not in self._mcp_result_cache:
                _mcp_t0 = __import__('time').perf_counter()
                with ThreadPoolExecutor(max_workers=1) as ex:
                    future = ex.submit(
                        provider.resolve,
                        entity_type=entity_type,
                        params=params,
                        limit=None,
                    )
                    try:
                        mcp_result = future.result(
                            timeout=self._mcp_timeout_ms / 1000
                        )
                    except FuturesTimeoutError:
                        if self._mcp_timeout_behavior == "error":
                            raise RuntimeError(
                                f"MCP provider '{ref.name}' timed out after "
                                f"{self._mcp_timeout_ms}ms"
                            )
                        warnings.warn(
                            f"MCP provider '{ref.name}' timed out after "
                            f"{self._mcp_timeout_ms}ms, returning empty result",
                            stacklevel=4,
                        )
                        return pd.Series([False] * len(df), index=df.index)

                if mcp_result.entity_type != entity_type:
                    raise ValueError(
                        f"MCP provider '{ref.name}' returned entity_type "
                        f"'{mcp_result.entity_type}' but query expected "
                        f"'{entity_type}'."
                    )

                _mcp_elapsed = (__import__('time').perf_counter() - _mcp_t0) * 1000
                self._mcp_result_cache[cache_key] = mcp_result
                # Record in trace
                if hasattr(self, '_trace'):
                    self._trace.provider_calls.append(ProviderCall(
                        provider_name=ref.name,
                        provider_type="MCP",
                        entity_count=(
                            len(mcp_result.entity_ids)
                            if mcp_result.entity_ids is not None
                            else len(mcp_result.membership_array())
                        ),
                        elapsed_ms=_mcp_elapsed,
                        data_as_of=getattr(mcp_result, 'data_as_of', None),
                    ))

            mcp_result = self._mcp_result_cache[cache_key]
            # ID-list results become sets; bitmap results stay as NumPy
            # arrays — never expanded into Python objects (plan 8.2).
            if mcp_result.entity_ids is not None:
                entity_ids = set(mcp_result.entity_ids)
            else:
                entity_ids = mcp_result.membership_array()
            key_col = self._resolve_mcp_key_column(df, query, pred, ref.name)
            # Record context in trace
            _mcp_label = f"MCP({ref.name})"
            if hasattr(self, '_trace') and _mcp_label not in self._trace.contexts_resolved:
                self._trace.contexts_resolved.append(_mcp_label)
            return df[key_col].isin(entity_ids)

        try:
            ctx = self.adapter.get_context(ref.name)
        except KeyError:
            raise ValueError(
                f"Context '{ref.name}' is not registered in the adapter. "
                "Call register_context() before executing queries that reference it."
            )
        key_col = self._resolve_dataframe_key_column(df, pred, ctx.entity_key_name)
        values = df[key_col]

        # Snapshot-backed contexts read the membership store, not the
        # definition SQL (plan 7.4); others resolve live via the adapter.
        snapshot = None
        if self._snapshot_entry(ref.name) is not None:
            snapshot = self.membership.get_snapshot(ref.name)
        if snapshot is not None:
            context_keys = self.membership.members(ref.name)
            label = f"{ref.name}@v{snapshot.version}"
        else:
            context_keys = self.adapter.resolve_context_keys(ref.name)
            label = ref.name
        # Record native context in trace
        if hasattr(self, '_trace') and label not in self._trace.contexts_resolved:
            self._trace.contexts_resolved.append(label)
        return values.isin(context_keys)

    def _resolve_via_identity_map(
        self, df: pd.DataFrame, ctx_entity_key: str
    ) -> Optional[str]:
        """Return a DataFrame column that maps to *ctx_entity_key* via a registered identity map."""
        for mapping in self._identity_maps.values():
            for col_a, col_b in mapping.items():
                _, _, a_col = col_a.rpartition(".")
                _, _, b_col = col_b.rpartition(".")
                if b_col == ctx_entity_key and a_col in df.columns:
                    return a_col
                if a_col == ctx_entity_key and b_col in df.columns:
                    return b_col
        return None

    def _resolve_dataframe_key_column(
        self,
        df: pd.DataFrame,
        pred: ContextPredicate,
        key_name: str,
    ) -> str:
        if pred.binding_alias:
            alias_col = f"{pred.binding_alias}.{key_name}"
            if alias_col in df.columns:
                return alias_col

        if key_name in df.columns:
            return key_name

        suffix_matches = [c for c in df.columns if c.endswith(f".{key_name}")]
        if len(suffix_matches) == 1:
            return suffix_matches[0]

        if len(suffix_matches) > 1:
            raise ValueError(
                f"Ambiguous binding for key '{key_name}'. "
                f"Use CONTEXT ON <alias> explicitly."
            )

        mapped = self._resolve_via_identity_map(df, key_name)
        if mapped:
            return mapped

        raise ValueError(
            f"Could not resolve key column '{key_name}' in result DataFrame. "
            "If entity keys differ across tables, register an identity map with "
            "Engine.register_identity_map()."
        )

    # ---------------------------------------------------------
    # Context scoring
    # ---------------------------------------------------------

    def _apply_context_scoring(self, df: pd.DataFrame, query: QueryModel) -> pd.DataFrame:
        # TODO(v0.3): Only MAX/MIN scoring strategies implemented.
        # Whitepaper Section 9.2 specifies AVG, SUM, COUNT, WEIGHTED_SUM.
        # THEN chain scoping (Section 6.6) not enforced — c2 should
        # evaluate only over c1 members.
        if df.empty:
            df = df.copy()
            df["__context_score"] = []
            df["__context_count"] = []
            return df

        total_score = pd.Series([0.0] * len(df), index=df.index)
        context_count = pd.Series([0] * len(df), index=df.index)

        for pred in query.context_predicates:
            pred_score = pd.Series([0.0] * len(df), index=df.index)
            pred_count = pd.Series([0] * len(df), index=df.index)

            for ref in pred.refs:
                membership = self._evaluate_single_context(df, query, pred, ref)

                if ref.source_kind == "MCP":
                    _et = pred.binding_alias or (
                        query.from_table.name if query.from_table else ""
                    )
                    _params = {p.name: p.value for p in ref.parameters}
                    _ck = (ref.name, _et, frozenset(_params.items()))
                    mcp_result = self._mcp_result_cache.get(_ck)
                    if mcp_result is not None and mcp_result.scores is not None:
                        score_map = mcp_result.score_map()
                        key_col = self._resolve_mcp_key_column(df, query, pred, ref.name)
                        score_values = df[key_col].map(score_map).fillna(0.0)
                    else:
                        score_values = membership.astype(float)
                else:
                    ctx = self.adapter.get_context(ref.name)
                    key_col = self._resolve_dataframe_key_column(df, pred, ctx.entity_key_name)

                    if ctx.has_score:
                        # Snapshot scores are attached after membership
                        # narrowing (plan 7.4); live evaluation otherwise.
                        if (
                            self._snapshot_entry(ref.name) is not None
                            and self.membership.get_snapshot(ref.name)
                            is not None
                        ):
                            score_map = self.membership.scores(ref.name)
                        else:
                            score_map = self.adapter.resolve_context_score_map(ref.name)
                        score_values = df[key_col].map(score_map).fillna(0.0)
                    else:
                        score_values = membership.astype(float)

                weight = ref.weight if ref.weight is not None else 1.0
                score_values = score_values * weight

                if pred.all_mode or pred.sequence_mode:
                    if pred_count.eq(0).all():
                        pred_score = score_values.where(membership, 0.0)
                        pred_count = membership.astype(int)
                    else:
                        pred_score = pred_score.where(~membership, pred_score + score_values)
                        pred_count = pred_count + membership.astype(int)
                else:
                    pred_score = pred_score + score_values
                    pred_count = pred_count + membership.astype(int)

            total_score = total_score + pred_score
            context_count = context_count + pred_count

        out = df.copy()
        out["__context_score"] = total_score
        out["__context_count"] = context_count

        if query.uses_context_score:
            self._replace_placeholder_column(out, "__context_score_placeholder", "__context_score")

        if query.uses_context_count:
            self._replace_placeholder_column(out, "__context_count_placeholder", "__context_count")

        return out

    def _replace_placeholder_column(self, df: pd.DataFrame, placeholder: str, actual: str) -> None:
        if placeholder in df.columns:
            # Copy actual score values into placeholder position, then rename.
            # This preserves column ordering while avoiding duplicate column names.
            if actual in df.columns:
                df[placeholder] = df[actual].values
                df.drop(columns=[actual], inplace=True)
            idx = list(df.columns).index(placeholder)
            cols = list(df.columns)
            cols[idx] = actual
            df.columns = cols

    # ---------------------------------------------------------
    # Projection cleanup
    # ---------------------------------------------------------

    def _apply_projection_aliases(self, df: pd.DataFrame, query: QueryModel) -> pd.DataFrame:
        out = df.copy()

        score_alias = self._extract_context_func_alias(query, "CONTEXT_SCORE()")
        count_alias = self._extract_context_func_alias(query, "CONTEXT_COUNT()")

        # Rename (or drop) __context_score
        if "__context_score" in out.columns:
            if score_alias:
                out.rename(columns={"__context_score": score_alias}, inplace=True)
            else:
                out.drop(columns=["__context_score"], inplace=True, errors="ignore")

        # Rename (or drop) __context_count
        if "__context_count" in out.columns:
            if count_alias:
                out.rename(columns={"__context_count": count_alias}, inplace=True)
            else:
                out.drop(columns=["__context_count"], inplace=True, errors="ignore")

        # Drop any stale placeholders
        out.drop(
            columns=["__context_score_placeholder", "__context_count_placeholder"],
            inplace=True,
            errors="ignore",
        )

        return out

    def _extract_context_func_alias(self, query: QueryModel, func_name: str) -> Optional[str]:
        """Return the AS alias for CONTEXT_SCORE() or CONTEXT_COUNT() in projections, or None."""
        upper_func = func_name.upper()
        for proj in query.projections:
            if upper_func in proj.upper():
                parts = proj.split()
                as_idx = next((i for i, p in enumerate(parts) if p.upper() == "AS"), -1)
                if as_idx >= 0 and as_idx + 1 < len(parts):
                    return parts[as_idx + 1]
        return None

    # ---------------------------------------------------------
    # Ordering
    # ---------------------------------------------------------

    def _apply_order(self, df: pd.DataFrame, query: QueryModel) -> pd.DataFrame:
        if df.empty or not query.order_items:
            return df

        for item in query.order_items:
            if item.is_context_order:
                ascending = (item.direction or "DESC").upper() == "ASC"
                if "__context_score" not in df.columns:
                    return df
                return df.sort_values(
                    by=["__context_score", "__context_count"],
                    ascending=[ascending, False if not ascending else True],
                )

        # Plain column ordering (result row order is not guaranteed by the
        # base SQL — the membership semi-join in particular reorders rows).
        by: List[str] = []
        ascending_flags: List[bool] = []
        for item in query.order_items:
            column = item.column_name
            if column and column in df.columns:
                by.append(column)
                ascending_flags.append(
                    (item.direction or "ASC").upper() == "ASC"
                )
        if by:
            return df.sort_values(by=by, ascending=ascending_flags, kind="mergesort")
        return df

    # ---------------------------------------------------------
    # Context registration helpers
    # ---------------------------------------------------------

    def register_context(
        self,
        name: str,
        sql: str,
        entity_key_name: str,
        has_score: bool = False,
        score_column_name: Optional[str] = None,
        replace: bool = True,
    ) -> None:
        self.adapter.register_context(
            name=name,
            sql=sql,
            entity_key_name=entity_key_name,
            has_score=has_score,
            score_column_name=score_column_name,
            replace=replace,
        )


# =============================================================
# Demo
# =============================================================

if __name__ == "__main__":
    from contextql.semantic import (
        ContextCatalogEntry,
        EntityKeyType,
        TableCatalogEntry,
    )

    invoices = pd.DataFrame(
        {
            "invoice_id": [1, 2, 3, 4, 5, 6],
            "vendor_id": [10, 11, 10, 12, 11, 12],
            "amount": [100, 500, 200, 800, 50, 900],
            "status": ["open", "open", "paid", "open", "open", "open"],
        }
    )

    vendors = pd.DataFrame(
        {
            "vendor_id": [10, 11, 12],
            "vendor_name": ["A", "B", "C"],
            "risk_score": [0.2, 0.9, 0.8],
        }
    )

    catalog = InMemoryCatalog(
        contexts={
            "late_invoice": ContextCatalogEntry(
                name="late_invoice",
                entity_key_name="invoice_id",
                entity_key_type=EntityKeyType.INT64,
                has_score=False,
            ),
            "high_value_invoice": ContextCatalogEntry(
                name="high_value_invoice",
                entity_key_name="invoice_id",
                entity_key_type=EntityKeyType.INT64,
                has_score=True,
            ),
            "risky_vendor": ContextCatalogEntry(
                name="risky_vendor",
                entity_key_name="vendor_id",
                entity_key_type=EntityKeyType.INT64,
                has_score=True,
            ),
        },
        tables={
            "invoices": TableCatalogEntry(
                name="invoices",
                alias="i",
                primary_key_name="invoice_id",
                primary_key_type=EntityKeyType.INT64,
            ),
            "vendors": TableCatalogEntry(
                name="vendors",
                alias="v",
                primary_key_name="vendor_id",
                primary_key_type=EntityKeyType.INT64,
            ),
        },
    )

    adapter = DuckDBAdapter()
    adapter.register_table("invoices", invoices)
    adapter.register_table("vendors", vendors)

    # Boolean context
    adapter.register_context(
        name="late_invoice",
        entity_key_name="invoice_id",
        sql="""
        SELECT invoice_id
        FROM invoices
        WHERE status = 'open' AND invoice_id IN (2, 4, 6)
        """,
        has_score=False,
    )

    # Scored invoice context
    adapter.register_context(
        name="high_value_invoice",
        entity_key_name="invoice_id",
        sql="""
        SELECT invoice_id, amount / 1000.0 AS priority_score
        FROM invoices
        WHERE amount >= 500
        """,
        has_score=True,
        score_column_name="priority_score",
    )

    # Scored vendor context
    adapter.register_context(
        name="risky_vendor",
        entity_key_name="vendor_id",
        sql="""
        SELECT vendor_id, risk_score
        FROM vendors
        WHERE risk_score >= 0.7
        """,
        has_score=True,
        score_column_name="risk_score",
    )

    executor = ContextQLExecutor(
        catalog=catalog,
        adapter=adapter,
    )

    sql = """
    SELECT i.invoice_id, i.amount, v.vendor_name, CONTEXT_SCORE() AS priority
    FROM invoices AS i
    JOIN vendors AS v ON i.vendor_id = v.vendor_id
    WHERE CONTEXT ON i IN (late_invoice, high_value_invoice WEIGHT 1.2)
      AND CONTEXT ON v IN (risky_vendor WEIGHT 0.5)
    ORDER BY CONTEXT DESC
    LIMIT 10;
    """

    result = executor.execute_sql(sql)

    print("=== Generated SQL ===")
    print(result.generated_sql)
    print()
    print("=== Result ===")
    print(result.dataframe)