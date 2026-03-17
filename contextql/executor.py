# contextql/executor.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from contextql.adapters.duckdb_adapter import DuckDBAdapter
from contextql.semantic import (
    AnalysisResult,
    ContextPredicate,
    ContextReference,
    InMemoryCatalog,
    QueryModel,
    TableRef,
    analyze_sql,
)


@dataclass
class ExecutionResult:
    dataframe: pd.DataFrame
    generated_sql: str
    analysis: AnalysisResult


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
    ):
        self.catalog = catalog
        self.adapter = adapter

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
        if not isinstance(stmt, QueryModel):
            raise ValueError("Executor currently supports SELECT queries only.")

        df, generated_sql = self._execute_query(stmt)

        return ExecutionResult(
            dataframe=df,
            generated_sql=generated_sql,
            analysis=analysis,
        )

    # ---------------------------------------------------------
    # Main query execution
    # ---------------------------------------------------------

    def _execute_query(self, query: QueryModel) -> Tuple[pd.DataFrame, str]:
        extra_key_cols = self._collect_extra_key_cols(query)
        base_sql = self._build_base_sql(query, extra_key_cols)
        df = self.adapter.execute_df(base_sql)

        if query.context_predicates:
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

    # ---------------------------------------------------------
    # SQL lowering
    # ---------------------------------------------------------

    def _collect_extra_key_cols(self, query: QueryModel) -> Dict[str, str]:
        """Return {key_col_name: select_expr} for context key columns not already projected."""
        proj_text = " ".join(query.projections).lower()
        extra: Dict[str, str] = {}
        for pred in query.context_predicates:
            for ref in pred.refs:
                if ref.source_kind == "MCP":
                    continue
                try:
                    ctx = self.adapter.get_context(ref.name)
                except KeyError:
                    continue
                key_name = ctx.entity_key_name
                if key_name.lower() in proj_text or key_name in extra:
                    continue
                if pred.binding_alias:
                    extra[key_name] = f"{pred.binding_alias}.{key_name}"
                else:
                    extra[key_name] = key_name
        return extra

    def _build_base_sql(self, query: QueryModel, extra_key_cols: Optional[Dict[str, str]] = None) -> str:
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

        if non_context_where:
            parts.append(f"WHERE {non_context_where}")

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
        if table.source_kind == "REMOTE":
            raise NotImplementedError("REMOTE() is not yet supported in the DuckDB executor.")

        if table.alias:
            return f"{table.name} AS {table.alias}"
        return table.name

    def _joins_sql(self, query: QueryModel) -> str:
        if not query.joins:
            return ""

        chunks: List[str] = []
        for join in query.joins:
            if join.table.source_kind == "REMOTE":
                raise NotImplementedError("REMOTE() in JOINs is not yet supported.")

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

        parts = self._split_top_level_and(where_text)
        kept = [p for p in parts if "CONTEXT" not in p.upper()]
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

    def _evaluate_single_context(
        self,
        df: pd.DataFrame,
        query: QueryModel,
        pred: ContextPredicate,
        ref: ContextReference,
    ) -> pd.Series:
        if ref.source_kind == "MCP":
            # Stub behavior for now: no members
            return pd.Series([False] * len(df), index=df.index)

        try:
            ctx = self.adapter.get_context(ref.name)
        except KeyError:
            raise ValueError(
                f"Context '{ref.name}' is not registered in the adapter. "
                "Call register_context() before executing queries that reference it."
            )
        key_col = self._resolve_dataframe_key_column(df, pred, ctx.entity_key_name)
        values = df[key_col]

        context_keys = self.adapter.resolve_context_keys(ref.name)
        return values.isin(context_keys)

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

        raise ValueError(f"Could not resolve key column '{key_name}' in result DataFrame.")

    # ---------------------------------------------------------
    # Context scoring
    # ---------------------------------------------------------

    def _apply_context_scoring(self, df: pd.DataFrame, query: QueryModel) -> pd.DataFrame:
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
                    score_values = membership.astype(float)
                else:
                    ctx = self.adapter.get_context(ref.name)
                    key_col = self._resolve_dataframe_key_column(df, pred, ctx.entity_key_name)

                    if ctx.has_score:
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