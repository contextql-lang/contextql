"""ContextQL fluent query builder.

Provides :class:`QueryBuilder` — a chainable API for building ContextQL SELECT
queries in Python without constructing SQL strings manually.

Typical usage::

    result = (engine.query("invoices")
        .select("invoice_id", "amount", "CONTEXT_SCORE() AS score")
        .join("vendors AS v", on="invoices.vendor_id = v.vendor_id")
        .where("amount > 500")
        .where_context("open_invoice", "risky_vendor WEIGHT 1.5")
        .order_by_context(desc=True)
        .limit(10)
        .execute())
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from contextql import Engine, Result


class QueryBuilder:
    """Fluent builder for ContextQL SELECT statements.

    Instances are created via :meth:`Engine.query`::

        qb = engine.query("invoices")
    """

    def __init__(self, engine: "Engine", table: str) -> None:
        self._engine = engine
        self._table = table
        self._projections: List[str] = []
        self._joins: List[Tuple[str, str, str]] = []   # (how, table_expr, on)
        self._where_conditions: List[str] = []
        self._context_preds: List[Tuple[str, Optional[str], bool, bool, str]] = []
        # Each tuple: (contexts_str, table_alias, negated, all_mode, "raw")
        self._order_by: List[str] = []
        self._context_order: Optional[str] = None  # "ASC" or "DESC"
        self._limit_val: Optional[int] = None
        self._offset_val: Optional[int] = None

    # ---------------------------------------------------------
    # Projection
    # ---------------------------------------------------------

    def select(self, *cols: str) -> "QueryBuilder":
        """Specify the SELECT columns.

        Accepts any ContextQL/SQL expressions including ``CONTEXT_SCORE() AS alias``.
        Calling multiple times replaces the previous selection.

        Example::

            .select("invoice_id", "amount", "CONTEXT_SCORE() AS score")
        """
        self._projections = list(cols)
        return self

    # ---------------------------------------------------------
    # FROM / JOIN
    # ---------------------------------------------------------

    def join(
        self,
        table: str,
        *,
        on: str,
        how: str = "JOIN",
    ) -> "QueryBuilder":
        """Add a JOIN clause.

        Args:
            table: Table name or alias expression, e.g. ``"vendors AS v"``.
            on: The ON condition, e.g. ``"invoices.vendor_id = v.vendor_id"``.
            how: Join type string (``"JOIN"``, ``"LEFT JOIN"``, etc.).

        Example::

            .join("vendors AS v", on="invoices.vendor_id = v.vendor_id")
            .join("payments AS p", on="invoices.invoice_id = p.invoice_id", how="LEFT JOIN")
        """
        self._joins.append((how.upper(), table, on))
        return self

    # ---------------------------------------------------------
    # WHERE (plain SQL)
    # ---------------------------------------------------------

    def where(self, condition: str) -> "QueryBuilder":
        """Add a non-context WHERE condition (plain SQL).

        Multiple calls are combined with AND.

        Example::

            .where("amount > 500")
            .where("status != 'disputed'")
        """
        self._where_conditions.append(condition)
        return self

    # ---------------------------------------------------------
    # Context predicates
    # ---------------------------------------------------------

    def where_context(
        self,
        *contexts: str,
        table_alias: Optional[str] = None,
        negated: bool = False,
        all_mode: bool = False,
    ) -> "QueryBuilder":
        """Add a CONTEXT predicate.

        Each positional argument is a context reference. You may include
        optional ``WEIGHT`` modifiers in the string, e.g. ``"risky_vendor WEIGHT 1.5"``.

        Multiple calls produce separate ``CONTEXT IN (...)`` predicates joined with AND.

        Args:
            *contexts: Context names (with optional WEIGHT suffix).
            table_alias: If set, generates ``CONTEXT ON alias IN (...)``.
            negated: If ``True``, generates ``CONTEXT NOT IN (...)``.
            all_mode: If ``True``, generates ``CONTEXT IN ALL (...)``.

        Example::

            .where_context("open_invoice")
            .where_context("risky_vendor WEIGHT 1.5", table_alias="v")
        """
        refs = ", ".join(contexts)
        self._context_preds.append((refs, table_alias, negated, all_mode, "normal"))
        return self

    # ---------------------------------------------------------
    # ORDER BY
    # ---------------------------------------------------------

    def order_by_context(self, *, desc: bool = True) -> "QueryBuilder":
        """Add ``ORDER BY CONTEXT DESC`` (or ASC) to the query.

        Example::

            .order_by_context()          # descending (highest score first)
            .order_by_context(desc=False)  # ascending
        """
        self._context_order = "DESC" if desc else "ASC"
        return self

    def order_by(self, *cols: str) -> "QueryBuilder":
        """Add SQL ORDER BY expressions.

        Example::

            .order_by("amount DESC", "invoice_id ASC")
        """
        self._order_by.extend(cols)
        return self

    # ---------------------------------------------------------
    # LIMIT / OFFSET
    # ---------------------------------------------------------

    def limit(self, n: int) -> "QueryBuilder":
        """Apply a LIMIT clause."""
        self._limit_val = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        """Apply an OFFSET clause."""
        self._offset_val = n
        return self

    # ---------------------------------------------------------
    # Terminal
    # ---------------------------------------------------------

    def build(self) -> str:
        """Return the generated ContextQL SQL string."""
        parts: List[str] = []

        # SELECT
        proj = ", ".join(self._projections) if self._projections else "*"
        parts.append(f"SELECT {proj}")

        # FROM
        parts.append(f"FROM {self._table}")

        # JOINs
        for how, tbl, on in self._joins:
            parts.append(f"{how} {tbl} ON {on}")

        # WHERE
        all_conditions: List[str] = list(self._where_conditions)
        for refs, alias, negated, all_mode, _ in self._context_preds:
            on_part = f"ON {alias} " if alias else ""
            not_part = "NOT " if negated else ""
            all_part = "ALL " if all_mode else ""
            all_conditions.append(f"CONTEXT {on_part}{not_part}IN {all_part}({refs})")

        if all_conditions:
            parts.append("WHERE " + " AND ".join(all_conditions))

        # ORDER BY
        order_parts: List[str] = []
        if self._context_order:
            order_parts.append(f"CONTEXT {self._context_order}")
        order_parts.extend(self._order_by)
        if order_parts:
            parts.append("ORDER BY " + ", ".join(order_parts))

        # LIMIT / OFFSET
        if self._limit_val is not None:
            parts.append(f"LIMIT {self._limit_val}")
        if self._offset_val is not None:
            parts.append(f"OFFSET {self._offset_val}")

        return "\n".join(parts) + ";"

    def execute(self) -> "Result":
        """Build the SQL and execute it via the engine."""
        return self._engine.execute(self.build())

    def explain(self) -> str:
        """Build the SQL and return the query plan via the engine."""
        return self._engine.explain(self.build())

    def __repr__(self) -> str:
        return f"<QueryBuilder table={self._table!r} sql={self.build()!r}>"
