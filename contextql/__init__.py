"""ContextQL — A context-native query language for operational intelligence."""

from __future__ import annotations

__version__ = "0.2.0"

from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


# ============================================================
# Result
# ============================================================


class Result:
    """Wraps a ContextQL query execution result."""

    def __init__(self, exec_result) -> None:
        self._result = exec_result

    # ---------------------------------------------------------
    # Primary output
    # ---------------------------------------------------------

    def to_pandas(self) -> "pd.DataFrame":
        """Return the result as a pandas DataFrame."""
        return self._result.dataframe

    def to_arrow(self):
        """Return the result as a :class:`pyarrow.Table`.

        Requires ``pip install pyarrow`` (or ``pip install 'contextql[arrow]'``).
        """
        try:
            import pyarrow as pa  # noqa: F401
        except ImportError:
            raise ImportError(
                "pyarrow is required for Result.to_arrow(). "
                "Install it with: pip install pyarrow"
            ) from None
        return pa.Table.from_pandas(self._result.dataframe)

    def to_polars(self):
        """Return the result as a :class:`polars.DataFrame`.

        Requires ``pip install polars`` (or ``pip install 'contextql[polars]'``).
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars is required for Result.to_polars(). "
                "Install it with: pip install polars"
            ) from None
        return pl.from_pandas(self._result.dataframe)

    def show(self, max_rows: int = 40) -> None:
        """Print the result as a formatted table to stdout."""
        df = self._result.dataframe
        rows = len(df)
        print(df.head(max_rows).to_string(index=False))
        if rows > max_rows:
            print(f"... ({rows - max_rows} more rows not shown)")
        print(f"\n({rows} {'row' if rows == 1 else 'rows'})")

    # ---------------------------------------------------------
    # Metadata
    # ---------------------------------------------------------

    @property
    def row_count(self) -> int:
        """Number of rows in the result."""
        return len(self._result.dataframe)

    @property
    def columns(self) -> List[str]:
        """Column names in the result."""
        return list(self._result.dataframe.columns)

    @property
    def sql(self) -> str:
        """The base SQL that was executed against DuckDB."""
        return self._result.generated_sql

    @property
    def diagnostics(self) -> list:
        """Any analysis diagnostics raised during query planning."""
        return self._result.analysis.diagnostics

    def __repr__(self) -> str:
        rows = len(self._result.dataframe)
        return f"<Result rows={rows}>"

    def __str__(self) -> str:
        return self._result.dataframe.to_string(index=False)


# ============================================================
# CatalogProxy
# ============================================================


class CatalogProxy:
    """Read-only view of the engine's registered catalog."""

    def __init__(self, catalog, adapter) -> None:
        self._catalog = catalog
        self._adapter = adapter

    def contexts(self) -> List[str]:
        """Return names of all registered contexts."""
        return [e.name for e in self._catalog.list_contexts()]

    def tables(self) -> List[str]:
        """Return names of all registered tables."""
        return self._adapter.list_tables()


# ============================================================
# Engine
# ============================================================


class Engine:
    """
    Public ContextQL engine.

    Usage::

        import contextql as cql

        engine = cql.Engine()
        engine.register_table("invoices", df)
        engine.register_context(
            "open_invoice",
            "SELECT invoice_id FROM invoices WHERE status = 'open'",
            entity_key="invoice_id",
        )
        result = engine.execute("SELECT * FROM invoices WHERE CONTEXT IN (open_invoice)")
        print(result.to_pandas())

    For a pre-loaded demo engine::

        engine = cql.demo()
        result = engine.execute("SELECT invoice_id, CONTEXT_SCORE() AS s "
                                "FROM invoices WHERE CONTEXT IN (risky_vendor) "
                                "ORDER BY CONTEXT DESC LIMIT 10;")
        print(result.to_pandas())
    """

    def __init__(self, database: str = ":memory:") -> None:
        from contextql.adapters.duckdb_adapter import DuckDBAdapter
        from contextql.semantic import InMemoryCatalog
        from contextql.executor import ContextQLExecutor

        # Support duckdb://path/to/file.duckdb URL scheme
        if database.startswith("duckdb://"):
            database = database[len("duckdb://"):]

        self._adapter = DuckDBAdapter(database=database)
        self._catalog = InMemoryCatalog()
        self._executor = ContextQLExecutor(catalog=self._catalog, adapter=self._adapter)

    # ---------------------------------------------------------
    # Registration
    # ---------------------------------------------------------

    def register_table(
        self,
        name: str,
        df: "pd.DataFrame",
        *,
        primary_key: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> None:
        """Register a pandas DataFrame as a queryable table.

        Args:
            name: Table name used in SQL queries.
            df: The pandas DataFrame to register.
            primary_key: Optional primary key column name (used by the semantic analyzer).
            alias: Optional short alias for the table (e.g. ``"i"`` for ``"invoices"``).
        """
        self._adapter.register_table(name, df)
        if primary_key is not None:
            from contextql.semantic import TableCatalogEntry
            entry = TableCatalogEntry(name=name, alias=alias, primary_key_name=primary_key)
            self._catalog.tables[name.lower()] = entry

    def register_context(
        self,
        name: str,
        sql: str,
        *,
        entity_key: str,
        has_score: bool = False,
        score_column: Optional[str] = None,
        replace: bool = True,
    ) -> None:
        """Register a context definition.

        Args:
            name: Context name referenced in ``CONTEXT IN (name)`` predicates.
            sql: SQL query that produces the context membership set.
                 If ``has_score=True``, the query must also return a score column.
            entity_key: Name of the entity key column in the query result.
            has_score: Whether the context produces a numeric relevance score.
            score_column: Name of the score column (required if ``has_score=True``).
            replace: If ``False``, raises ``ValueError`` if the context already exists.
        """
        self._adapter.register_context(
            name=name,
            sql=sql,
            entity_key_name=entity_key,
            has_score=has_score,
            score_column_name=score_column,
            replace=replace,
        )
        from contextql.semantic import ContextCatalogEntry
        entry = ContextCatalogEntry(name=name, entity_key_name=entity_key, has_score=has_score)
        self._catalog.contexts[name.lower()] = entry

    # ---------------------------------------------------------
    # Query
    # ---------------------------------------------------------

    def execute(self, sql: str) -> Result:
        """Execute a ContextQL SELECT query and return a :class:`Result`.

        Raises:
            ValueError: If the query has semantic errors or references unregistered contexts.
        """
        exec_result = self._executor.execute_sql(sql)
        return Result(exec_result)

    def explain(self, sql: str) -> str:
        """Return a human-readable query plan for a ContextQL statement."""
        from contextql.semantic import analyze_sql, QueryModel
        analysis = analyze_sql(sql, self._catalog)
        lines: List[str] = []
        lines.append("=== ContextQL Query Plan ===")

        if not analysis.ok:
            lines.append("ERRORS:")
            for d in analysis.diagnostics:
                lines.append(f"  {d}")
            return "\n".join(lines)

        stmt = analysis.statements[0] if analysis.statements else None
        if stmt is None:
            lines.append("(no statements)")
            return "\n".join(lines)

        if isinstance(stmt, QueryModel):
            lines.append(f"Kind          : SELECT")
            from_name = stmt.from_table.name if stmt.from_table else "-"
            from_alias = f" AS {stmt.from_table.alias}" if stmt.from_table and stmt.from_table.alias else ""
            lines.append(f"From          : {from_name}{from_alias}")
            for j in stmt.joins:
                on_clause = f" ON {j.condition}" if j.condition else ""
                t = j.table
                t_sql = t.name + (f" AS {t.alias}" if t.alias else "")
                lines.append(f"Join          : {j.join_type} {t_sql}{on_clause}")
            if stmt.where_text:
                lines.append(f"Where         : {stmt.where_text}")
            if stmt.context_predicates:
                lines.append("Context preds :")
                for pred in stmt.context_predicates:
                    neg = "NOT " if pred.negated else ""
                    alias_part = f"ON {pred.binding_alias} " if pred.binding_alias else ""
                    refs_str = ", ".join(
                        r.name + (f" WEIGHT {r.weight}" if r.weight is not None else "")
                        for r in pred.refs
                    )
                    lines.append(f"  {alias_part}CONTEXT {neg}IN ({refs_str})")
            lines.append(f"Projections   : {', '.join(stmt.projections)}")
            for item in stmt.order_items:
                if item.is_context_order:
                    lines.append(f"Order         : BY CONTEXT {item.direction or 'DESC'}")
            if stmt.limit is not None:
                lines.append(f"Limit         : {stmt.limit}")
            if stmt.offset is not None:
                lines.append(f"Offset        : {stmt.offset}")
        else:
            lines.append(f"Kind          : {stmt.kind}")

        return "\n".join(lines)

    # ---------------------------------------------------------
    # Fluent query builder
    # ---------------------------------------------------------

    def query(self, table: str) -> "QueryBuilder":
        """Return a :class:`QueryBuilder` for *table*.

        Example::

            result = (engine.query("invoices")
                .select("invoice_id", "amount", "CONTEXT_SCORE() AS score")
                .where_context("open_invoice")
                .order_by_context()
                .limit(10)
                .execute())
        """
        from contextql._builder import QueryBuilder
        return QueryBuilder(self, table)

    # ---------------------------------------------------------
    # @context decorator
    # ---------------------------------------------------------

    def context(
        self,
        name: str,
        *,
        entity_key: str,
        has_score: bool = False,
        score_column: Optional[str] = None,
        replace: bool = True,
    ):
        """Decorator that registers the decorated function as a ContextQL context.

        The decorated function must accept no arguments and return a SQL string.

        Example::

            @engine.context("late_invoice", entity_key="invoice_id")
            def late_invoice():
                return "SELECT invoice_id FROM invoices WHERE status = 'open'"

            @engine.context("high_value", entity_key="invoice_id",
                             has_score=True, score_column="urgency")
            def high_value():
                return "SELECT invoice_id, amount / 24250.0 AS urgency FROM invoices"
        """
        def _decorator(fn):
            sql = fn()
            self.register_context(
                name=name,
                sql=sql,
                entity_key=entity_key,
                has_score=has_score,
                score_column=score_column,
                replace=replace,
            )
            return fn
        return _decorator

    # ---------------------------------------------------------
    # Catalog introspection
    # ---------------------------------------------------------

    @property
    def catalog(self) -> CatalogProxy:
        """Read-only view of registered tables and contexts."""
        return CatalogProxy(self._catalog, self._adapter)


# ============================================================
# Jupyter magic extension hook
# ============================================================


def load_ipython_extension(ip) -> None:
    """Register ContextQL Jupyter magics.

    Called automatically by ``%load_ext contextql`` in a Jupyter notebook.
    """
    from contextql._magic import load_ipython_extension as _load
    _load(ip)


# ============================================================
# QueryBuilder type alias (re-export for type checkers)
# ============================================================


def _get_builder_class():
    from contextql._builder import QueryBuilder
    return QueryBuilder


# ============================================================
# Demo engine
# ============================================================


def demo() -> Engine:
    """Return a pre-loaded Engine with sample operational data.

    Tables registered:
        - ``vendors``      — 60 vendors with risk tiers and categories
        - ``invoices``     — 240 invoices linked to vendors
        - ``payments``     — payment records for paid invoices
        - ``orders``       — 150 procurement orders
        - ``order_events`` — lifecycle events per order
        - ``tickets``      — 120 support tickets

    Contexts registered:
        - ``open_invoice``      — invoices with status = 'open'
        - ``disputed_invoice``  — invoices with status = 'disputed'
        - ``overdue_invoice``   — open invoices scored by amount (urgency)
        - ``risky_vendor``      — high/critical risk vendors (scored)
        - ``watchlist_vendor``  — vendors flagged on the watchlist
        - ``stalled_order``     — orders with current_status = 'stalled'
        - ``expedite_order``    — orders with priority = 'expedite'
        - ``sev1_ticket``       — severity-1 support tickets
        - ``open_ticket``       — open/waiting tickets scored by backlog hours
    """
    import pandas as pd

    # ── Vendors (60 rows) ─────────────────────────────────────────────────────
    _COUNTRIES = ["US", "DE", "GB", "ZA", "AE", "SG"]
    _CATEGORIES = ["logistics", "software", "manufacturing", "consulting", "raw-materials"]

    def _risk_tier(g: int) -> str:
        if g % 10 == 0:
            return "critical"
        if g % 4 == 0:
            return "high"
        if g % 3 == 0:
            return "medium"
        return "low"

    vendors = pd.DataFrame({
        "vendor_id":            list(range(1, 61)),
        "vendor_name":          [f"Vendor {g:03d}" for g in range(1, 61)],
        "country":              [_COUNTRIES[g % 6] for g in range(1, 61)],
        "risk_tier":            [_risk_tier(g) for g in range(1, 61)],
        "on_watchlist":         [g % 13 == 0 for g in range(1, 61)],
        "payment_terms_days":   [15 + (g % 4) * 15 for g in range(1, 61)],
        "category":             [_CATEGORIES[g % 5] for g in range(1, 61)],
    })

    # ── Invoices (240 rows) ───────────────────────────────────────────────────
    from datetime import date, timedelta

    _today = date.today()

    def _inv_date(g: int) -> date:
        return _today - timedelta(days=(g * 3) % 180)

    def _due_date(g: int) -> date:
        return _inv_date(g) + timedelta(days=15 + (g % 45))

    def _currency(g: int) -> str:
        if g % 19 == 0:
            return "EUR"
        if g % 17 == 0:
            return "GBP"
        return "USD"

    def _inv_status(g: int) -> str:
        if g % 11 == 0:
            return "disputed"
        if g % 5 == 0:
            return "paid"
        if _today > _due_date(g):
            return "open"
        return "approved"

    def _payment_date(g: int) -> Optional[str]:
        if g % 5 == 0:
            return (_due_date(g) + timedelta(days=(g % 8) - 2)).isoformat()
        return None

    invoices = pd.DataFrame({
        "invoice_id":   list(range(1, 241)),
        "vendor_id":    [((g - 1) % 60) + 1 for g in range(1, 241)],
        "invoice_date": [_inv_date(g).isoformat() for g in range(1, 241)],
        "due_date":     [_due_date(g).isoformat() for g in range(1, 241)],
        "amount":       [round(250 + ((g * 137) % 24000), 2) for g in range(1, 241)],
        "currency":     [_currency(g) for g in range(1, 241)],
        "status":       [_inv_status(g) for g in range(1, 241)],
        "payment_date": [_payment_date(g) for g in range(1, 241)],
        "dispute_flag": [g % 11 == 0 for g in range(1, 241)],
        "po_number":    [f"PO-{((g * 7) % 90) + 1:05d}" for g in range(1, 241)],
        "cost_center":  [["OPS", "FIN", "RND", "GTM"][g % 4] for g in range(1, 241)],
    })

    # ── Payments ──────────────────────────────────────────────────────────────
    _METHODS = ["wire", "ach", "card", "manual"]
    _paid = [g for g in range(1, 241) if g % 5 == 0]

    def _is_late(g: int) -> bool:
        pd_str = _payment_date(g)
        return pd_str is not None and pd_str > _due_date(g).isoformat()

    payments = pd.DataFrame({
        "payment_id":        list(range(1, len(_paid) + 1)),
        "invoice_id":        _paid,
        "paid_amount":       [round(250 + ((g * 137) % 24000), 2) for g in _paid],
        "paid_date":         [_payment_date(g) for g in _paid],
        "payment_method":    [_METHODS[g % 4] for g in _paid],
        "settlement_status": ["late" if _is_late(g) else "on_time" for g in _paid],
    })

    # ── Orders (150 rows) ─────────────────────────────────────────────────────
    _REGIONS = ["emea", "us-east", "apac", "latam"]

    def _order_priority(g: int) -> str:
        if g % 9 == 0:
            return "expedite"
        if g % 5 == 0:
            return "review"
        return "standard"

    def _order_status(g: int) -> str:
        if g % 10 == 0:
            return "stalled"
        if g % 6 == 0:
            return "shipped"
        return "in_flight"

    orders = pd.DataFrame({
        "order_id":       list(range(1, 151)),
        "vendor_id":      [((g - 1) % 60) + 1 for g in range(1, 151)],
        "region":         [_REGIONS[g % 4] for g in range(1, 151)],
        "priority":       [_order_priority(g) for g in range(1, 151)],
        "current_status": [_order_status(g) for g in range(1, 151)],
        "days_open":      [g % 35 for g in range(1, 151)],
    })

    # ── Order events ──────────────────────────────────────────────────────────
    _evt_rows: list = []
    for g in range(1, 151):
        _evt_rows.append((g, "Create",   "portal",                                          "done"))
        if g % 8 != 0:
            _evt_rows.append((g, "Approve",  "manual-review" if g % 7 == 0 else "auto-approval", "done"))
        if g % 10 != 0:
            _evt_rows.append((g, "Pick",     "warehouse-b" if g % 5 == 0 else "warehouse-a",     "done"))
        if g % 6 == 0:
            _evt_rows.append((g, "Ship",     "carrier-x" if g % 4 == 0 else "carrier-y",         "done"))
        if g % 9 == 0:
            _evt_rows.append((g, "Escalate", "ops-control",                                      "alert"))

    order_events = pd.DataFrame(_evt_rows, columns=["order_id", "activity", "resource", "state"])
    order_events.insert(0, "event_id", range(1, len(order_events) + 1))

    # ── Tickets (120 rows) ────────────────────────────────────────────────────
    _TEAMS = ["finops", "vendor-risk", "fulfillment", "shared-services"]

    def _severity(g: int) -> str:
        if g % 12 == 0:
            return "sev1"
        if g % 5 == 0:
            return "sev2"
        return "sev3"

    def _ticket_status(g: int) -> str:
        if g % 7 == 0:
            return "waiting_on_vendor"
        if g % 3 == 0:
            return "open"
        return "resolved"

    tickets = pd.DataFrame({
        "ticket_id":    list(range(1, 121)),
        "vendor_id":    [((g - 1) % 60) + 1 for g in range(1, 121)],
        "order_id":     [((g - 1) % 150) + 1 for g in range(1, 121)],
        "severity":     [_severity(g) for g in range(1, 121)],
        "status":       [_ticket_status(g) for g in range(1, 121)],
        "backlog_hours":[4 + ((g * 9) % 240) for g in range(1, 121)],
        "assigned_team":[_TEAMS[g % 4] for g in range(1, 121)],
    })

    # ── Build engine ──────────────────────────────────────────────────────────
    engine = Engine()
    engine.register_table("vendors",      vendors,      primary_key="vendor_id")
    engine.register_table("invoices",     invoices,     primary_key="invoice_id")
    engine.register_table("payments",     payments,     primary_key="payment_id")
    engine.register_table("orders",       orders,       primary_key="order_id")
    engine.register_table("order_events", order_events, primary_key="event_id")
    engine.register_table("tickets",      tickets,      primary_key="ticket_id")

    # ── Contexts ──────────────────────────────────────────────────────────────
    engine.register_context(
        "open_invoice",
        "SELECT invoice_id FROM invoices WHERE status = 'open'",
        entity_key="invoice_id",
    )
    engine.register_context(
        "disputed_invoice",
        "SELECT invoice_id FROM invoices WHERE status = 'disputed'",
        entity_key="invoice_id",
    )
    engine.register_context(
        "overdue_invoice",
        "SELECT invoice_id, amount / 24250.0 AS urgency_score "
        "FROM invoices WHERE status = 'open'",
        entity_key="invoice_id",
        has_score=True,
        score_column="urgency_score",
    )
    engine.register_context(
        "risky_vendor",
        "SELECT vendor_id, "
        "CASE risk_tier "
        "  WHEN 'critical' THEN 1.0 "
        "  WHEN 'high'     THEN 0.75 "
        "  WHEN 'medium'   THEN 0.4 "
        "  ELSE 0.1 END AS risk_score "
        "FROM vendors WHERE risk_tier IN ('critical', 'high')",
        entity_key="vendor_id",
        has_score=True,
        score_column="risk_score",
    )
    engine.register_context(
        "watchlist_vendor",
        "SELECT vendor_id FROM vendors WHERE on_watchlist = true",
        entity_key="vendor_id",
    )
    engine.register_context(
        "stalled_order",
        "SELECT order_id FROM orders WHERE current_status = 'stalled'",
        entity_key="order_id",
    )
    engine.register_context(
        "expedite_order",
        "SELECT order_id FROM orders WHERE priority = 'expedite'",
        entity_key="order_id",
    )
    engine.register_context(
        "sev1_ticket",
        "SELECT ticket_id FROM tickets WHERE severity = 'sev1'",
        entity_key="ticket_id",
    )
    engine.register_context(
        "open_ticket",
        "SELECT ticket_id, backlog_hours / 244.0 AS urgency "
        "FROM tickets WHERE status IN ('open', 'waiting_on_vendor')",
        entity_key="ticket_id",
        has_score=True,
        score_column="urgency",
    )

    return engine
