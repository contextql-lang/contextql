# contextql/adapters/duckdb_adapter.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Any

import duckdb
import pandas as pd


@dataclass
class DuckDBRegisteredContext:
    name: str
    sql: str
    entity_key_name: str
    has_score: bool = False
    score_column_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DuckDBAdapter:
    """
    First real execution target for ContextQL.

    Responsibilities:
    - own the DuckDB connection
    - register pandas tables as DuckDB views
    - register context definitions
    - materialize/resolve contexts
    - execute generated SQL
    """

    def __init__(self, database: str = ":memory:"):
        self.conn = duckdb.connect(database=database)
        self._tables: Dict[str, pd.DataFrame] = {}
        self._contexts: Dict[str, DuckDBRegisteredContext] = {}

    # ---------------------------------------------------------
    # Table registration
    # ---------------------------------------------------------

    def register_table(self, name: str, df: pd.DataFrame) -> None:
        if name in self._tables:
            try:
                self.conn.unregister(name)
            except Exception:
                pass
        self._tables[name] = df
        self.conn.register(name, df)

    def unregister_table(self, name: str) -> None:
        if name in self._tables:
            del self._tables[name]
        try:
            self.conn.unregister(name)
        except Exception:
            pass

    def list_tables(self) -> list[str]:
        return sorted(self._tables.keys())

    # ---------------------------------------------------------
    # Context registration
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
        if not replace and name in self._contexts:
            raise ValueError(f"Context already exists: {name}")

        self._contexts[name] = DuckDBRegisteredContext(
            name=name,
            sql=sql.strip().rstrip(";"),
            entity_key_name=entity_key_name,
            has_score=has_score,
            score_column_name=score_column_name,
        )

    def unregister_context(self, name: str) -> None:
        self._contexts.pop(name, None)

    def get_context(self, name: str) -> DuckDBRegisteredContext:
        if name not in self._contexts:
            raise KeyError(f"Unknown context: {name}")
        return self._contexts[name]

    def list_contexts(self) -> list[str]:
        return sorted(self._contexts.keys())

    # ---------------------------------------------------------
    # Context resolution
    # ---------------------------------------------------------

    def resolve_context_df(self, name: str) -> pd.DataFrame:
        ctx = self.get_context(name)
        df = self.conn.execute(ctx.sql).df()

        if ctx.entity_key_name not in df.columns:
            raise ValueError(
                f"Context '{name}' query result does not include entity key column "
                f"'{ctx.entity_key_name}'. Returned columns: {list(df.columns)}"
            )

        if ctx.has_score:
            if not ctx.score_column_name:
                raise ValueError(
                    f"Context '{name}' is marked scored but no score_column_name was provided."
                )
            if ctx.score_column_name not in df.columns:
                raise ValueError(
                    f"Context '{name}' query result does not include score column "
                    f"'{ctx.score_column_name}'. Returned columns: {list(df.columns)}"
                )

        return df

    def resolve_context_keys(self, name: str) -> set:
        ctx = self.get_context(name)
        df = self.resolve_context_df(name)
        return set(df[ctx.entity_key_name].tolist())

    def resolve_context_score_map(self, name: str) -> dict:
        ctx = self.get_context(name)
        if not ctx.has_score:
            return {}

        df = self.resolve_context_df(name)
        score_col = ctx.score_column_name
        key_col = ctx.entity_key_name

        return dict(zip(df[key_col].tolist(), df[score_col].tolist()))

    # ---------------------------------------------------------
    # Query execution
    # ---------------------------------------------------------

    def execute_df(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).df()

    def execute_batches(self, sql: str, batch_size: int = 65_536):
        """Yield bounded DataFrames from one DuckDB execution."""
        cursor = self.conn.execute(sql)
        columns = [description[0] for description in cursor.description]
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield pd.DataFrame.from_records(rows, columns=columns)

    def register_member_batches(self, name: str, batches) -> None:
        """Create a temporary member relation from bounded ID batches."""
        self.conn.execute(
            f'CREATE TEMP TABLE "{name}" (entity_id BIGINT)'
        )
        try:
            for batch in batches:
                self.conn.executemany(
                    f'INSERT INTO "{name}" VALUES (?)',
                    [(int(value),) for value in batch],
                )
        except Exception:
            self.conn.execute(f'DROP TABLE IF EXISTS "{name}"')
            raise

    def unregister_relation(self, name: str) -> None:
        try:
            self.conn.unregister(name)
        except Exception:
            pass
        try:
            self.conn.execute(f'DROP TABLE IF EXISTS "{name}"')
        except Exception:
            self.conn.execute(f'DROP VIEW IF EXISTS "{name}"')

    def execute(self, sql: str):
        return self.conn.execute(sql)

    def close(self) -> None:
        self.conn.close()
