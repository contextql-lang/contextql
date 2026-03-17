# contextql/adapters/__init__.py

from .duckdb_adapter import DuckDBAdapter, DuckDBRegisteredContext

__all__ = ["DuckDBAdapter", "DuckDBRegisteredContext"]