"""Public import path for :class:`QueryBuilder`.

``contextql._builder`` is the implementation module; this module provides
a stable public path::

    from contextql.builder import QueryBuilder
"""

from contextql._builder import QueryBuilder

__all__ = ["QueryBuilder"]
