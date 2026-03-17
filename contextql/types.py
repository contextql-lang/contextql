"""ContextQL type system.

Implements the type lattice from WHITEPAPER Section 5:

                    ANY
                     |
       +-------------+-------------+
       |             |             |
    SCALAR      CONTEXT_TYPE    SET_TYPE
       |             |             |
  (delegated    +----+----+    ENTITY_SET
   to engine)   |         |
            CONTEXT  CONTEXT_REF
                |
          +-----+------+
          |            |
     SCORED_CTX   BOOLEAN_CTX

Static typing at the context layer; value-level type checking is
delegated to the execution engine (DuckDB, Polars, Arrow).
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class TypeKind(Enum):
    """Top-level type categories in the ContextQL type lattice."""
    ANY = auto()
    SCALAR = auto()
    CONTEXT_TYPE = auto()
    SET_TYPE = auto()
    # Leaf context types
    SCORED_CTX = auto()
    BOOLEAN_CTX = auto()
    # Leaf set type
    ENTITY_SET = auto()


@dataclass(frozen=True, slots=True)
class ContextQLType:
    """A type in the ContextQL type lattice."""
    kind: TypeKind
    name: str

    def is_context(self) -> bool:
        return self.kind in (
            TypeKind.CONTEXT_TYPE,
            TypeKind.SCORED_CTX,
            TypeKind.BOOLEAN_CTX,
        )

    def is_scored(self) -> bool:
        return self.kind == TypeKind.SCORED_CTX

    def is_boolean(self) -> bool:
        return self.kind == TypeKind.BOOLEAN_CTX

    def is_set(self) -> bool:
        return self.kind in (TypeKind.SET_TYPE, TypeKind.ENTITY_SET)


# Singleton type instances
ANY = ContextQLType(TypeKind.ANY, "ANY")
SCALAR = ContextQLType(TypeKind.SCALAR, "SCALAR")
CONTEXT_TYPE = ContextQLType(TypeKind.CONTEXT_TYPE, "CONTEXT_TYPE")
SET_TYPE = ContextQLType(TypeKind.SET_TYPE, "SET_TYPE")
SCORED_CTX = ContextQLType(TypeKind.SCORED_CTX, "SCORED_CTX")
BOOLEAN_CTX = ContextQLType(TypeKind.BOOLEAN_CTX, "BOOLEAN_CTX")
ENTITY_SET = ContextQLType(TypeKind.ENTITY_SET, "ENTITY_SET")


# ── Primitive ContextQL types (whitepaper Section 5.2) ──────────────────

class EntityKeyKind(str, Enum):
    """Entity key type categories for compatibility checking."""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    VARCHAR = "VARCHAR"
    UUID = "UUID"
    COMPOSITE = "COMPOSITE"


@dataclass(frozen=True, slots=True)
class EntityKeyType:
    """Type of an entity key column."""
    kind: EntityKeyKind
    column_name: str

    def is_compatible_with(self, other: EntityKeyType) -> bool:
        """Check join-compatibility per FORMALIST coercion rules.

        Integer types are mutually compatible (INTEGER <-> BIGINT).
        VARCHAR and UUID are only compatible with themselves.
        Composite keys require exact structural match.
        """
        if self.kind == other.kind:
            return True
        integer_types = {EntityKeyKind.INTEGER, EntityKeyKind.BIGINT}
        return self.kind in integer_types and other.kind in integer_types
