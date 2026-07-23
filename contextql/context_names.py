"""Canonical context-name handling.

Context names are case-insensitive and are resolved inside a namespace.  This
module is deliberately dependency-free so the parser, semantic layer, runtime,
and server persistence adapter can share exactly the same normalization rules.
"""
from __future__ import annotations

from dataclasses import dataclass

DEFAULT_NAMESPACE = "default"


def _normalize_identifier(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"Context {label} must not be empty.")
    return normalized.lower()


@dataclass(frozen=True)
class QualifiedContextName:
    """A display name with a stable, case-insensitive catalog key."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace.strip():
            raise ValueError("Context namespace must not be empty.")
        if not self.name.strip():
            raise ValueError("Context name must not be empty.")

    @property
    def key(self) -> str:
        """String key used by the current in-memory catalog."""
        return (
            f"{_normalize_identifier(self.namespace, label='namespace')}."
            f"{_normalize_identifier(self.name, label='name')}"
        )

    @property
    def display(self) -> str:
        if self.namespace.lower() == DEFAULT_NAMESPACE:
            return self.name
        return f"{self.namespace}.{self.name}"


def qualify_context_name(
    value: str,
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
) -> QualifiedContextName:
    """Split a possibly-qualified name using the final dot as namespace."""
    text = value.strip()
    if "." in text:
        namespace, name = text.rsplit(".", 1)
    else:
        namespace, name = default_namespace, text
    return QualifiedContextName(namespace=namespace, name=name)


def context_catalog_key(
    name: str,
    namespace: str | None = None,
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
) -> str:
    """Return the canonical catalog key for a name/namespace pair."""
    if namespace is not None:
        return QualifiedContextName(namespace=namespace, name=name).key
    return qualify_context_name(
        name, default_namespace=default_namespace
    ).key
