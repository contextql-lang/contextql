"""ContextQL provider interfaces and reference implementations.

Public API (backward-compatible)::

    from contextql.providers import MCPProvider, MCPResult, RemoteProvider, RemoteResult

Reference implementations::

    from contextql.providers import FraudDetectionMCP, PriorityMCP, JiraRemoteProvider
"""

from contextql.providers.base import (
    MCPProvider,
    MCPResult,
    RemoteProvider,
    RemoteResult,
)
from contextql.providers.fraud import FraudDetectionMCP
from contextql.providers.jira import JiraRemoteProvider
from contextql.providers.priority import PriorityMCP

__all__ = [
    # Protocols + dataclasses
    "MCPProvider",
    "MCPResult",
    "RemoteProvider",
    "RemoteResult",
    # Concrete implementations
    "FraudDetectionMCP",
    "PriorityMCP",
    "JiraRemoteProvider",
]
