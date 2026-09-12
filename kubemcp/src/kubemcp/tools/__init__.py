"""MCP tool definitions, grouped by concern.

Adding a module here is the only step needed to expose its tools: ``register_all``
is what :mod:`kubemcp.server` calls.
"""

from __future__ import annotations

from mcp.server import MCPServer

from kubemcp.tools import discovery, exec, metrics, resources, workloads

__all__ = ["register_all"]

_MODULES = (discovery, resources, workloads, exec, metrics)


def register_all(mcp: MCPServer) -> None:
    """Register every tool this server exposes."""
    for module in _MODULES:
        module.register(mcp)
