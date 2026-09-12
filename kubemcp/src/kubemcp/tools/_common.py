"""Helpers shared by the tool modules."""

from __future__ import annotations

from typing import Any

from kubernetes_asyncio.dynamic.resource import Resource
from mcp.server.mcpserver import Context

from kubemcp.config import Settings
from kubemcp.context import AppContext
from kubemcp.k8s.client import KubeClient, to_dict
from kubemcp.k8s.errors import ToolError
from kubemcp.shaping import fit_json, shape


def app(ctx: Context[AppContext]) -> AppContext:
    """Pull the shared application state off the request context."""
    return ctx.request_context.lifespan_context


def kube_of(ctx: Context[AppContext]) -> KubeClient:
    return app(ctx).kube


def settings_of(ctx: Context[AppContext]) -> Settings:
    """The settings governing tool behaviour.

    Always read configuration from the request context, never from
    ``KubeClient.settings``. The client owns only its own transport concerns
    (timeouts, concurrency); the server owns everything a tool needs to decide
    -- the default namespace, the response budgets, the field manager. Reading
    them off the client lets a client built with different settings silently
    override the server's, which is how a write reaches the wrong namespace.
    """
    return app(ctx).settings


def resolve_namespace(
    resource: Resource,
    namespace: str | None,
    *,
    default: str,
    all_namespaces: bool = False,
) -> str | None:
    """Work out which namespace an operation targets.

    Returns ``None`` for a cluster-scoped resource or an explicit cluster-wide
    listing. Passing a namespace for a cluster-scoped kind is an error rather
    than something to quietly ignore: the caller has misunderstood the resource,
    and saying so is more useful than silently doing something else.
    """
    if not resource.namespaced:
        if namespace:
            raise ToolError(
                f"{resource.kind} is cluster-scoped, so it has no namespace. "
                f"Remove the namespace argument ({namespace!r})."
            )
        return None
    if all_namespaces:
        return None
    return namespace or default


def describe_target(kind: str, name: str | None, namespace: str | None) -> str:
    """Render an object reference for an error message."""
    where = f"{namespace}/{name}" if namespace and name else (name or namespace or "")
    return f"{kind} {where}".strip()


def shape_items(
    items: list[dict[str, Any]],
    *,
    view: str,
    kind: str,
    budget: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Shape, cap and budget a list of objects.

    Returns ``(items, dropped, truncated)``. Two ceilings apply: the caller's
    item limit and the response byte budget, because a hundred small objects and
    three enormous ones are both ways to swamp a context window.
    """
    capped = items[:limit]
    dropped = len(items) - len(capped)
    shaped = [shape(item, view=view, kind=kind) for item in capped]
    kept, budget_dropped = fit_json(shaped, budget)
    total_dropped = dropped + budget_dropped
    return kept, total_dropped, total_dropped > 0


def object_identity(obj: Any, *, fallback_kind: str, fallback_version: str) -> dict[str, Any]:
    """Extract apiVersion/kind/name/namespace/resourceVersion from a result."""
    body = to_dict(obj)
    metadata = body.get("metadata") or {}
    return {
        "api_version": body.get("apiVersion") or fallback_version,
        "kind": body.get("kind") or fallback_kind,
        "name": metadata.get("name", ""),
        "namespace": metadata.get("namespace"),
        "resource_version": metadata.get("resourceVersion"),
    }
