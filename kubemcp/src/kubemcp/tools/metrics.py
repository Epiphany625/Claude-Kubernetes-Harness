"""Live resource usage from ``metrics.k8s.io``.

metrics-server is an optional add-on, so its absence is an ordinary outcome
rather than a fault. When it is missing the tool says so plainly and names the
cause, instead of surfacing a bare 404 that reads like the query was wrong.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from pydantic import Field

from kubemcp.context import AppContext
from kubemcp.k8s.client import to_dict
from kubemcp.k8s.errors import ToolError
from kubemcp.models import LabelSelector, MetricsItem, MetricsResult, OptionalNamespace
from kubemcp.tools._common import kube_of, settings_of
from kubemcp.tools.resources import READ_ONLY

METRICS_API = "metrics.k8s.io/v1beta1"

_UNAVAILABLE = (
    "Resource metrics are not available on this cluster: the metrics.k8s.io API is not "
    "served, which means metrics-server is not installed or not yet ready. Install it to "
    "use this tool. Pod and node *specifications* (requests and limits) are still "
    "readable with get_resource."
)


def _usage(entry: dict[str, Any]) -> tuple[str | None, str | None]:
    usage = entry.get("usage") or {}
    return usage.get("cpu"), usage.get("memory")


def register(mcp: MCPServer) -> None:
    """Register the metrics tool."""

    @mcp.tool(
        title="Top nodes or pods",
        annotations=READ_ONLY,
        description=(
            "Current CPU and memory usage for nodes or pods, equivalent to 'kubectl top'. "
            "Requires metrics-server. Usage is what is actually being consumed now, which "
            "is not the same as the requests and limits declared on the spec."
        ),
    )
    async def get_top_metrics(
        ctx: Context[AppContext],
        scope: Annotated[
            Literal["nodes", "pods"],
            Field(description="Measure nodes or pods."),
        ] = "pods",
        namespace: OptionalNamespace = None,
        all_namespaces: Annotated[
            bool, Field(description="For pods, measure every namespace.")
        ] = False,
        label_selector: LabelSelector = None,
        limit: Annotated[int, Field(ge=1, le=500, description="Maximum rows.")] = 50,
    ) -> MetricsResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        kind = "NodeMetrics" if scope == "nodes" else "PodMetrics"

        try:
            resource = await kube.resource_for(METRICS_API, kind)
        except ToolError as exc:
            raise ToolError(_UNAVAILABLE) from exc

        query: dict[str, Any] = {}
        target_ns: str | None = None
        if scope == "pods" and not all_namespaces:
            target_ns = namespace or settings.default_namespace
            query["namespace"] = target_ns
        if label_selector:
            query["label_selector"] = label_selector

        try:
            raw = to_dict(await kube.get(resource, action=f"read {scope} metrics", **query))
        except ToolError as exc:
            if "Not found" in str(exc):
                raise ToolError(_UNAVAILABLE) from exc
            raise

        items: list[MetricsItem] = []
        for entry in (raw.get("items") or [])[:limit]:
            metadata = entry.get("metadata") or {}
            if scope == "nodes":
                cpu, memory = _usage(entry)
                items.append(MetricsItem(name=metadata.get("name", ""), cpu=cpu, memory=memory))
            else:
                containers = [
                    {
                        "name": container.get("name"),
                        "cpu": (container.get("usage") or {}).get("cpu"),
                        "memory": (container.get("usage") or {}).get("memory"),
                    }
                    for container in entry.get("containers") or []
                ]
                items.append(
                    MetricsItem(
                        name=metadata.get("name", ""),
                        namespace=metadata.get("namespace"),
                        containers=containers,
                    )
                )

        return MetricsResult(scope=scope, namespace=target_ns, count=len(items), items=items)
