"""Discovery tools: what this cluster serves, and how healthy it is."""

from __future__ import annotations

from typing import Annotated

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from pydantic import Field

from kubemcp.context import AppContext
from kubemcp.k8s.discovery import cluster_info
from kubemcp.k8s.discovery import list_api_resources as enumerate_resources
from kubemcp.models import (
    ApiResourceItem,
    ApiResourceListResult,
    ClusterInfoResult,
)
from kubemcp.tools._common import kube_of
from kubemcp.tools.resources import READ_ONLY


def register(mcp: MCPServer) -> None:
    """Register the discovery tools."""

    @mcp.tool(
        title="List API resources",
        annotations=READ_ONLY,
        description=(
            "List the resource types this cluster serves, with the apiVersion each one "
            "needs. Use this when you do not know a resource's apiVersion, or to find "
            "custom resources installed by an operator. Infrastructure groups "
            "(authorization, admission, flowcontrol) are hidden unless asked for."
        ),
    )
    async def list_api_resources(
        ctx: Context[AppContext],
        group: Annotated[
            str | None,
            Field(
                description=(
                    "Only this API group, e.g. 'apps' or 'ckh.io'. Use '' for the core "
                    "group. Omit for everything."
                )
            ),
        ] = None,
        namespaced: Annotated[
            bool | None,
            Field(description="Filter to namespaced (true) or cluster-scoped (false) kinds."),
        ] = None,
        verb: Annotated[
            str | None,
            Field(description="Only kinds supporting this verb, e.g. 'list' or 'delete'."),
        ] = None,
        include_infrastructure: Annotated[
            bool, Field(description="Include the cluster-plumbing API groups.")
        ] = False,
    ) -> ApiResourceListResult:
        kube = kube_of(ctx)
        found = await enumerate_resources(
            kube,
            group=group,
            namespaced=namespaced,
            verb=verb,
            include_infrastructure=include_infrastructure,
        )
        return ApiResourceListResult(
            count=len(found),
            items=[ApiResourceItem(**entry) for entry in found],
        )

    @mcp.tool(
        title="Get cluster info",
        annotations=READ_ONLY,
        description=(
            "Kubernetes version and a roll-up of node readiness. A good first call to "
            "confirm connectivity and to spot nodes that are not Ready."
        ),
    )
    async def get_cluster_info(ctx: Context[AppContext]) -> ClusterInfoResult:
        info = await cluster_info(kube_of(ctx))
        return ClusterInfoResult(**info)
