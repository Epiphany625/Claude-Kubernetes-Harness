"""Server construction: lifespan, health probes, transport, tool registration."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from kubemcp import __version__
from kubemcp.config import Settings, load_settings
from kubemcp.context import AppContext
from kubemcp.k8s.client import ClusterUnreachable, KubeClient
from kubemcp.tools import register_all

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _Runtime:
    """Mutable handoff from the lifespan to the health routes."""

    kube: KubeClient | None = None


INSTRUCTIONS: str = """\
Tools for inspecting and changing a Kubernetes cluster.

Resources are addressed by apiVersion and kind, so the same tools work on built-in
resources and on custom resources installed by operators. When you do not know a
kind's apiVersion, call list_api_resources.

Diagnosing something that is broken: start with describe_resource, which returns the
object's state, its recent events and observations about what looks wrong in one call.
For a pod that is restarting, the reason it failed is in the previous container's logs
-- get_pod_logs with previous=true.

Responses are summarised and size-capped by default to stay readable. A result marked
truncated is incomplete: narrow the query with a label or field selector rather than
treating it as the whole picture. Use view='full' only when you need a field the
summary omits.

Changing state: apply_resource is the idempotent way to reach a desired state; prefer
it to create_resource. Every mutating tool accepts dry_run=true to validate first.
"""


def build_server(settings: Settings | None = None, kube: KubeClient | None = None) -> MCPServer:
    """Construct the MCP server.

    ``kube`` is injectable so tests can supply a client pointed at a fake
    apiserver; in production the lifespan builds one from ``settings``.
    """
    resolved = settings or load_settings()
    # The readiness probe lives outside the MCP protocol and so cannot reach the
    # lifespan context, but it still has to answer for the client the lifespan
    # builds. This holder is the handoff: without it /readyz would report
    # "starting" forever in production -- the pod would pass liveness, fail
    # readiness, and never receive traffic.
    runtime = _Runtime()

    @asynccontextmanager
    async def lifespan(_: MCPServer) -> AsyncGenerator[AppContext]:
        # Connecting once here, rather than per call, is what makes discovery
        # affordable: the API surface is fetched and cached at startup.
        client = kube or KubeClient(resolved)
        owned = kube is None
        if owned:
            try:
                await client.connect()
            except ClusterUnreachable as exc:
                # One clear line, because in Kubernetes this is all anyone sees
                # before the pod restarts.
                logger.error("kubemcp cannot start: %s", exc)
                raise
        runtime.kube = client
        try:
            yield AppContext(kube=client, settings=resolved)
        finally:
            runtime.kube = None
            if owned:
                await client.close()

    mcp: MCPServer = MCPServer(
        name="kubemcp",
        title="Kubernetes",
        version=__version__,
        instructions=INSTRUCTIONS,
        lifespan=lifespan,
        log_level=resolved.log_level,
    )

    register_all(mcp)
    _register_health_routes(mcp, runtime)
    return mcp


def _register_health_routes(mcp: MCPServer, runtime: _Runtime) -> None:
    """Add the probe endpoints Kubernetes needs.

    The SDK ships no health endpoint, and these are deliberately outside the MCP
    protocol so a kubelet can call them without speaking MCP.
    """

    @mcp.custom_route("/healthz", methods=["GET"])  # type: ignore[untyped-decorator]
    async def healthz(_: Request) -> Response:
        # Liveness: the process is up. It must not depend on the apiserver, or a
        # cluster blip would have Kubernetes restart every replica at once.
        return JSONResponse({"status": "ok", "version": __version__})

    @mcp.custom_route("/readyz", methods=["GET"])  # type: ignore[untyped-decorator]
    async def readyz(_: Request) -> Response:
        # Readiness: we can actually serve, which means reaching the apiserver.
        client = runtime.kube
        if client is None:
            return JSONResponse({"status": "starting"}, status_code=503)
        if await client.ready():
            return JSONResponse({"status": "ok"})
        return JSONResponse({"status": "apiserver unreachable"}, status_code=503)


def transport_security(settings: Settings) -> TransportSecuritySettings:
    """Build the Host/Origin allowlist for the HTTP transport.

    The transport rejects unknown Host headers as DNS-rebinding protection. In a
    cluster the Host is the Service DNS name, so leaving this unset would reject
    every request the harness makes -- which is why it is configurable and why
    the README calls it out. An empty allowlist means "allow any", which has to
    be stated explicitly rather than left to the default.
    """
    if not settings.allowed_hosts:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        allowed_hosts=settings.allowed_hosts,
        allowed_origins=settings.allowed_origins,
    )


def build_http_app(settings: Settings | None = None) -> Starlette:
    """Build the ASGI app, for running under an external server."""
    resolved = settings or load_settings()
    mcp = build_server(resolved)
    return mcp.streamable_http_app(
        streamable_http_path=resolved.mcp_path,
        # Stateless + JSON responses: every request is self-contained, so
        # replicas scale behind an ordinary Service with no session affinity.
        stateless_http=True,
        json_response=True,
        transport_security=transport_security(resolved),
        host=resolved.host,
    )


def run(settings: Settings | None = None) -> None:
    """Run the server on the configured transport."""
    resolved = settings or load_settings()
    logging.basicConfig(
        level=resolved.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    mcp = build_server(resolved)
    if resolved.transport == "stdio":
        logger.info("kubemcp %s starting on stdio", __version__)
        mcp.run("stdio")
        return

    logger.info(
        "kubemcp %s serving streamable HTTP on %s:%s%s",
        __version__,
        resolved.host,
        resolved.port,
        resolved.mcp_path,
    )
    if not resolved.allowed_hosts:
        logger.warning(
            "KUBEMCP_ALLOWED_HOSTS is unset, so any Host header is accepted. Set it to "
            "the Service DNS name in production."
        )
    mcp.run(
        "streamable-http",
        host=resolved.host,
        port=resolved.port,
        streamable_http_path=resolved.mcp_path,
        stateless_http=True,
        json_response=True,
        transport_security=transport_security(resolved),
    )
