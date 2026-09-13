"""Serving concerns: health probes, transport security, and the ASGI app.

These cover the wiring that decides whether the server is reachable at all
inside a cluster -- the part that is invisible until a deploy fails.
"""

from __future__ import annotations

import pytest
from httpx2 import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.testclient import TestClient

from kubemcp.config import Settings
from kubemcp.k8s.client import KubeClient
from kubemcp.server import build_http_app, build_server, transport_security

pytestmark = pytest.mark.anyio


def _app(settings: Settings, kube: KubeClient | None) -> Starlette:
    server = build_server(settings, kube=kube)
    return server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=transport_security(settings),
        host=settings.host,
    )


class TestHealthProbes:
    """The probes Kubernetes uses to decide whether to restart this pod and
    whether to send it traffic.

    These are driven through the ASGI app, lifespan included, rather than by
    calling the client directly: the bug worth catching here is the probe
    reporting the *wrong* thing while the client underneath is fine.
    """

    def test_healthz_answers_without_touching_the_apiserver(self, settings: Settings) -> None:
        # Liveness must not depend on the cluster: a control-plane blip would
        # otherwise have Kubernetes restart every replica at once.
        with TestClient(_app(settings, kube=None)) as client:
            response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    async def test_readyz_becomes_ready_once_the_lifespan_has_run(
        self, settings: Settings, kube: KubeClient
    ) -> None:
        # Regression test. The readiness route is registered outside the MCP
        # protocol and cannot see the lifespan context, so it reads the client
        # through a holder the lifespan fills in. When that handoff was missing,
        # /readyz answered "starting" forever: the pod stayed permanently
        # unready and never received traffic, while every other test passed.
        app = _app(settings, kube=kube)
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://kubemcp") as client:
                response = await client.get("/readyz")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    async def test_readyz_reports_unready_when_the_apiserver_is_gone(
        self, settings: Settings, kube: KubeClient
    ) -> None:
        app = _app(settings, kube=kube)
        async with app.router.lifespan_context(app):
            kube.api_client.configuration.host = "http://127.0.0.1:1"
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://kubemcp") as client:
                response = await client.get("/readyz")
        assert response.status_code == 503

    async def test_readyz_is_unready_before_startup(self, settings: Settings) -> None:
        # The lifespan is deliberately not entered here: while the process is
        # still starting there is no client yet, and "not ready" is the correct
        # answer. Liveness still passes, so Kubernetes waits rather than
        # restarting the pod.
        app = _app(settings, kube=None)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://kubemcp") as client:
            ready = await client.get("/readyz")
            alive = await client.get("/healthz")
        assert ready.status_code == 503
        assert ready.json()["status"] == "starting"
        assert alive.status_code == 200


class TestTransportSecurity:
    def test_defaults_to_allowing_any_host(self) -> None:
        # Unset means "allow any", applied explicitly. The transport's default
        # would otherwise reject every in-cluster request, since the Host is the
        # Service DNS name rather than localhost.
        security = transport_security(Settings(_env_file=None))
        assert security.enable_dns_rebinding_protection == False

    def test_uses_the_configured_allowlist(self) -> None:
        settings = Settings(
            _env_file=None,
            allowed_hosts="kubemcp.ckh.svc.cluster.local:8080,localhost:8080",
        )
        security = transport_security(settings)
        assert security.allowed_hosts == [
            "kubemcp.ckh.svc.cluster.local:8080",
            "localhost:8080",
        ]

    def test_an_allowlisted_host_is_accepted(self, settings: Settings) -> None:
        settings.allowed_hosts = ["kubemcp.ckh.svc.cluster.local:8080"]
        app = build_server(settings, kube=None).streamable_http_app(
            stateless_http=True,
            json_response=True,
            transport_security=transport_security(settings),
        )
        with TestClient(app) as client:
            response = client.post(
                "/mcp",
                headers={
                    "Host": "kubemcp.ckh.svc.cluster.local:8080",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                },
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
        assert response.status_code != 421

    def test_a_host_outside_the_allowlist_is_rejected(self, settings: Settings) -> None:
        settings.allowed_hosts = ["kubemcp.ckh.svc.cluster.local:8080"]
        app = build_server(settings, kube=None).streamable_http_app(
            stateless_http=True,
            json_response=True,
            transport_security=transport_security(settings),
        )
        with TestClient(app) as client:
            response = client.post(
                "/mcp",
                headers={
                    "Host": "evil.example.com",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                },
                json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
            )
        assert response.status_code >= 400


class TestHttpApp:
    def test_serves_mcp_at_the_configured_path(self) -> None:
        settings = Settings(_env_file=None, mcp_path="/custom-mcp")
        app = build_http_app(settings)
        paths = {getattr(route, "path", None) for route in app.routes}
        assert "/healthz" in paths
        assert any("custom-mcp" in str(path) for path in paths if path)

    def test_builds_without_a_cluster_connection(self) -> None:
        # Construction must not require the apiserver, or the pod could never
        # start before the control plane is reachable.
        assert build_http_app(Settings(_env_file=None)) is not None


class TestInstructions:
    async def test_the_server_tells_the_caller_how_to_use_it(self, mcp_client: object) -> None:
        from mcp import Client

        assert isinstance(mcp_client, Client)
        instructions = mcp_client.instructions or ""
        # The instructions carry the guidance that does not fit in any single
        # tool description: where to start, and how to read a truncated result.
        assert "describe_resource" in instructions
        assert "previous=true" in instructions
        assert "truncated" in instructions
