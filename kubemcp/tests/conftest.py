"""Shared fixtures.

The integration fixtures wire a real MCP client to a real MCP server to a real
``kubernetes_asyncio`` client to a real HTTP server. The only thing faked is the
cluster's contents, so a passing test says something about the actual stack.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from aiohttp import web
from kubernetes_asyncio.client import ApiClient, Configuration
from kubernetes_asyncio.dynamic import DynamicClient
from mcp import Client

from kubemcp.config import Settings
from kubemcp.k8s.client import KubeClient
from kubemcp.server import build_server
from tests.fake_apiserver import FakeCluster


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Run anyio tests on asyncio, matching how the server runs in production."""
    return "asyncio"


@pytest.fixture
def settings() -> Settings:
    """Settings with the environment and any .env file ignored."""
    return Settings(
        _env_file=None,
        default_namespace="default",
        max_response_bytes=96_000,
        max_list_items=200,
    )


@pytest.fixture
def cluster() -> FakeCluster:
    """An empty fake cluster. Seed it in the test."""
    return FakeCluster()


@pytest.fixture
async def apiserver(cluster: FakeCluster) -> AsyncIterator[str]:
    """Serve ``cluster`` over HTTP on a loopback port; yield its base URL."""
    runner = web.AppRunner(cluster.build_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


@pytest.fixture
async def kube(apiserver: str, settings: Settings) -> AsyncIterator[KubeClient]:
    """A KubeClient pointed at the fake apiserver."""
    configuration = Configuration(host=apiserver)
    configuration.verify_ssl = False

    client = KubeClient(settings)
    client._api_client = ApiClient(configuration=configuration)
    client._dynamic = await DynamicClient(client._api_client)
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def mcp_client(kube: KubeClient, settings: Settings) -> AsyncIterator[Client]:
    """An MCP client connected in-process to a server backed by the fake cluster.

    This runs the genuine protocol -- initialize, tools/list, tools/call -- so
    schema generation, argument validation and result validation are all covered.
    """
    server = build_server(settings, kube=kube)
    async with Client(server) as client:
        yield client


async def call(client: Client, tool: str, **arguments: Any) -> Any:
    """Call a tool and return its structured content, failing on a tool error."""
    result = await client.call_tool(tool, arguments)
    if result.is_error:
        text = "".join(getattr(block, "text", "") for block in result.content)
        raise AssertionError(f"{tool} returned an error: {text}")
    return result.structured_content


async def call_raw(client: Client, tool: str, **arguments: Any) -> Any:
    """Call a tool and return the raw result, errors included."""
    return await client.call_tool(tool, arguments)


def error_text(result: Any) -> str:
    """Concatenate the text blocks of a tool result."""
    return "".join(getattr(block, "text", "") for block in result.content)


@pytest.fixture
def pod_factory() -> Iterator[Any]:
    """Build pod manifests without repeating the boilerplate."""

    def make(
        name: str,
        namespace: str = "default",
        *,
        phase: str = "Running",
        labels: dict[str, str] | None = None,
        ready: bool = True,
        restarts: int = 0,
        waiting_reason: str | None = None,
        last_termination: dict[str, Any] | None = None,
        node: str | None = "node-1",
    ) -> dict[str, Any]:
        state: dict[str, Any] = (
            {"waiting": {"reason": waiting_reason, "message": f"{waiting_reason} detail"}}
            if waiting_reason
            else {"running": {"startedAt": "2026-01-01T00:00:00Z"}}
        )
        container: dict[str, Any] = {
            "name": "app",
            "ready": ready,
            "restartCount": restarts,
            "state": state,
        }
        if last_termination:
            container["lastState"] = {"terminated": last_termination}
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels or {"app": "web"},
                "annotations": {
                    "kubectl.kubernetes.io/last-applied-configuration": '{"huge":"blob"}',
                    "team": "platform",
                },
            },
            "spec": {"nodeName": node, "containers": [{"name": "app", "image": "nginx:1.27"}]},
            "status": {
                "phase": phase,
                "podIP": "10.0.0.5",
                "containerStatuses": [container],
                "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            },
        }

    yield make
