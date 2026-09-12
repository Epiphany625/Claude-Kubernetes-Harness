"""End-to-end tests against a real Kubernetes cluster.

Deselected by default (``-m 'not e2e'`` in ``pyproject.toml``) and skipped
outright when no cluster is reachable, so the ordinary suite never needs one.

Run with a cluster up (minikube, kind, Docker Desktop):

    uv run pytest -m e2e

Everything is created inside a throwaway namespace that is deleted afterwards,
whatever the outcome.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import anyio
import pytest
from mcp import Client

from kubemcp.config import Settings
from kubemcp.k8s.client import KubeClient
from kubemcp.server import build_server
from tests.conftest import call, call_raw

pytestmark = [pytest.mark.e2e, pytest.mark.anyio]

BUSYBOX = os.environ.get("KUBEMCP_E2E_IMAGE", "busybox:1.36")


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def live_kube() -> AsyncIterator[KubeClient]:
    """A client against the real cluster, or skip if there isn't one."""
    settings = Settings(_env_file=None, request_timeout_seconds=60)
    client = KubeClient(settings)
    try:
        await client.connect()
        if not await client.ready():
            pytest.skip("no reachable Kubernetes cluster")
    except Exception as exc:
        pytest.skip(f"no reachable Kubernetes cluster: {exc}")
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
async def namespace(live_kube: KubeClient) -> AsyncIterator[str]:
    """A throwaway namespace, removed afterwards."""
    name = f"kubemcp-e2e-{uuid.uuid4().hex[:8]}"
    resource = await live_kube.resource_for("v1", "Namespace")
    await live_kube.create(
        resource,
        action=f"create namespace {name}",
        body={"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}},
    )
    try:
        yield name
    finally:
        await live_kube.delete(
            resource,
            action=f"delete namespace {name}",
            name=name,
            propagation_policy="Background",
        )


@pytest.fixture
async def client(live_kube: KubeClient, namespace: str) -> AsyncIterator[Client]:
    settings = Settings(_env_file=None, default_namespace=namespace)
    async with Client(build_server(settings, kube=live_kube)) as mcp_client:
        yield mcp_client


async def _wait_for_pod(
    mcp_client: Client, name: str, *, deadline_seconds: float = 120.0
) -> dict[str, Any]:
    """Poll until the pod is Running, or fail with what it was doing instead."""
    deadline = time.monotonic() + deadline_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = await call(mcp_client, "describe_resource", api_version="v1", kind="Pod", name=name)
        if last["summary"].get("phase") == "Running":
            return last
        await anyio.sleep(2)
    pytest.fail(f"pod {name} never reached Running: {last.get('summary')} {last.get('hints')}")


class TestLiveCluster:
    async def test_reports_cluster_info(self, client: Client) -> None:
        info = await call(client, "get_cluster_info")
        assert info["version"]
        assert info["nodes_total"] >= 1

    async def test_discovers_real_api_resources(self, client: Client) -> None:
        result = await call(client, "list_api_resources")
        kinds = {item["kind"] for item in result["items"]}
        assert {"Pod", "Service", "Deployment", "Node"} <= kinds

    async def test_full_lifecycle(self, client: Client, namespace: str) -> None:
        """Apply a pod, read it, log it, exec in it, then delete it."""
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "e2e-pod", "labels": {"app": "e2e"}},
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "app",
                        "image": BUSYBOX,
                        "command": ["sh", "-c", "echo hello-from-kubemcp; sleep 3600"],
                    }
                ],
            },
        }

        applied = await call(client, "apply_resource", manifest=manifest, namespace=namespace)
        assert applied["action"] == "configured"

        # Applying the identical manifest again must be a no-op, not an error.
        await call(client, "apply_resource", manifest=manifest, namespace=namespace)

        described = await _wait_for_pod(client, "e2e-pod")
        assert described["summary"]["phase"] == "Running"

        listed = await call(
            client,
            "list_resources",
            api_version="v1",
            kind="Pod",
            namespace=namespace,
            label_selector="app=e2e",
        )
        assert [item["name"] for item in listed["items"]] == ["e2e-pod"]

        logs = await call(client, "get_pod_logs", name="e2e-pod", namespace=namespace)
        assert "hello-from-kubemcp" in logs["logs"]

        result = await call(
            client,
            "exec_in_pod",
            name="e2e-pod",
            namespace=namespace,
            command=["sh", "-c", "echo exec-works"],
        )
        assert result["exit_code"] == 0
        assert "exec-works" in result["stdout"]

        failing = await call(
            client,
            "exec_in_pod",
            name="e2e-pod",
            namespace=namespace,
            command=["sh", "-c", "exit 42"],
        )
        assert failing["exit_code"] == 42

        deleted = await call(
            client,
            "delete_resource",
            api_version="v1",
            kind="Pod",
            name="e2e-pod",
            namespace=namespace,
        )
        assert deleted["action"] == "deleted"

    async def test_pruning_actually_shrinks_a_real_object(
        self, client: Client, namespace: str
    ) -> None:
        """Real objects carry the bulk that shaping exists to remove."""
        import json

        await call(
            client,
            "apply_resource",
            namespace=namespace,
            manifest={
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "e2e-cfg"},
                "data": {"k": "v"},
            },
        )
        summary = await call(
            client,
            "get_resource",
            api_version="v1",
            kind="ConfigMap",
            name="e2e-cfg",
            namespace=namespace,
        )
        full = await call(
            client,
            "get_resource",
            api_version="v1",
            kind="ConfigMap",
            name="e2e-cfg",
            namespace=namespace,
            view="full",
        )
        assert "managedFields" not in json.dumps(full)
        assert len(json.dumps(summary)) <= len(json.dumps(full))

    async def test_a_missing_object_is_reported_as_an_error(
        self, client: Client, namespace: str
    ) -> None:
        result = await call_raw(
            client,
            "get_resource",
            api_version="v1",
            kind="Pod",
            name="definitely-not-here",
            namespace=namespace,
        )
        assert result.is_error
        assert result.structured_content is None

    async def test_crashlooping_pod_is_diagnosed(self, client: Client, namespace: str) -> None:
        """The case the harness exists for: explain why a pod keeps dying."""
        await call(
            client,
            "apply_resource",
            namespace=namespace,
            manifest={
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "e2e-crash"},
                "spec": {
                    "restartPolicy": "Always",
                    "containers": [
                        {
                            "name": "app",
                            "image": BUSYBOX,
                            "command": ["sh", "-c", "echo dying-now >&2; exit 1"],
                        }
                    ],
                },
            },
        )

        deadline = time.monotonic() + 120
        described: dict[str, Any] = {}
        while time.monotonic() < deadline:
            described = await call(
                client,
                "describe_resource",
                api_version="v1",
                kind="Pod",
                name="e2e-crash",
                namespace=namespace,
            )
            if described["hints"]:
                break
            await anyio.sleep(3)

        assert described["hints"], f"no hint produced: {described.get('summary')}"
        hints = " ".join(described["hints"])
        assert "previous=true" in hints or "terminated" in hints

        # Whether a dead container's logs are still on the node is decided by
        # the kubelet's log retention and GC, not by this server, so the
        # content is checked only when the node still has it. What must hold is
        # that the call is well-formed and the diagnosis above was produced.
        previous = await call_raw(
            client, "get_pod_logs", name="e2e-crash", namespace=namespace, previous=True
        )
        if not previous.is_error:
            logs = previous.structured_content["logs"]
            assert "dying-now" in logs or "unable to retrieve" in logs
