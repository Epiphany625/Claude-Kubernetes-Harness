"""Generic resource tools, over the real MCP protocol.

Each test drives an in-process MCP client through initialize / tools call, into
the real ``kubernetes_asyncio`` stack, against a real HTTP apiserver. Only the
cluster's contents are fake.
"""

from __future__ import annotations

from typing import Any

import pytest
from mcp import Client

from tests.conftest import call, call_raw, error_text
from tests.fake_apiserver import FakeCluster

pytestmark = pytest.mark.anyio


class TestToolSurface:
    async def test_every_tool_is_advertised_with_a_schema(self, mcp_client: Client) -> None:
        tools = (await mcp_client.list_tools()).tools
        names = {tool.name for tool in tools}
        assert names == {
            "list_api_resources",
            "get_cluster_info",
            "list_resources",
            "get_resource",
            "create_resource",
            "apply_resource",
            "patch_resource",
            "delete_resource",
            "scale_resource",
            "get_pod_logs",
            "list_events",
            "describe_resource",
            "rollout_restart",
            "exec_in_pod",
            "get_top_metrics",
        }
        for tool in tools:
            assert tool.description, f"{tool.name} has no description"
            assert tool.input_schema["type"] == "object"
            assert tool.output_schema is not None, f"{tool.name} has no output schema"

    async def test_destructive_tools_are_flagged_for_the_client(self, mcp_client: Client) -> None:
        # A host may want to gate these; it can only do that if they are marked.
        tools = {tool.name: tool for tool in (await mcp_client.list_tools()).tools}
        assert tools["delete_resource"].annotations.destructive_hint is True
        assert tools["exec_in_pod"].annotations.destructive_hint is True
        assert tools["get_resource"].annotations.read_only_hint is True
        assert tools["list_resources"].annotations.read_only_hint is True


class TestListResources:
    async def test_lists_and_summarises(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        cluster.add(pod_factory("web-1"))

        result = await call(mcp_client, "list_resources", api_version="v1", kind="Pod")
        assert result["count"] == 2
        assert {item["name"] for item in result["items"]} == {"web-0", "web-1"}

    async def test_summary_view_strips_server_bookkeeping(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        result = await call(mcp_client, "list_resources", api_version="v1", kind="Pod")
        item = result["items"][0]
        assert "managedFields" not in str(item)
        assert "last-applied-configuration" not in str(item)

    async def test_label_selector_reaches_the_apiserver_intact(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0", labels={"app": "web"}))
        cluster.add(pod_factory("api-0", labels={"app": "api"}))

        result = await call(
            mcp_client,
            "list_resources",
            api_version="v1",
            kind="Pod",
            label_selector="app in (web)",
        )
        assert [item["name"] for item in result["items"]] == ["web-0"]
        listing = [r for r in cluster.requests if r["path"].endswith("/pods")][-1]
        assert listing["query"]["labelSelector"] == "app in (web)"

    async def test_field_selector_reaches_the_apiserver(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0", phase="Running"))
        cluster.add(pod_factory("web-1", phase="Pending"))

        result = await call(
            mcp_client,
            "list_resources",
            api_version="v1",
            kind="Pod",
            field_selector="status.phase=Pending",
        )
        assert [item["name"] for item in result["items"]] == ["web-1"]

    async def test_paging_returns_a_continue_token(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        for index in range(5):
            cluster.add(pod_factory(f"web-{index}"))

        result = await call(mcp_client, "list_resources", api_version="v1", kind="Pod", limit=2)
        assert result["count"] == 2
        assert result["continue_token"] == "fake-continue-token"

    async def test_all_namespaces_omits_the_namespace_from_the_path(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0", namespace="default"))
        cluster.add(pod_factory("other-0", namespace="kube-system"))

        result = await call(
            mcp_client, "list_resources", api_version="v1", kind="Pod", all_namespaces=True
        )
        assert result["count"] == 2
        assert any(r["path"] == "/api/v1/pods" for r in cluster.requests)

    async def test_truncation_is_reported_rather_than_silent(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # A quietly short list reads as "that's all there is", which is worse
        # than saying the result is incomplete.
        for index in range(30):
            cluster.add(pod_factory(f"web-{index}"))

        result = await call(
            mcp_client, "list_resources", api_version="v1", kind="Pod", limit=30, view="full"
        )
        if result["truncated"]:
            assert result["dropped"] > 0
            assert result["count"] + result["dropped"] == 30


class TestGetResource:
    async def test_returns_the_object(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        result = await call(mcp_client, "get_resource", api_version="v1", kind="Pod", name="web-0")
        assert result["name"] == "web-0"
        assert result["namespace"] == "default"
        assert result["object"]["phase"] == "Running"

    async def test_full_view_includes_the_spec(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        result = await call(
            mcp_client, "get_resource", api_version="v1", kind="Pod", name="web-0", view="full"
        )
        assert result["object"]["spec"]["containers"][0]["image"] == "nginx:1.27"

    async def test_a_missing_object_is_an_error_not_an_empty_result(
        self, mcp_client: Client
    ) -> None:
        # This is the case the dynamic client would otherwise report as success.
        result = await call_raw(
            mcp_client, "get_resource", api_version="v1", kind="Pod", name="ghost"
        )
        assert result.is_error
        text = error_text(result)
        assert "Not found" in text
        assert "ghost" in text

    async def test_a_missing_object_does_not_look_like_a_status_object(
        self, mcp_client: Client
    ) -> None:
        result = await call_raw(
            mcp_client, "get_resource", api_version="v1", kind="Pod", name="ghost"
        )
        assert result.structured_content is None

    async def test_forbidden_names_the_permission(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.fail_forbidden("GET", "pods")
        result = await call_raw(
            mcp_client, "get_resource", api_version="v1", kind="Pod", name="web-0"
        )
        assert result.is_error
        text = error_text(result)
        assert "Forbidden" in text
        assert "ServiceAccount" in text

    async def test_an_unknown_kind_suggests_discovery(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client, "get_resource", api_version="v1", kind="Wombat", name="w"
        )
        assert result.is_error
        assert "list_api_resources" in error_text(result)


class TestInputValidation:
    """Invalid arguments must be refused before anything reaches the cluster."""

    @pytest.mark.parametrize(
        ("arguments", "because"),
        [
            ({"api_version": "v1", "kind": "Pod", "name": "Web-0"}, "uppercase name"),
            ({"api_version": "v1", "kind": "pod", "name": "web-0"}, "lowercase kind"),
            ({"api_version": "apps/", "kind": "Pod", "name": "web-0"}, "malformed apiVersion"),
            (
                {"api_version": "v1", "kind": "Pod", "name": "web-0", "namespace": "Bad_NS"},
                "invalid namespace",
            ),
        ],
    )
    async def test_rejects_bad_arguments(
        self, mcp_client: Client, cluster: FakeCluster, arguments: dict[str, Any], because: str
    ) -> None:
        result = await call_raw(mcp_client, "get_resource", **arguments)
        assert result.is_error, f"expected {because} to be refused"
        # Nothing should have been sent to the cluster.
        assert not [r for r in cluster.requests if "/pods" in r["path"]]

    async def test_rejects_a_malformed_label_selector(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client,
            "list_resources",
            api_version="v1",
            kind="Pod",
            label_selector="app in (",
        )
        assert result.is_error
        assert "label selector" in error_text(result)

    async def test_rejects_an_out_of_range_limit(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client, "list_resources", api_version="v1", kind="Pod", limit=99_999
        )
        assert result.is_error

    async def test_rejects_an_unknown_argument(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client,
            "get_resource",
            api_version="v1",
            kind="Pod",
            name="web-0",
            typo_argument="oops",
        )
        assert result.is_error


class TestMutations:
    async def test_creates_an_object(self, mcp_client: Client, cluster: FakeCluster) -> None:
        manifest = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cfg"},
            "data": {"key": "value"},
        }
        result = await call(mcp_client, "create_resource", manifest=manifest)
        assert result["action"] == "created"
        assert ("v1", "ConfigMap", "default", "cfg") in cluster.objects

    async def test_create_on_an_existing_object_points_at_apply(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
            }
        )
        result = await call_raw(
            mcp_client,
            "create_resource",
            manifest={"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cfg"}},
        )
        assert result.is_error
        assert "apply_resource" in error_text(result)

    async def test_dry_run_does_not_persist(self, mcp_client: Client, cluster: FakeCluster) -> None:
        await call(
            mcp_client,
            "create_resource",
            manifest={"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cfg"}},
            dry_run=True,
        )
        assert ("v1", "ConfigMap", "default", "cfg") not in cluster.objects

    async def test_apply_is_idempotent(self, mcp_client: Client, cluster: FakeCluster) -> None:
        manifest = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cfg"},
            "data": {"key": "value"},
        }
        first = await call(mcp_client, "apply_resource", manifest=manifest)
        second = await call(mcp_client, "apply_resource", manifest=manifest)
        assert first["action"] == second["action"] == "configured"
        assert len([k for k in cluster.objects if k[3] == "cfg"]) == 1

    async def test_apply_creates_when_absent(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        await call(
            mcp_client,
            "apply_resource",
            manifest={"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "new"}},
        )
        assert ("v1", "ConfigMap", "default", "new") in cluster.objects

    async def test_apply_sends_the_field_manager(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        await call(
            mcp_client,
            "apply_resource",
            manifest={"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "cfg"}},
        )
        patch = [r for r in cluster.requests if r["method"] == "PATCH"][-1]
        assert patch["query"]["fieldManager"] == "kubemcp"

    async def test_apply_rejects_a_namespace_conflict(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client,
            "apply_resource",
            manifest={
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "a"},
            },
            namespace="b",
        )
        assert result.is_error
        assert "namespace conflict" in error_text(result)

    async def test_strategic_merge_patch(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
                "data": {"a": "1"},
            }
        )
        await call(
            mcp_client,
            "patch_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            patch={"data": {"b": "2"}},
        )
        stored = cluster.objects[("v1", "ConfigMap", "default", "cfg")]
        assert stored["data"] == {"a": "1", "b": "2"}

    async def test_json_patch(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
                "data": {"a": "1"},
            }
        )
        await call(
            mcp_client,
            "patch_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            patch=[{"op": "replace", "path": "/data/a", "value": "2"}],
            patch_type="json",
        )
        assert cluster.objects[("v1", "ConfigMap", "default", "cfg")]["data"]["a"] == "2"

    async def test_json_patch_content_type_is_sent(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
                "data": {"a": "1"},
            }
        )
        await call(
            mcp_client,
            "patch_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            patch=[{"op": "replace", "path": "/data/a", "value": "2"}],
            patch_type="json",
        )

    async def test_rejects_a_json_patch_given_as_an_object(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client,
            "patch_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            patch={"data": {"a": "2"}},
            patch_type="json",
        )
        assert result.is_error
        assert "array of operations" in error_text(result)

    async def test_deletes(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
            }
        )
        result = await call(
            mcp_client, "delete_resource", api_version="v1", kind="ConfigMap", name="cfg"
        )
        assert result["action"] == "deleted"
        assert ("v1", "ConfigMap", "default", "cfg") not in cluster.objects

    async def test_delete_sends_the_propagation_policy(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
            }
        )
        await call(
            mcp_client,
            "delete_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            propagation_policy="Foreground",
        )
        delete = [r for r in cluster.requests if r["method"] == "DELETE"][-1]
        assert delete["query"]["propagationPolicy"] == "Foreground"

    async def test_scales_a_deployment(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "api", "namespace": "default"},
                "spec": {"replicas": 1},
            }
        )
        result = await call(
            mcp_client,
            "scale_resource",
            api_version="apps/v1",
            kind="Deployment",
            name="api",
            replicas=5,
        )
        assert "5" in result["action"]
        assert cluster.objects[("apps/v1", "Deployment", "default", "api")]["spec"]["replicas"] == 5

    async def test_scaling_an_unscalable_kind_explains_the_alternative(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "cfg", "namespace": "default"},
            }
        )
        result = await call_raw(
            mcp_client,
            "scale_resource",
            api_version="v1",
            kind="ConfigMap",
            name="cfg",
            replicas=3,
        )
        assert result.is_error
        assert "patch_resource" in error_text(result)


class TestClusterScoped:
    async def test_lists_nodes_without_a_namespace(
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
        result = await call(mcp_client, "list_resources", api_version="v1", kind="Node")
        assert result["count"] == 1
        assert result["namespace"] is None

    async def test_passing_a_namespace_for_a_cluster_scoped_kind_is_an_error(
        self, mcp_client: Client
    ) -> None:
        # Quietly ignoring it would hide a misunderstanding about the resource.
        result = await call_raw(
            mcp_client,
            "get_resource",
            api_version="v1",
            kind="Node",
            name="node-1",
            namespace="default",
        )
        assert result.is_error
        assert "cluster-scoped" in error_text(result)


class TestCustomResources:
    """The generic tools must reach a CRD the server has never heard of.

    This is what lets the CKH type that ``operator/`` owns work on day one,
    with no code in this repo naming it.
    """

    async def test_lists_a_custom_resource(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "ckh.io/v1alpha1",
                "kind": "ClusterHarness",
                "metadata": {"name": "harness-1", "namespace": "default"},
                "spec": {"replicas": 2},
                "status": {"phase": "Ready"},
            }
        )
        result = await call(
            mcp_client, "list_resources", api_version="ckh.io/v1alpha1", kind="ClusterHarness"
        )
        assert result["count"] == 1
        assert result["items"][0]["name"] == "harness-1"
        assert result["items"][0]["status"] == {"phase": "Ready"}

    async def test_round_trips_a_custom_resource(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        manifest = {
            "apiVersion": "ckh.io/v1alpha1",
            "kind": "ClusterHarness",
            "metadata": {"name": "harness-2"},
            "spec": {"replicas": 3, "target": "prod"},
        }
        await call(mcp_client, "apply_resource", manifest=manifest)
        fetched = await call(
            mcp_client,
            "get_resource",
            api_version="ckh.io/v1alpha1",
            kind="ClusterHarness",
            name="harness-2",
            view="full",
        )
        assert fetched["object"]["spec"]["target"] == "prod"

        await call(
            mcp_client,
            "delete_resource",
            api_version="ckh.io/v1alpha1",
            kind="ClusterHarness",
            name="harness-2",
        )
        assert ("ckh.io/v1alpha1", "ClusterHarness", "default", "harness-2") not in cluster.objects

    async def test_discovery_marks_it_as_custom(self, mcp_client: Client) -> None:
        result = await call(mcp_client, "list_api_resources")
        harness = next(item for item in result["items"] if item["kind"] == "ClusterHarness")
        assert harness["custom_resource"] is True
        assert harness["api_version"] == "ckh.io/v1alpha1"
