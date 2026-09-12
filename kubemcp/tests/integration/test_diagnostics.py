"""Logs, events, describe and rollout restart, over the real MCP protocol."""

from __future__ import annotations

from typing import Any

import pytest
from mcp import Client

from tests.conftest import call, call_raw, error_text
from tests.fake_apiserver import FakeCluster

pytestmark = pytest.mark.anyio


def event(
    name: str,
    *,
    reason: str,
    message: str,
    kind: str = "Pod",
    involved: str = "web-0",
    event_type: str = "Warning",
    last: str = "2026-01-01T00:00:00Z",
    namespace: str = "default",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": name, "namespace": namespace},
        "involvedObject": {"kind": kind, "name": involved, "namespace": namespace},
        "reason": reason,
        "message": message,
        "type": event_type,
        "count": 1,
        "firstTimestamp": last,
        "lastTimestamp": last,
    }


class TestLogs:
    async def test_returns_logs(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        cluster.add_log("default", "web-0", "listening on :8080\nready\n")

        result = await call(mcp_client, "get_pod_logs", name="web-0")
        assert "listening on :8080" in result["logs"]
        assert result["lines"] == 2
        assert result["previous"] is False

    async def test_previous_reads_the_terminated_container(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # The whole point of previous=true: the *current* container has not
        # failed yet, so only the previous one explains a CrashLoopBackOff.
        cluster.add(pod_factory("web-0", waiting_reason="CrashLoopBackOff", ready=False))
        cluster.add_log("default", "web-0", "starting\n", previous=False)
        cluster.add_log("default", "web-0", "FATAL: cannot bind port 80\n", previous=True)

        current = await call(mcp_client, "get_pod_logs", name="web-0")
        assert "FATAL" not in current["logs"]

        previous = await call(mcp_client, "get_pod_logs", name="web-0", previous=True)
        assert "FATAL: cannot bind port 80" in previous["logs"]
        assert previous["previous"] is True

    async def test_passes_options_to_the_apiserver(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        cluster.add_log("default", "web-0", "x\n", container="sidecar")

        await call(
            mcp_client,
            "get_pod_logs",
            name="web-0",
            container="sidecar",
            tail_lines=50,
            since_seconds=300,
            timestamps=True,
        )
        request = [r for r in cluster.requests if r["path"].endswith("/log")][-1]
        assert request["query"] == {
            "container": "sidecar",
            "tailLines": "50",
            "sinceSeconds": "300",
            "timestamps": "true",
        }

    async def test_truncates_long_logs_and_keeps_the_tail(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any, settings: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        body = "".join(f"line {i}\n" for i in range(20_000))
        cluster.add_log("default", "web-0", body + "THE LAST LINE\n")

        result = await call(mcp_client, "get_pod_logs", name="web-0")
        assert result["truncated"] is True
        assert "THE LAST LINE" in result["logs"]
        assert len(result["logs"].encode()) <= settings.max_log_bytes + 200

    async def test_rejects_an_invalid_container_name(self, mcp_client: Client) -> None:
        result = await call_raw(mcp_client, "get_pod_logs", name="web-0", container="Bad_Container")
        assert result.is_error


class TestEvents:
    async def test_lists_newest_first(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(event("e1", reason="Old", message="older", last="2026-01-01T00:00:00Z"))
        cluster.add(event("e2", reason="New", message="newer", last="2026-01-02T00:00:00Z"))

        result = await call(mcp_client, "list_events")
        assert [item["reason"] for item in result["items"]] == ["New", "Old"]

    async def test_flattens_the_involved_object(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(event("e1", reason="Failed", message="boom"))
        result = await call(mcp_client, "list_events")
        assert result["items"][0]["object"] == "Pod/web-0"

    async def test_filters_by_type(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(event("e1", reason="Pulled", message="ok", event_type="Normal"))
        cluster.add(event("e2", reason="Failed", message="bad", event_type="Warning"))

        result = await call(mcp_client, "list_events", type_filter="Warning")
        assert [item["reason"] for item in result["items"]] == ["Failed"]

    async def test_filters_by_involved_object(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(event("e1", reason="A", message="m", involved="web-0"))
        cluster.add(event("e2", reason="B", message="m", involved="web-1"))

        result = await call(mcp_client, "list_events", involved_name="web-0")
        assert [item["reason"] for item in result["items"]] == ["A"]
        request = [r for r in cluster.requests if "events" in r["path"]][-1]
        assert "involvedObject.name=web-0" in request["query"]["fieldSelector"]


class TestDescribe:
    async def test_combines_object_events_and_hints(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(
            pod_factory("web-0", ready=False, restarts=5, waiting_reason="CrashLoopBackOff")
        )
        cluster.add(event("e1", reason="BackOff", message="Back-off restarting failed container"))

        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        assert result["summary"]["restarts"] == 5
        assert result["events"][0]["reason"] == "BackOff"
        assert result["hints"], "a crash-looping pod should produce a hint"

    async def test_crashloop_hint_points_at_previous_logs(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # The hint has to name the *next* action, not merely restate the state.
        cluster.add(
            pod_factory("web-0", ready=False, restarts=9, waiting_reason="CrashLoopBackOff")
        )
        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        hints = " ".join(result["hints"])
        assert "previous=true" in hints
        assert "get_pod_logs" in hints

    async def test_image_pull_failure_is_explained(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0", ready=False, waiting_reason="ImagePullBackOff"))
        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        hints = " ".join(result["hints"])
        assert "pull" in hints.lower()
        assert "pull secret" in hints.lower()

    async def test_oomkill_is_explained(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(
            pod_factory(
                "web-0",
                ready=False,
                restarts=2,
                waiting_reason="CrashLoopBackOff",
                last_termination={"reason": "OOMKilled", "exitCode": 137},
            )
        )
        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        hints = " ".join(result["hints"])
        assert "OOMKilled" in hints
        assert "memory" in hints.lower()

    async def test_unschedulable_pod_is_explained(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0", phase="Pending", node=None, ready=False))
        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        assert any("unscheduled" in hint.lower() for hint in result["hints"])

    async def test_healthy_pod_produces_no_alarming_hints(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        cluster.add(pod_factory("web-0"))
        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        assert result["hints"] == []

    async def test_degrades_when_events_cannot_be_read(
        self, mcp_client: Client, cluster: FakeCluster, pod_factory: Any
    ) -> None:
        # Events are supporting context. Losing them must not cost the object.
        cluster.add(pod_factory("web-0"))
        cluster.fail_forbidden("GET", "events")

        result = await call(
            mcp_client, "describe_resource", api_version="v1", kind="Pod", name="web-0"
        )
        assert result["summary"]["name"] == "web-0"
        assert result["events"] == []

    async def test_describes_a_deployment(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "api", "namespace": "default"},
                "spec": {"replicas": 3, "template": {"spec": {"containers": [{"image": "api:1"}]}}},
                "status": {"readyReplicas": 1},
            }
        )
        result = await call(
            mcp_client,
            "describe_resource",
            api_version="apps/v1",
            kind="Deployment",
            name="api",
        )
        assert "1 of 3 replicas ready" in " ".join(result["hints"])

    async def test_describes_a_custom_resource(
        self, mcp_client: Client, cluster: FakeCluster
    ) -> None:
        cluster.add(
            {
                "apiVersion": "ckh.io/v1alpha1",
                "kind": "ClusterHarness",
                "metadata": {"name": "h1", "namespace": "default"},
                "spec": {"replicas": 1},
                "status": {
                    "conditions": [
                        {
                            "type": "Ready",
                            "status": "False",
                            "reason": "BackendUnavailable",
                            "message": "cannot reach postgres",
                        }
                    ]
                },
            }
        )
        result = await call(
            mcp_client,
            "describe_resource",
            api_version="ckh.io/v1alpha1",
            kind="ClusterHarness",
            name="h1",
        )
        assert "BackendUnavailable" in " ".join(result["hints"])


class TestRolloutRestart:
    async def test_stamps_the_pod_template(self, mcp_client: Client, cluster: FakeCluster) -> None:
        cluster.add(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "api", "namespace": "default"},
                "spec": {"replicas": 1, "template": {"metadata": {}, "spec": {}}},
            }
        )
        result = await call(mcp_client, "rollout_restart", kind="Deployment", name="api")
        assert "restart triggered" in result["action"]

        stored = cluster.objects[("apps/v1", "Deployment", "default", "api")]
        annotations = stored["spec"]["template"]["metadata"]["annotations"]
        assert "kubectl.kubernetes.io/restartedAt" in annotations

    async def test_refuses_kinds_it_cannot_restart(self, mcp_client: Client) -> None:
        result = await call_raw(
            mcp_client, "rollout_restart", kind="ConfigMap", name="cfg", api_version="v1"
        )
        assert result.is_error
        assert "Deployment, StatefulSet and DaemonSet" in error_text(result)
