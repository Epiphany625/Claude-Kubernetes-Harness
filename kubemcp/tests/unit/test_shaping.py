"""Shaping decides what a language model actually sees, so these tests assert on
what survives, what is dropped, and that nothing is dropped silently."""

from __future__ import annotations

import json
from typing import Any

from kubemcp import shaping


def _pod(**status: Any) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "web-0",
            "namespace": "default",
            "managedFields": [{"manager": "kubelet", "fieldsV1": {"f:spec": {}}}],
            "annotations": {
                "kubectl.kubernetes.io/last-applied-configuration": '{"a":"' + "x" * 5000 + '"}',
                "team": "platform",
            },
        },
        "spec": {"nodeName": "node-1"},
        "status": {"phase": "Running", "podIP": "10.0.0.1", **status},
    }


class TestPrune:
    def test_strips_managed_fields(self) -> None:
        assert "managedFields" not in shaping.prune(_pod())["metadata"]

    def test_strips_last_applied_configuration(self) -> None:
        annotations = shaping.prune(_pod())["metadata"]["annotations"]
        assert "kubectl.kubernetes.io/last-applied-configuration" not in annotations

    def test_keeps_meaningful_annotations(self) -> None:
        assert shaping.prune(_pod())["metadata"]["annotations"] == {"team": "platform"}

    def test_drops_the_annotations_key_when_nothing_is_left(self) -> None:
        pod = _pod()
        pod["metadata"]["annotations"].pop("team")
        assert "annotations" not in shaping.prune(pod)["metadata"]

    def test_can_be_asked_to_keep_managed_fields(self) -> None:
        kept = shaping.prune(_pod(), keep_managed_fields=True)
        assert "managedFields" in kept["metadata"]

    def test_does_not_mutate_the_input(self) -> None:
        pod = _pod()
        shaping.prune(pod)
        assert "managedFields" in pod["metadata"]

    def test_prunes_items_of_a_list(self) -> None:
        listing = {"kind": "PodList", "items": [_pod(), _pod()]}
        for item in shaping.prune(listing)["items"]:
            assert "managedFields" not in item["metadata"]

    def test_saves_a_large_fraction_of_the_bytes(self) -> None:
        # The point of pruning: the bookkeeping dwarfs the object.
        pod = _pod()
        before = len(json.dumps(pod))
        after = len(json.dumps(shaping.prune(pod)))
        assert after < before / 4


class TestPodProjection:
    def test_surfaces_the_crash_reason(self) -> None:
        pod = _pod(
            containerStatuses=[
                {
                    "name": "app",
                    "ready": False,
                    "restartCount": 7,
                    "state": {"waiting": {"reason": "CrashLoopBackOff", "message": "back-off"}},
                }
            ]
        )
        summary = shaping.project(shaping.prune(pod))
        container = summary["containers"][0]
        assert container["reason"] == "CrashLoopBackOff"
        assert container["restarts"] == 7
        assert summary["ready"] == "0/1"
        assert summary["restarts"] == 7

    def test_surfaces_the_previous_termination(self) -> None:
        pod = _pod(
            containerStatuses=[
                {
                    "name": "app",
                    "ready": False,
                    "restartCount": 3,
                    "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                    "lastState": {
                        "terminated": {
                            "reason": "OOMKilled",
                            "exitCode": 137,
                            "finishedAt": "2026-01-01T00:00:00Z",
                        }
                    },
                }
            ]
        )
        summary = shaping.project(pod)
        assert summary["containers"][0]["last_termination"]["reason"] == "OOMKilled"

    def test_marks_init_containers(self) -> None:
        pod = _pod(
            initContainerStatuses=[
                {
                    "name": "setup",
                    "ready": False,
                    "restartCount": 0,
                    "state": {"terminated": {"reason": "Error", "exitCode": 1}},
                }
            ],
            containerStatuses=[
                {"name": "app", "ready": True, "restartCount": 0, "state": {"running": {}}}
            ],
        )
        summary = shaping.project(pod)
        init = [c for c in summary["containers"] if c.get("init")]
        assert len(init) == 1
        # Ready counts only the main containers, as kubectl reports them.
        assert summary["ready"] == "1/1"

    def test_keeps_only_unhealthy_conditions(self) -> None:
        pod = _pod(
            conditions=[
                {"type": "Ready", "status": "True"},
                {
                    "type": "PodScheduled",
                    "status": "False",
                    "reason": "Unschedulable",
                    "message": "no nodes available",
                },
            ]
        )
        summary = shaping.project(pod)
        assert [c["type"] for c in summary["conditions"]] == ["PodScheduled"]

    def test_reports_termination(self) -> None:
        pod = _pod()
        pod["metadata"]["deletionTimestamp"] = "2026-01-01T12:00:00Z"
        assert shaping.project(pod)["deleting_since"] == "2026-01-01T12:00:00Z"


class TestGenericProjection:
    def test_handles_an_unknown_custom_resource(self) -> None:
        # No projection is written for a CRD, so the fallback has to produce
        # something useful from the conventions alone.
        obj = {
            "apiVersion": "ckh.io/v1alpha1",
            "kind": "ClusterHarness",
            "metadata": {"name": "h1", "namespace": "ckh"},
            "spec": {"replicas": 3, "target": "prod", "nested": {"ignored": True}},
            "status": {
                "phase": "Degraded",
                "conditions": [{"type": "Ready", "status": "False", "reason": "BackendDown"}],
            },
        }
        summary = shaping.project(obj)
        assert summary["name"] == "h1"
        assert summary["status"] == {"phase": "Degraded"}
        assert summary["spec"] == {"replicas": 3, "target": "prod"}
        assert summary["conditions"][0]["reason"] == "BackendDown"


class TestWorkloadProjection:
    def test_summarises_replica_state(self) -> None:
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "api", "namespace": "default"},
            "spec": {"replicas": 3, "template": {"spec": {"containers": [{"image": "api:1"}]}}},
            "status": {"readyReplicas": 1, "updatedReplicas": 3, "availableReplicas": 1},
        }
        summary = shaping.project(deployment)
        assert summary["replicas"] == {"desired": 3, "ready": 1, "updated": 3, "available": 1}
        assert summary["images"] == ["api:1"]


class TestTruncation:
    def test_keeps_the_tail_because_errors_are_at_the_end(self) -> None:
        text = "".join(f"line {i}\n" for i in range(1000))
        out, truncated = shaping.truncate_text(text, 100)
        assert truncated
        assert out.endswith("line 999\n")
        assert "truncated" in out

    def test_says_how_much_it_dropped(self) -> None:
        out, _ = shaping.truncate_text("x" * 500, 100)
        assert "truncated 400 of 500 bytes" in out

    def test_leaves_short_text_alone(self) -> None:
        out, truncated = shaping.truncate_text("short", 100)
        assert out == "short"
        assert not truncated

    def test_does_not_split_a_multibyte_character(self) -> None:
        out, _ = shaping.truncate_text("é" * 200, 101)
        assert "�" not in out.split("]\n", 1)[-1].lstrip("�")


class TestFitJson:
    def test_returns_everything_that_fits(self) -> None:
        items = [{"n": i} for i in range(5)]
        kept, dropped = shaping.fit_json(items, 10_000)
        assert kept == items
        assert dropped == 0

    def test_drops_the_overflow_and_reports_the_count(self) -> None:
        items = [{"name": "x" * 100} for _ in range(50)]
        kept, dropped = shaping.fit_json(items, 500)
        assert 0 < len(kept) < 50
        assert dropped == 50 - len(kept)

    def test_always_returns_at_least_one_item(self) -> None:
        # A single item larger than the budget is better than an empty list that
        # looks like "nothing matched".
        kept, dropped = shaping.fit_json([{"name": "x" * 5000}], 100)
        assert len(kept) == 1
        assert dropped == 0

    def test_handles_an_empty_list(self) -> None:
        assert shaping.fit_json([], 100) == ([], 0)


class TestShape:
    def test_full_view_returns_the_pruned_object(self) -> None:
        full = shaping.shape(_pod(), view="full")
        assert full["spec"] == {"nodeName": "node-1"}
        assert "managedFields" not in full["metadata"]

    def test_summary_view_is_much_smaller_than_full(self) -> None:
        pod = _pod(
            containerStatuses=[
                {"name": "app", "ready": True, "restartCount": 0, "state": {"running": {}}}
            ]
        )
        summary = len(json.dumps(shaping.shape(pod, view="summary")))
        full = len(json.dumps(shaping.shape(pod, view="full")))
        assert summary < full
