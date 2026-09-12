"""Response shaping.

The consumer of this server is a language model, so response *size* is a
correctness concern rather than a performance one. A raw Pod is routinely 10-15k
tokens, most of it ``managedFields`` and a ``last-applied-configuration``
annotation that restates the whole spec; a handful of unshaped list calls will
bury the caller's context and degrade its reasoning long before any limit is hit.

Three things happen here:

* **Pruning** removes server bookkeeping that no caller needs.
* **Projection** ("summary" view) reduces an object to the fields someone would
  actually look at when diagnosing it. "full" returns the pruned whole object.
* **Budgeting** caps every result and marks what it dropped. The marker matters:
  a silently clipped object is one a model will reason from as though complete,
  whereas an explicit "... truncated" tells it to narrow the query.
"""

from __future__ import annotations

import json
from typing import Any

__all__ = [
    "TRUNCATION_NOTE",
    "fit_json",
    "project",
    "prune",
    "shape",
    "truncate_text",
]

TRUNCATION_NOTE = "__truncated__"

_STRIPPED_ANNOTATIONS = frozenset(
    {
        "kubectl.kubernetes.io/last-applied-configuration",
        "kubectl.kubernetes.io/restartedAt",
    }
)


def prune(obj: Any, *, keep_managed_fields: bool = False) -> Any:
    """Strip apiserver bookkeeping from an object (recursively through lists)."""
    if isinstance(obj, list):
        return [prune(item, keep_managed_fields=keep_managed_fields) for item in obj]
    if not isinstance(obj, dict):
        return obj

    result = dict(obj)
    metadata = result.get("metadata")
    if isinstance(metadata, dict):
        metadata = dict(metadata)
        if not keep_managed_fields:
            metadata.pop("managedFields", None)
        annotations = metadata.get("annotations")
        if isinstance(annotations, dict):
            kept = {
                key: value for key, value in annotations.items() if key not in _STRIPPED_ANNOTATIONS
            }
            if kept:
                metadata["annotations"] = kept
            else:
                metadata.pop("annotations", None)
        result["metadata"] = metadata

    items = result.get("items")
    if isinstance(items, list):
        result["items"] = [prune(item, keep_managed_fields=keep_managed_fields) for item in items]
    return result


def _meta(obj: dict[str, Any]) -> dict[str, Any]:
    metadata = obj.get("metadata") or {}
    summary: dict[str, Any] = {"name": metadata.get("name")}
    if metadata.get("namespace"):
        summary["namespace"] = metadata["namespace"]
    if metadata.get("creationTimestamp"):
        summary["created"] = metadata["creationTimestamp"]
    if metadata.get("deletionTimestamp"):
        # A caller chasing a "stuck" object needs to see it is already terminating.
        summary["deleting_since"] = metadata["deletionTimestamp"]
    return summary


def _conditions(status: dict[str, Any]) -> list[dict[str, Any]]:
    """Keep only conditions that are not in their healthy state."""
    raw = status.get("conditions")
    if not isinstance(raw, list):
        return []
    interesting: list[dict[str, Any]] = []
    for condition in raw:
        if not isinstance(condition, dict):
            continue
        if condition.get("status") == "True" and condition.get("type") in {
            "Ready",
            "Available",
            "Initialized",
            "PodScheduled",
            "ContainersReady",
        }:
            continue
        entry = {
            "type": condition.get("type"),
            "status": condition.get("status"),
        }
        for key in ("reason", "message"):
            if condition.get(key):
                entry[key] = condition[key]
        interesting.append(entry)
    return interesting


def _container_states(status: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten container statuses, keeping what explains a failure."""
    out: list[dict[str, Any]] = []
    for key in ("initContainerStatuses", "containerStatuses"):
        for container in status.get(key) or []:
            if not isinstance(container, dict):
                continue
            entry: dict[str, Any] = {
                "name": container.get("name"),
                "ready": container.get("ready"),
                "restarts": container.get("restartCount"),
            }
            if key == "initContainerStatuses":
                entry["init"] = True
            state = container.get("state") or {}
            for state_name in ("waiting", "terminated", "running"):
                detail = state.get(state_name)
                if not isinstance(detail, dict):
                    continue
                entry["state"] = state_name
                # The reason is the single most useful field for diagnosis
                # (CrashLoopBackOff, ImagePullBackOff, OOMKilled).
                for field in ("reason", "message", "exitCode", "startedAt", "finishedAt"):
                    if detail.get(field) is not None:
                        entry[field] = detail[field]
                break
            last_state = (container.get("lastState") or {}).get("terminated")
            if isinstance(last_state, dict) and last_state.get("reason"):
                entry["last_termination"] = {
                    "reason": last_state.get("reason"),
                    "exitCode": last_state.get("exitCode"),
                    "finishedAt": last_state.get("finishedAt"),
                }
            out.append(entry)
    return out


def _summarize_pod(obj: dict[str, Any]) -> dict[str, Any]:
    status = obj.get("status") or {}
    spec = obj.get("spec") or {}
    containers = _container_states(status)
    ready = sum(1 for c in containers if c.get("ready") and not c.get("init"))
    total = sum(1 for c in containers if not c.get("init"))
    summary = _meta(obj) | {
        "phase": status.get("phase"),
        "ready": f"{ready}/{total}" if total else None,
        "restarts": sum(c.get("restarts") or 0 for c in containers),
        "node": spec.get("nodeName"),
        "pod_ip": status.get("podIP"),
    }
    if status.get("reason"):
        summary["reason"] = status["reason"]
    if containers:
        summary["containers"] = containers
    if conditions := _conditions(status):
        summary["conditions"] = conditions
    return summary


def _summarize_workload(obj: dict[str, Any]) -> dict[str, Any]:
    spec = obj.get("spec") or {}
    status = obj.get("status") or {}
    summary = _meta(obj) | {
        "replicas": {
            "desired": spec.get("replicas"),
            "ready": status.get("readyReplicas", 0),
            "updated": status.get("updatedReplicas", 0),
            "available": status.get("availableReplicas", 0),
        },
        "images": [
            c.get("image")
            for c in ((spec.get("template") or {}).get("spec") or {}).get("containers", [])
            if isinstance(c, dict)
        ],
    }
    if conditions := _conditions(status):
        summary["conditions"] = conditions
    return summary


def _summarize_daemonset(obj: dict[str, Any]) -> dict[str, Any]:
    status = obj.get("status") or {}
    return _meta(obj) | {
        "desired": status.get("desiredNumberScheduled"),
        "ready": status.get("numberReady"),
        "available": status.get("numberAvailable"),
        "misscheduled": status.get("numberMisscheduled"),
        "conditions": _conditions(status),
    }


def _summarize_node(obj: dict[str, Any]) -> dict[str, Any]:
    status = obj.get("status") or {}
    spec = obj.get("spec") or {}
    info = status.get("nodeInfo") or {}
    ready = next(
        (
            c.get("status")
            for c in status.get("conditions") or []
            if isinstance(c, dict) and c.get("type") == "Ready"
        ),
        None,
    )
    summary = _meta(obj) | {
        "ready": ready,
        "kubelet": info.get("kubeletVersion"),
        "os": info.get("osImage"),
        "capacity": status.get("capacity"),
        "allocatable": status.get("allocatable"),
    }
    if spec.get("unschedulable"):
        summary["unschedulable"] = True
    if taints := spec.get("taints"):
        summary["taints"] = taints
    if conditions := _conditions(status):
        summary["conditions"] = conditions
    return summary


def _summarize_service(obj: dict[str, Any]) -> dict[str, Any]:
    spec = obj.get("spec") or {}
    return _meta(obj) | {
        "type": spec.get("type"),
        "cluster_ip": spec.get("clusterIP"),
        "ports": spec.get("ports"),
        "selector": spec.get("selector"),
    }


def _summarize_job(obj: dict[str, Any]) -> dict[str, Any]:
    status = obj.get("status") or {}
    return _meta(obj) | {
        "active": status.get("active", 0),
        "succeeded": status.get("succeeded", 0),
        "failed": status.get("failed", 0),
        "start_time": status.get("startTime"),
        "completion_time": status.get("completionTime"),
        "conditions": _conditions(status),
    }


def _summarize_event(obj: dict[str, Any]) -> dict[str, Any]:
    involved = obj.get("involvedObject") or {}
    return {
        "type": obj.get("type"),
        "reason": obj.get("reason"),
        "message": obj.get("message"),
        "object": f"{involved.get('kind')}/{involved.get('name')}",
        "namespace": involved.get("namespace") or (obj.get("metadata") or {}).get("namespace"),
        "count": obj.get("count"),
        "first_seen": obj.get("firstTimestamp") or obj.get("eventTime"),
        "last_seen": obj.get("lastTimestamp") or obj.get("eventTime"),
    }


def _summarize_generic(obj: dict[str, Any]) -> dict[str, Any]:
    """Fallback projection, used for CRDs among others.

    Custom resources are not known ahead of time, but nearly all of them follow
    the conventions: a ``status.conditions`` array and a handful of top-level
    status fields. Surfacing those makes a CRD useful under the summary view
    without a hand-written projection.
    """
    summary = _meta(obj)
    status = obj.get("status")
    if isinstance(status, dict):
        if conditions := _conditions(status):
            summary["conditions"] = conditions
        scalars = {
            key: value
            for key, value in status.items()
            if key != "conditions" and isinstance(value, str | int | float | bool)
        }
        if scalars:
            summary["status"] = scalars
    if (spec := obj.get("spec")) and isinstance(spec, dict):
        scalars = {
            key: value for key, value in spec.items() if isinstance(value, str | int | float | bool)
        }
        if scalars:
            summary["spec"] = scalars
    return summary


_PROJECTIONS = {
    "Pod": _summarize_pod,
    "Deployment": _summarize_workload,
    "ReplicaSet": _summarize_workload,
    "StatefulSet": _summarize_workload,
    "DaemonSet": _summarize_daemonset,
    "Node": _summarize_node,
    "Service": _summarize_service,
    "Job": _summarize_job,
    "Event": _summarize_event,
}


def project(obj: dict[str, Any], *, kind: str | None = None) -> dict[str, Any]:
    """Reduce one object to its diagnostically useful fields."""
    resolved = kind or obj.get("kind") or ""
    projector = _PROJECTIONS.get(resolved, _summarize_generic)
    summary = projector(obj)
    summary = {key: value for key, value in summary.items() if value not in (None, [], {})}
    if resolved and "kind" not in summary:
        summary["kind"] = resolved
    return summary


def shape(
    obj: dict[str, Any],
    *,
    view: str = "summary",
    kind: str | None = None,
    keep_managed_fields: bool = False,
) -> dict[str, Any]:
    """Apply the requested view to a single object."""
    pruned = prune(obj, keep_managed_fields=keep_managed_fields)
    if view == "full":
        return dict(pruned)
    return project(pruned, kind=kind)


def truncate_text(text: str, limit: int) -> tuple[str, bool]:
    """Cap ``text`` at ``limit`` bytes, keeping the **tail**.

    Logs and command output are read tail-first: the error that matters is at
    the end, so dropping the head preserves the useful part.
    """
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return text, False
    kept = encoded[-limit:].decode("utf-8", errors="replace")
    dropped = len(encoded) - limit
    note = f"[... truncated {dropped} of {len(encoded)} bytes; showing the last {limit} ...]\n"
    return note + kept, True


def fit_json(items: list[Any], limit: int) -> tuple[list[Any], int]:
    """Return as many leading ``items`` as fit in ``limit`` bytes of JSON.

    Returns the kept items and the number dropped, so the caller can say so
    explicitly rather than returning a quietly short list.
    """
    if not items:
        return [], 0
    kept: list[Any] = []
    used = 2  # the enclosing "[]"
    for item in items:
        encoded = len(json.dumps(item, default=str).encode("utf-8")) + 1
        if used + encoded > limit and kept:
            break
        used += encoded
        kept.append(item)
    return kept, len(items) - len(kept)
