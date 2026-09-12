"""Diagnostic tools: logs, events, describe, rollout restart.

``describe_resource`` is the one to reach for first when something is wrong. A
person diagnosing a workload reads the object, then its events, then its
container states; doing all three in one call saves two round trips and, more
importantly, keeps the three pieces next to each other where the relationship
between them is visible.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Any

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations
from pydantic import Field

from kubemcp.context import AppContext
from kubemcp.k8s.client import to_dict
from kubemcp.k8s.logs import get_pod_logs as fetch_logs
from kubemcp.models import (
    ApiVersion,
    ContainerName,
    DescribeResult,
    EventItem,
    EventListResult,
    Kind,
    LogResult,
    MutationResult,
    Namespace,
    OptionalNamespace,
    ResourceName,
)
from kubemcp.shaping import project
from kubemcp.tools._common import kube_of, resolve_namespace, settings_of
from kubemcp.tools.resources import READ_ONLY


def _event_item(raw: dict[str, Any]) -> EventItem:
    """Project one event into the result model.

    Only the model's own fields are taken: the projection layer is free to add
    keys (it stamps ``kind`` on everything), and the result models forbid extras,
    so filtering here keeps the two from having to move in lockstep.
    """
    projected = project(raw, kind="Event")
    return EventItem(**{k: v for k, v in projected.items() if k in EventItem.model_fields})


def _event_sort_key(event: dict[str, Any]) -> str:
    return str(
        event.get("lastTimestamp")
        or event.get("eventTime")
        or (event.get("metadata") or {}).get("creationTimestamp")
        or ""
    )


async def _events_for(
    kube: Any,
    *,
    namespace: str | None,
    involved_name: str | None = None,
    involved_kind: str | None = None,
    limit: int = 25,
) -> list[EventItem]:
    """Fetch events, most recent first."""
    resource = await kube.resource_for("v1", "Event")
    query: dict[str, Any] = {"limit": max(limit * 4, 50)}
    if namespace:
        query["namespace"] = namespace

    selectors = []
    if involved_name:
        selectors.append(f"involvedObject.name={involved_name}")
    if involved_kind:
        selectors.append(f"involvedObject.kind={involved_kind}")
    if selectors:
        query["field_selector"] = ",".join(selectors)

    scope = f"namespace {namespace}" if namespace else "all namespaces"
    raw = to_dict(await kube.get(resource, action=f"list events in {scope}", **query))
    items = raw.get("items") or []
    items.sort(key=_event_sort_key, reverse=True)
    return [_event_item(item) for item in items[:limit]]


def _pod_hints(summary: dict[str, Any]) -> list[str]:
    """Turn a pod's state into plain-language observations."""
    hints: list[str] = []
    for container in summary.get("containers") or []:
        name = container.get("name")
        reason = container.get("reason")
        restarts = container.get("restarts") or 0
        if reason in {"CrashLoopBackOff", "Error"}:
            hints.append(
                f"Container {name!r} is crash-looping ({restarts} restarts). The reason it "
                "exited is in the *previous* container's logs: call get_pod_logs with "
                "previous=true."
            )
        elif reason in {"ImagePullBackOff", "ErrImagePull"}:
            hints.append(
                f"Container {name!r} cannot pull its image ({reason}). Check the image name "
                "and tag, and whether a pull secret is needed."
            )
        elif reason == "CreateContainerConfigError":
            hints.append(
                f"Container {name!r} cannot start because its configuration is invalid -- "
                "usually a missing ConfigMap or Secret referenced by env or volumes."
            )
        last = container.get("last_termination") or {}
        if last.get("reason") == "OOMKilled":
            hints.append(
                f"Container {name!r} was OOMKilled: it exceeded its memory limit. Raise "
                "resources.limits.memory or reduce the workload's usage."
            )
        elif last.get("reason") and restarts:
            hints.append(
                f"Container {name!r} last terminated with reason {last['reason']!r} "
                f"(exit code {last.get('exitCode')})."
            )

    phase = summary.get("phase")
    if phase == "Pending" and not summary.get("node"):
        hints.append(
            "The pod is Pending and unscheduled. The events below usually name the reason "
            "(insufficient resources, unsatisfied node selector, or a taint)."
        )
    if summary.get("deleting_since"):
        hints.append(
            "The pod is terminating. If it stays this way, a finalizer or a stuck "
            "preStop hook is usually holding it."
        )
    return hints


def _workload_hints(summary: dict[str, Any], kind: str) -> list[str]:
    hints: list[str] = []
    replicas = summary.get("replicas") or {}
    desired, ready = replicas.get("desired"), replicas.get("ready")
    if isinstance(desired, int) and isinstance(ready, int) and ready < desired:
        hints.append(
            f"{kind} has {ready} of {desired} replicas ready. List its Pods "
            "(list_resources kind='Pod' with the matching label selector) to see which "
            "are unhealthy and why."
        )
    for condition in summary.get("conditions") or []:
        if condition.get("status") == "False" and condition.get("reason"):
            hints.append(
                f"Condition {condition.get('type')} is False: {condition.get('reason')}"
                + (f" -- {condition['message']}" if condition.get("message") else "")
            )
    return hints


def register(mcp: MCPServer) -> None:
    """Register the diagnostic tools."""

    @mcp.tool(
        title="Get pod logs",
        annotations=READ_ONLY,
        description=(
            "Read a container's logs. For a pod that is crash-looping, pass previous=true: "
            "the current container has not produced the failure yet, and the previous "
            "terminated one is what explains it. Output is capped and keeps the tail."
        ),
    )
    async def get_pod_logs(
        ctx: Context[AppContext],
        name: ResourceName,
        namespace: Namespace = "default",
        container: ContainerName = None,
        previous: Annotated[
            bool,
            Field(
                description=(
                    "Read the previous terminated container instead of the running one. "
                    "This is how you see why a CrashLoopBackOff pod died."
                )
            ),
        ] = False,
        tail_lines: Annotated[
            int | None,
            Field(ge=1, le=10_000, description="Return only the last N lines."),
        ] = 200,
        since_seconds: Annotated[
            int | None,
            Field(ge=1, le=2_592_000, description="Only logs newer than this many seconds."),
        ] = None,
        timestamps: Annotated[
            bool, Field(description="Prefix every line with its RFC 3339 timestamp.")
        ] = False,
    ) -> LogResult:
        kube = kube_of(ctx)
        text, truncated = await fetch_logs(
            kube,
            namespace=namespace,
            pod=name,
            container=container,
            previous=previous,
            tail_lines=tail_lines,
            since_seconds=since_seconds,
            timestamps=timestamps,
            max_bytes=settings_of(ctx).max_log_bytes,
        )
        return LogResult(
            pod=name,
            namespace=namespace,
            container=container,
            previous=previous,
            lines=text.count("\n") + (1 if text and not text.endswith("\n") else 0),
            truncated=truncated,
            logs=text,
        )

    @mcp.tool(
        title="List events",
        annotations=READ_ONLY,
        description=(
            "List recent cluster events, newest first. Events are where the scheduler, "
            "kubelet and controllers explain themselves, so this is usually the fastest "
            "route to why something did not happen."
        ),
    )
    async def list_events(
        ctx: Context[AppContext],
        namespace: OptionalNamespace = None,
        all_namespaces: Annotated[bool, Field(description="Search every namespace.")] = False,
        involved_name: Annotated[
            str | None,
            Field(description="Only events about the object with this name."),
        ] = None,
        involved_kind: Annotated[
            str | None,
            Field(description="Only events about objects of this kind, e.g. 'Pod'."),
        ] = None,
        type_filter: Annotated[
            str | None,
            Field(description="Only events of this type: 'Warning' or 'Normal'."),
        ] = None,
        limit: Annotated[int, Field(ge=1, le=500, description="Maximum events.")] = 30,
    ) -> EventListResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        target_ns = None if all_namespaces else (namespace or settings.default_namespace)

        events = await _events_for(
            kube,
            namespace=target_ns,
            involved_name=involved_name,
            involved_kind=involved_kind,
            limit=limit,
        )
        if type_filter:
            events = [event for event in events if event.type == type_filter]
        return EventListResult(
            namespace=target_ns,
            count=len(events),
            items=events,
            truncated=len(events) >= limit,
        )

    @mcp.tool(
        title="Describe resource",
        annotations=READ_ONLY,
        description=(
            "The diagnostic entry point: returns an object's state, its recent events and "
            "plain-language observations about what looks wrong, in one call. Start here "
            "when something is broken and you do not yet know why."
        ),
    )
    async def describe_resource(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        name: ResourceName,
        namespace: OptionalNamespace = None,
        event_limit: Annotated[
            int, Field(ge=0, le=100, description="How many recent events to include.")
        ] = 15,
    ) -> DescribeResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(resource, namespace, default=settings.default_namespace)

        obj = to_dict(
            await kube.get(
                resource,
                action=f"describe {kind} {target_ns or ''}/{name}".replace("//", "/"),
                name=name,
                namespace=target_ns,
            )
        )
        summary = project(obj, kind=kind)

        events: list[EventItem] = []
        if event_limit:
            # Events are supporting context: if they cannot be read (no RBAC for
            # them, say), the object itself is still worth returning.
            try:
                events = await _events_for(
                    kube,
                    namespace=target_ns,
                    involved_name=name,
                    involved_kind=kind,
                    limit=event_limit,
                )
            except Exception:
                events = []

        if kind == "Pod":
            hints = _pod_hints(summary)
        elif kind in {"Deployment", "StatefulSet", "ReplicaSet", "DaemonSet"}:
            hints = _workload_hints(summary, kind)
        else:
            hints = _workload_hints(summary, kind)

        warnings = [event for event in events if event.type == "Warning"]
        if warnings and not hints:
            hints.append(
                f"There are {len(warnings)} recent Warning events; the newest is "
                f"{warnings[0].reason!r}: {warnings[0].message}"
            )

        return DescribeResult(
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=target_ns,
            summary=summary,
            events=events,
            hints=hints,
        )

    @mcp.tool(
        title="Restart workload",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=False
        ),
        description=(
            "Trigger a rolling restart of a Deployment, StatefulSet or DaemonSet, the "
            "same way 'kubectl rollout restart' does: it stamps the pod template with a "
            "restart timestamp so the controller replaces pods under its normal rolling "
            "update strategy. Existing pods are replaced gradually, not killed at once."
        ),
    )
    async def rollout_restart(
        ctx: Context[AppContext],
        kind: Annotated[
            str,
            Field(description="Deployment, StatefulSet or DaemonSet."),
        ],
        name: ResourceName,
        namespace: Namespace = "default",
        api_version: ApiVersion = "apps/v1",
    ) -> MutationResult:
        kube = kube_of(ctx)
        if kind not in {"Deployment", "StatefulSet", "DaemonSet"}:
            from kubemcp.k8s.errors import ToolError

            raise ToolError(
                f"rollout_restart supports Deployment, StatefulSet and DaemonSet, not {kind!r}. "
                "For anything else, delete its pods or patch the template yourself."
            )

        resource = await kube.resource_for(api_version, kind)
        stamp = dt.datetime.now(dt.UTC).isoformat()
        patch = {
            "spec": {
                "template": {
                    "metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": stamp}}
                }
            }
        }
        patched = await kube.patch(
            resource,
            action=f"restart {kind} {namespace}/{name}",
            body=patch,
            name=name,
            namespace=namespace,
            content_type="application/strategic-merge-patch+json",
        )
        obj = to_dict(patched)
        return MutationResult(
            action=f"restart triggered at {stamp}",
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=namespace,
            resource_version=(obj.get("metadata") or {}).get("resourceVersion"),
            object=project(obj, kind=kind),
        )
