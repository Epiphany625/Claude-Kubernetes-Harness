"""Enumerating what this cluster actually serves.

This is the tool an agent reaches for first when it does not know a resource's
apiVersion -- including for custom resources, which is how the CKH type that
``operator/`` owns becomes reachable without anything here knowing its name.
"""

from __future__ import annotations

import logging
from typing import Any

from kubemcp.k8s.client import KubeClient

logger = logging.getLogger(__name__)

# Groups whose resources are cluster plumbing rather than workload state. They
# stay available by exact apiVersion, but are hidden from an unfiltered listing
# so the common case is not drowned in them.
NOISY_GROUPS = frozenset(
    {
        "admissionregistration.k8s.io",
        "apiregistration.k8s.io",
        "authentication.k8s.io",
        "authorization.k8s.io",
        "certificates.k8s.io",
        "coordination.k8s.io",
        "flowcontrol.apiserver.k8s.io",
        "internal.apiserver.k8s.io",
    }
)


BUILTIN_GROUP_SUFFIXES = ("k8s.io", "kubernetes.io")


async def _group_versions(kube: KubeClient) -> list[str]:
    """Every groupVersion this cluster serves, core first."""
    versions: list[str] = []

    core = await kube.get_json("/api", action="discover the core API versions")
    versions.extend(core.get("versions") or ["v1"])

    groups = await kube.get_json("/apis", action="discover the API groups")
    for group in groups.get("groups") or []:
        for version in group.get("versions") or []:
            group_version = version.get("groupVersion")
            if group_version:
                versions.append(group_version)
    return versions


async def list_api_resources(
    kube: KubeClient,
    *,
    group: str | None = None,
    namespaced: bool | None = None,
    verb: str | None = None,
    include_infrastructure: bool = False,
) -> list[dict[str, Any]]:
    """List the resource types this cluster serves.

    This walks the discovery endpoints itself rather than iterating the dynamic
    client's discoverer. ``LazyDiscoverer.__aiter__`` iterates a dict of
    resources without calling ``.values()``, so it yields the dict's keys --
    plain strings -- instead of ``Resource`` objects. Reading the discovery
    documents directly is both correct and closer to what ``kubectl
    api-resources`` reports.
    """
    found: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for group_version in await _group_versions(kube):
        resource_group = group_version.rsplit("/", 1)[0] if "/" in group_version else ""

        # Skip whole groups we would filter out anyway, to save a round trip.
        if group is not None and resource_group != group:
            continue
        if not include_infrastructure and group is None and resource_group in NOISY_GROUPS:
            continue

        path = f"/apis/{group_version}" if resource_group else f"/api/{group_version}"
        try:
            document = await kube.get_json(path, action=f"discover resources in {group_version}")
        except Exception:
            # An unavailable aggregated API (a CRD whose backing service is
            # down) should cost that group, not the whole listing.
            logger.warning("discovery failed for %s; skipping", group_version, exc_info=True)
            continue

        for resource in document.get("resources") or []:
            name = resource.get("name") or ""
            # Subresources (pods/log, deployments/scale) are reached through
            # their parent, not listed as standalone types.
            if "/" in name:
                continue

            kind = resource.get("kind") or ""
            key = (group_version, kind)
            if key in seen:
                continue

            verbs = list(resource.get("verbs") or [])
            if namespaced is not None and bool(resource.get("namespaced")) != namespaced:
                continue
            if verb is not None and verb not in verbs:
                continue

            seen.add(key)
            entry: dict[str, Any] = {
                "kind": kind,
                "api_version": group_version,
                "name": name,
                "namespaced": bool(resource.get("namespaced")),
                "verbs": sorted(verbs),
            }
            if short_names := resource.get("shortNames"):
                entry["short_names"] = list(short_names)
            subresources = sorted(
                item["name"].split("/", 1)[1]
                for item in document.get("resources") or []
                if item.get("name", "").startswith(f"{name}/")
            )
            if subresources:
                entry["subresources"] = subresources
            # Anything outside the built-in groups is a custom resource; saying
            # so saves the caller from inferring it from the group name.
            if resource_group and not resource_group.endswith(BUILTIN_GROUP_SUFFIXES):
                entry["custom_resource"] = True
            found.append(entry)

    found.sort(key=lambda item: (item["api_version"], item["kind"]))
    return found


async def cluster_info(kube: KubeClient) -> dict[str, Any]:
    """Server version plus a roll-up of node health."""
    version = await kube.server_version()

    node_resource = await kube.resource_for("v1", "Node")
    nodes = await kube.get(node_resource, action="list nodes")
    node_items = (nodes.to_dict() if hasattr(nodes, "to_dict") else nodes).get("items", [])

    ready = 0
    not_ready: list[str] = []
    for node in node_items:
        conditions = (node.get("status") or {}).get("conditions") or []
        is_ready = any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in conditions
            if isinstance(c, dict)
        )
        if is_ready:
            ready += 1
        else:
            not_ready.append((node.get("metadata") or {}).get("name", "<unknown>"))

    info: dict[str, Any] = {
        "version": version.get("gitVersion"),
        "platform": version.get("platform"),
        "nodes_total": len(node_items),
        "nodes_ready": ready,
    }
    if not_ready:
        info["nodes_not_ready"] = not_ready
    return info
