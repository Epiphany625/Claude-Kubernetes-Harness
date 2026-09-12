"""Generic resource tools.

These address objects by ``apiVersion`` + ``kind`` through the dynamic client,
which is what lets one small set of tools cover every resource the cluster
serves -- built-in and custom alike. A CRD registered by an operator becomes
reachable the moment it exists, with no code here naming it.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations
from pydantic import Field

from kubemcp import validation
from kubemcp.context import AppContext
from kubemcp.k8s.client import to_dict
from kubemcp.k8s.errors import ToolError
from kubemcp.models import (
    ApiVersion,
    FieldSelector,
    Kind,
    LabelSelector,
    MutationResult,
    Namespace,
    OptionalNamespace,
    ResourceListResult,
    ResourceName,
    ResourceResult,
    View,
)
from kubemcp.shaping import shape
from kubemcp.tools._common import (
    kube_of,
    object_identity,
    resolve_namespace,
    settings_of,
    shape_items,
)

_PATCH_CONTENT_TYPES = {
    "strategic": "application/strategic-merge-patch+json",
    "merge": "application/merge-patch+json",
    "json": "application/json-patch+json",
}

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)


def register(mcp: MCPServer) -> None:
    """Register the generic resource tools."""

    @mcp.tool(
        title="List resources",
        annotations=READ_ONLY,
        description=(
            "List objects of any kind, including custom resources. Returns a compact "
            "summary of each object by default. Use label_selector or field_selector to "
            "narrow broad queries rather than paging through everything."
        ),
    )
    async def list_resources(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        namespace: OptionalNamespace = None,
        all_namespaces: Annotated[
            bool,
            Field(description="List across every namespace instead of just one."),
        ] = False,
        label_selector: LabelSelector = None,
        field_selector: FieldSelector = None,
        limit: Annotated[int, Field(ge=1, le=5000, description="Maximum objects to return.")] = 50,
        continue_token: Annotated[
            str | None,
            Field(description="Token from a previous page's continue_token."),
        ] = None,
        view: View = "summary",
    ) -> ResourceListResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(
            resource, namespace, default=settings.default_namespace, all_namespaces=all_namespaces
        )

        query: dict[str, Any] = {"limit": min(limit, settings.max_list_items)}
        if target_ns:
            query["namespace"] = target_ns
        if label_selector:
            query["label_selector"] = label_selector
        if field_selector:
            query["field_selector"] = field_selector
        if continue_token:
            query["_continue"] = continue_token

        scope = f"namespace {target_ns}" if target_ns else "all namespaces"
        result = to_dict(await kube.get(resource, action=f"list {kind} in {scope}", **query))

        items = result.get("items") or []
        shaped, dropped, truncated = shape_items(
            items,
            view=view,
            kind=kind,
            budget=settings.max_response_bytes,
            limit=min(limit, settings.max_list_items),
        )
        return ResourceListResult(
            api_version=api_version,
            kind=kind,
            namespace=target_ns,
            count=len(shaped),
            items=shaped,
            continue_token=(result.get("metadata") or {}).get("continue") or None,
            truncated=truncated,
            dropped=dropped,
        )

    @mcp.tool(
        title="Get resource",
        annotations=READ_ONLY,
        description=(
            "Fetch one object by name. Use view='full' when you need a field the "
            "summary omits; prefer describe_resource when diagnosing a problem, since "
            "it also returns the object's events."
        ),
    )
    async def get_resource(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        name: ResourceName,
        namespace: OptionalNamespace = None,
        view: View = "summary",
    ) -> ResourceResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(resource, namespace, default=settings.default_namespace)

        obj = to_dict(
            await kube.get(
                resource,
                action=f"get {kind} {target_ns or ''}/{name}".replace("//", "/"),
                name=name,
                namespace=target_ns,
            )
        )
        identity = object_identity(obj, fallback_kind=kind, fallback_version=api_version)
        return ResourceResult(
            **identity,
            object=shape(obj, view=view, kind=kind),
        )

    @mcp.tool(
        title="Create resource",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=False
        ),
        description=(
            "Create an object from a manifest. Fails if it already exists -- use "
            "apply_resource to create-or-update instead."
        ),
    )
    async def create_resource(
        ctx: Context[AppContext],
        manifest: Annotated[
            dict[str, Any],
            Field(
                description=(
                    "Full resource manifest, as you would write in YAML but as JSON. "
                    "Must include apiVersion and kind."
                )
            ),
        ],
        namespace: OptionalNamespace = None,
        dry_run: Annotated[
            bool,
            Field(description="Validate against the apiserver without persisting anything."),
        ] = False,
    ) -> MutationResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        body = validation.validate_manifest(manifest, namespace=namespace, require_name=False)
        api_version, kind = body["apiVersion"], body["kind"]
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(
            resource,
            namespace or (body.get("metadata") or {}).get("namespace"),
            default=settings.default_namespace,
        )

        extra: dict[str, Any] = {"dry_run": "All"} if dry_run else {}
        created = await kube.create(
            resource,
            action=f"create {kind} in {target_ns or 'cluster scope'}",
            body=body,
            namespace=target_ns,
            **extra,
        )
        obj = to_dict(created)
        return MutationResult(
            action="created",
            **object_identity(obj, fallback_kind=kind, fallback_version=api_version),
            dry_run=dry_run,
            object=shape(obj, view="summary", kind=kind),
        )

    @mcp.tool(
        title="Apply resource",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=True
        ),
        description=(
            "Create or update an object from a manifest using server-side apply. This is "
            "the idempotent way to reach a desired state: applying the same manifest "
            "twice is a no-op. If it reports a field conflict, another controller owns "
            "those fields; re-run with force_conflicts=true to take ownership."
        ),
    )
    async def apply_resource(
        ctx: Context[AppContext],
        manifest: Annotated[
            dict[str, Any],
            Field(description="Full resource manifest as JSON, including apiVersion and kind."),
        ],
        namespace: OptionalNamespace = None,
        force_conflicts: Annotated[
            bool,
            Field(description="Take ownership of fields currently managed by someone else."),
        ] = False,
        dry_run: Annotated[bool, Field(description="Validate without persisting.")] = False,
    ) -> MutationResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        body = validation.validate_manifest(manifest, namespace=namespace, require_name=True)
        api_version, kind = body["apiVersion"], body["kind"]
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(
            resource,
            namespace or (body.get("metadata") or {}).get("namespace"),
            default=settings.default_namespace,
        )
        name = (body.get("metadata") or {})["name"]

        extra: dict[str, Any] = {"dry_run": "All"} if dry_run else {}
        applied = await kube.apply(
            resource,
            action=f"apply {kind} {target_ns or ''}/{name}".replace("//", "/"),
            body=body,
            name=name,
            namespace=target_ns,
            force_conflicts=force_conflicts or None,
            field_manager=settings.field_manager,
            **extra,
        )
        obj = to_dict(applied)
        return MutationResult(
            action="configured",
            **object_identity(obj, fallback_kind=kind, fallback_version=api_version),
            dry_run=dry_run,
            object=shape(obj, view="summary", kind=kind),
        )

    @mcp.tool(
        title="Patch resource",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=False
        ),
        description=(
            "Patch specific fields of an existing object. patch_type 'strategic' (the "
            "default) merges lists intelligently for built-in kinds; use 'merge' for "
            "custom resources, which do not support strategic merge; use 'json' for an "
            "RFC 6902 operation array."
        ),
    )
    async def patch_resource(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        name: ResourceName,
        patch: Annotated[
            dict[str, Any] | list[dict[str, Any]],
            Field(
                description=(
                    "A partial object for 'strategic'/'merge', or an array of RFC 6902 "
                    "operations for 'json'."
                )
            ),
        ],
        namespace: OptionalNamespace = None,
        patch_type: Annotated[
            Literal["strategic", "merge", "json"],
            Field(description="Patch semantics to use."),
        ] = "strategic",
        dry_run: Annotated[bool, Field(description="Validate without persisting.")] = False,
    ) -> MutationResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        if patch_type == "json":
            body: Any = validation.validate_json_patch(patch)
        else:
            body = validation.validate_merge_patch(patch)

        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(resource, namespace, default=settings.default_namespace)

        extra: dict[str, Any] = {"dry_run": "All"} if dry_run else {}
        patched = await kube.patch(
            resource,
            action=f"patch {kind} {target_ns or ''}/{name}".replace("//", "/"),
            body=body,
            name=name,
            namespace=target_ns,
            content_type=_PATCH_CONTENT_TYPES[patch_type],
            **extra,
        )
        obj = to_dict(patched)
        return MutationResult(
            action="patched",
            **object_identity(obj, fallback_kind=kind, fallback_version=api_version),
            dry_run=dry_run,
            object=shape(obj, view="summary", kind=kind),
        )

    @mcp.tool(
        title="Delete resource",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=True, idempotent_hint=True
        ),
        description=(
            "Delete an object. Deletion is asynchronous: the call returns once the "
            "apiserver accepts it, and the object may linger while finalizers run. "
            "propagation_policy controls dependents -- 'Background' (default) deletes "
            "them after the owner, 'Foreground' before it, 'Orphan' leaves them."
        ),
    )
    async def delete_resource(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        name: ResourceName,
        namespace: OptionalNamespace = None,
        propagation_policy: Annotated[
            Literal["Background", "Foreground", "Orphan"],
            Field(description="How to treat dependent objects."),
        ] = "Background",
        grace_period_seconds: Annotated[
            int | None,
            Field(ge=0, le=86_400, description="Override the termination grace period."),
        ] = None,
        dry_run: Annotated[bool, Field(description="Validate without deleting.")] = False,
    ) -> MutationResult:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        resource = await kube.resource_for(api_version, kind)
        target_ns = resolve_namespace(resource, namespace, default=settings.default_namespace)

        extra: dict[str, Any] = {"dry_run": "All"} if dry_run else {}
        if grace_period_seconds is not None:
            extra["grace_period_seconds"] = grace_period_seconds

        await kube.delete(
            resource,
            action=f"delete {kind} {target_ns or ''}/{name}".replace("//", "/"),
            name=name,
            namespace=target_ns,
            propagation_policy=propagation_policy,
            **extra,
        )
        return MutationResult(
            action="deleted",
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=target_ns,
            dry_run=dry_run,
        )

    @mcp.tool(
        title="Scale workload",
        annotations=ToolAnnotations(
            read_only_hint=False, destructive_hint=False, idempotent_hint=True
        ),
        description=(
            "Set the replica count on a Deployment, StatefulSet or ReplicaSet via its "
            "scale subresource."
        ),
    )
    async def scale_resource(
        ctx: Context[AppContext],
        api_version: ApiVersion,
        kind: Kind,
        name: ResourceName,
        replicas: Annotated[int, Field(ge=0, le=10_000, description="Desired replica count.")],
        namespace: Namespace = "default",
    ) -> MutationResult:
        kube = kube_of(ctx)
        resource = await kube.resource_for(api_version, kind)
        if "scale" not in (getattr(resource, "subresources", None) or {}):
            raise ToolError(
                f"{kind} ({api_version}) has no 'scale' subresource on this cluster, so it "
                "cannot be scaled. Deployments, StatefulSets and ReplicaSets can; for "
                "anything else set the replica field with patch_resource."
            )

        path = f"{resource.path(name=name, namespace=namespace)}/scale"
        await kube.request(
            "patch",
            path,
            action=f"scale {kind} {namespace}/{name} to {replicas}",
            body={"spec": {"replicas": replicas}},
            content_type="application/merge-patch+json",
        )
        return MutationResult(
            action=f"scaled to {replicas}",
            api_version=api_version,
            kind=kind,
            name=name,
            namespace=namespace,
        )
