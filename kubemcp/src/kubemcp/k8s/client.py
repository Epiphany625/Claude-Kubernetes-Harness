"""The Kubernetes client this server shares across all tool calls.

One :class:`KubeClient` is built in the server lifespan and reused, so discovery
is paid for once rather than per call. All apiserver traffic passes through a
semaphore, which keeps a burst of concurrent tool calls from turning into a
burst of concurrent apiserver requests.

Everything is addressed by ``apiVersion`` + ``kind`` through the dynamic client
rather than the generated typed APIs. That is what lets the same handful of
tools reach built-in resources and custom resources alike -- including the CKH
CRD that ``operator/`` owns, with no code here needing to know it exists.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, Configuration
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.dynamic import DynamicClient
from kubernetes_asyncio.dynamic.exceptions import (
    ResourceNotFoundError,
    ResourceNotUniqueError,
)
from kubernetes_asyncio.dynamic.resource import Resource

from kubemcp.config import Settings
from kubemcp.k8s.errors import ToolError, guard

logger = logging.getLogger(__name__)

_IN_CLUSTER_TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")


class ClusterUnreachable(RuntimeError):
    """The server cannot reach a cluster, so it has nothing to serve.

    Raised only at startup. The message is written for whoever is reading pod
    logs after a failed rollout.
    """


def raise_for_status(obj: Any) -> Any:
    """Turn an apiserver ``Status`` failure into an :class:`ApiException`.

    ``kubernetes_asyncio``'s dynamic client issues every request with
    ``_preload_content=False``, and the REST layer only checks the HTTP status
    code when that flag is *true* (``client/rest.py``). The upshot is that the
    dynamic client never raises on an error response: it deserializes the
    apiserver's ``Status`` body into a ``ResourceInstance`` and hands it back as
    though it were the object that was asked for.

    Left alone, a 404 would reach the caller as a *successful* tool result whose
    content happens to describe a nonexistent object -- the worst shape of
    failure for a model trying to diagnose a cluster, because nothing signals
    that anything went wrong. So every dynamic-client response passes through
    here, centrally, rather than relying on each tool to remember.

    A successful delete legitimately returns ``kind: Status``, so the check is
    for an explicit failure, not merely for the kind.
    """
    body = obj if isinstance(obj, dict) else getattr(obj, "to_dict", dict)()
    if not isinstance(body, dict) or body.get("kind") != "Status":
        return obj

    code = body.get("code")
    if body.get("status") != "Failure" and not (isinstance(code, int) and code >= 400):
        return obj

    exc = ApiException(
        status=code if isinstance(code, int) else 500,
        reason=str(body.get("reason") or "Failure"),
    )
    # errors.describe_api_error() reads the Status out of .body, so the mapping
    # is identical whether the exception came from here or from the REST layer.
    exc.body = json.dumps(body).encode()
    raise exc


class KubeClient:
    """Shared, pooled access to one cluster."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._api_client: ApiClient | None = None
        self._dynamic: DynamicClient | None = None
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_requests)

    # -- lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        """Load credentials and warm the discovery cache.

        Failing here is correct -- a server that cannot reach a cluster has
        nothing to serve -- but the failure has to *explain itself*. In
        Kubernetes this surfaces as a CrashLoopBackOff, and the only thing
        anyone sees is this log line, so it names which credential source was
        tried and what to set.
        """
        configuration = Configuration()
        try:
            if self._settings.kubeconfig:
                source = f"kubeconfig {self._settings.kubeconfig}"
                await kube_config.load_kube_config(
                    config_file=self._settings.kubeconfig,
                    context=self._settings.kube_context,
                    client_configuration=configuration,
                )
            elif _IN_CLUSTER_TOKEN.exists():  # noqa: ASYNC240 - startup, before serving
                source = "in-cluster ServiceAccount"
                kube_config.load_incluster_config(  # type: ignore[no-untyped-call]
                    client_configuration=configuration
                )
            else:
                # Developer machine: fall back to the default kubeconfig.
                source = "default kubeconfig (~/.kube/config)"
                await kube_config.load_kube_config(
                    context=self._settings.kube_context,
                    client_configuration=configuration,
                )
        except Exception as exc:
            raise ClusterUnreachable(
                f"Could not load Kubernetes credentials from {source}: {exc}. "
                "Inside a cluster this server uses the pod's ServiceAccount token at "
                f"{_IN_CLUSTER_TOKEN}; outside one, set KUBEMCP_KUBECONFIG to a readable "
                "kubeconfig (note that a kubeconfig referencing certificate files must "
                "have those files available too)."
            ) from exc

        self._api_client = ApiClient(configuration=configuration)
        try:
            self._dynamic = await DynamicClient(self._api_client)
        except Exception as exc:
            await self.close()
            raise ClusterUnreachable(
                f"Loaded credentials from {source}, but could not reach the apiserver at "
                f"{configuration.host}: {exc}. Check the address is routable from this pod "
                "and that any NetworkPolicy permits egress to the apiserver."
            ) from exc
        logger.info("connected to %s via %s", configuration.host, source)

    async def close(self) -> None:
        if self._api_client is not None:
            await self._api_client.close()
            self._api_client = None
            self._dynamic = None

    @property
    def api_client(self) -> ApiClient:
        if self._api_client is None:
            raise RuntimeError("KubeClient.connect() has not been called")
        return self._api_client

    @property
    def dynamic(self) -> DynamicClient:
        if self._dynamic is None:
            raise RuntimeError("KubeClient.connect() has not been called")
        return self._dynamic

    @property
    def settings(self) -> Settings:
        return self._settings

    # -- discovery ----------------------------------------------------------

    async def resource_for(self, api_version: str, kind: str) -> Resource:
        """Resolve ``apiVersion`` + ``kind`` to a discovered resource.

        Discovery failures become :class:`ToolError` rather than surfacing as a
        crash, because a wrong kind is precisely the sort of mistake the caller
        can correct on its next attempt.
        """
        try:
            return await self.dynamic.resources.get(api_version=api_version, kind=kind)
        except ResourceNotFoundError as exc:
            raise ToolError(
                f"No resource {kind!r} found in apiVersion {api_version!r} on this cluster. "
                "Call list_api_resources to see what is registered, including custom "
                "resources."
            ) from exc
        except ResourceNotUniqueError as exc:
            raise ToolError(
                f"{kind!r} in {api_version!r} is ambiguous on this cluster: several resources "
                "match. Pass the fully qualified apiVersion, e.g. 'apps/v1'."
            ) from exc

    async def get_json(self, path: str, *, action: str) -> dict[str, Any]:
        """GET a path and return the decoded JSON body.

        Used for endpoints outside the dynamic client's model -- ``/version``
        and the discovery endpoints -- where a plain dict is what is wanted.
        """
        async with self._semaphore:
            with guard(action):
                raw = await asyncio.wait_for(
                    self.api_client.call_api(
                        path,
                        "GET",
                        response_types_map={200: "object"},
                        auth_settings=["BearerToken"],
                    ),
                    timeout=self._settings.request_timeout_seconds,
                )
        body = raw[0] if isinstance(raw, tuple) else raw
        return body if isinstance(body, dict) else {}

    async def server_version(self) -> dict[str, Any]:
        return await self.get_json("/version", action="read the cluster version")

    async def ready(self) -> bool:
        """Cheap readiness probe: can we still reach the apiserver?"""
        try:
            async with self._semaphore:
                await self.api_client.call_api(
                    "/readyz", "GET", response_types_map={200: "str"}, auth_settings=["BearerToken"]
                )
            return True
        except Exception:
            logger.warning("readiness probe against the apiserver failed", exc_info=True)
            return False

    # -- operations ---------------------------------------------------------
    # Each takes an explicit `action` so a failure can say what was attempted.

    async def get(self, resource: Resource, *, action: str, **kwargs: Any) -> Any:
        return await self._call(self.dynamic.get, resource, action=action, **kwargs)

    async def create(self, resource: Resource, *, action: str, **kwargs: Any) -> Any:
        return await self._call(self.dynamic.create, resource, action=action, **kwargs)

    async def apply(self, resource: Resource, *, action: str, **kwargs: Any) -> Any:
        return await self._call(self.dynamic.server_side_apply, resource, action=action, **kwargs)

    async def patch(self, resource: Resource, *, action: str, **kwargs: Any) -> Any:
        return await self._call(self.dynamic.patch, resource, action=action, **kwargs)

    async def delete(self, resource: Resource, *, action: str, **kwargs: Any) -> Any:
        return await self._call(self.dynamic.delete, resource, action=action, **kwargs)

    async def request(self, method: str, path: str, *, action: str, **kwargs: Any) -> Any:
        """Escape hatch for subresources the dynamic client does not model."""
        async with self._semaphore:
            with guard(action):
                return raise_for_status(
                    await asyncio.wait_for(
                        self.dynamic.request(method, path, **kwargs),
                        timeout=self._settings.request_timeout_seconds,
                    )
                )

    async def _call(
        self,
        operation: Any,
        resource: Resource,
        *,
        action: str,
        **kwargs: Any,
    ) -> Any:
        """Run one apiserver operation under the shared limits and checks.

        Every dynamic-client call funnels through here so the concurrency
        ceiling, the request timeout, the Status check and the error mapping are
        applied uniformly -- none of them can be forgotten at a call site.
        """
        async with self._semaphore:
            with guard(action):
                return raise_for_status(
                    await asyncio.wait_for(
                        operation(resource, **kwargs),
                        timeout=self._settings.request_timeout_seconds,
                    )
                )


def to_dict(obj: Any) -> dict[str, Any]:
    """Normalise a dynamic-client return value into a plain dict.

    The dynamic client hands back ``ResourceInstance`` wrappers; everything
    downstream (shaping, JSON encoding) wants plain data.
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    for attr in ("to_dict", "to_json"):
        method = getattr(obj, attr, None)
        if callable(method):
            result = method()
            if isinstance(result, dict):
                return result
    return dict(getattr(obj, "__dict__", {}) or {})
