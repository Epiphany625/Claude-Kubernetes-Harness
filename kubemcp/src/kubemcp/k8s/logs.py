"""Container logs.

The log subresource returns ``text/plain``, not JSON, so it bypasses the dynamic
client (which would try to deserialize it) and goes through ``call_api`` with a
string response type.

Logs are fetched tail-first and truncated from the head, because the line that
explains a failure is almost always the last one.
"""

from __future__ import annotations

from typing import Any

from kubemcp.k8s.client import KubeClient
from kubemcp.k8s.errors import guard
from kubemcp.shaping import truncate_text


async def get_pod_logs(
    kube: KubeClient,
    *,
    namespace: str,
    pod: str,
    container: str | None = None,
    previous: bool = False,
    tail_lines: int | None = None,
    since_seconds: int | None = None,
    timestamps: bool = False,
    max_bytes: int,
) -> tuple[str, bool]:
    """Fetch logs for one container. Returns ``(text, truncated)``.

    ``previous=True`` reads the *previous* terminated container, which is the
    only way to see why a pod in CrashLoopBackOff died -- the current container
    is either not started or too young to have logged the failure.
    """
    query: list[tuple[str, Any]] = []
    if container:
        query.append(("container", container))
    if previous:
        query.append(("previous", "true"))
    if tail_lines is not None:
        query.append(("tailLines", str(tail_lines)))
    if since_seconds is not None:
        query.append(("sinceSeconds", str(since_seconds)))
    if timestamps:
        query.append(("timestamps", "true"))

    target = f"{namespace}/{pod}" + (f" [{container}]" if container else "")
    action = f"read {'previous ' if previous else ''}logs for pod {target}"

    with guard(action):
        response = await kube.api_client.call_api(
            f"/api/v1/namespaces/{namespace}/pods/{pod}/log",
            "GET",
            {},
            query,
            # The log subresource replies with text/plain, but the apiserver
            # negotiates Accept against its own supported media types and
            # answers 406 to a bare "text/plain". kubectl sends */* here.
            {"Accept": "*/*"},
            response_types_map={200: "str"},
            auth_settings=["BearerToken"],
            _return_http_data_only=True,
            _request_timeout=kube.settings.request_timeout_seconds,
        )

    text = response if isinstance(response, str) else str(response)
    return truncate_text(text, max_bytes)
