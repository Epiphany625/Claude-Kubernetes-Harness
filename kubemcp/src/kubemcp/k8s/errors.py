"""Translating Kubernetes API failures into something a model can act on.

The MCP SDK masks unhandled exceptions: the caller sees only
``"Error executing tool <name>"`` while the traceback goes to the server log.
That default is right -- a raw traceback can describe server internals -- but it
means an unmapped ``ApiException`` teaches the caller nothing, and a model that
learns nothing retries the identical call.

So every apiserver failure is mapped here to a :class:`ToolError` whose message
names what failed and what to do about it. The 403 case is worth singling out:
reporting *which verb on which resource* was refused is exactly what the harness
needs to turn a failure into a concrete RBAC rule for the Helm chart in ``ops/``.

The split between the two exception types follows the SDK's own rule -- "could a
smarter model have avoided this?". A missing object or a malformed patch: yes,
so :class:`ToolError`, and the model can correct itself. No credentials at all,
or an apiserver that is down: no, so :class:`MCPError`, which is a protocol-level
rejection rather than a tool result.
"""

from __future__ import annotations

import json
from typing import Any, Literal

from kubernetes_asyncio.client.exceptions import ApiException
from mcp.server.mcpserver.exceptions import ToolError
from mcp.shared.exceptions import MCPError
from mcp.types import INTERNAL_ERROR

from kubemcp.validation import ValidationError

__all__ = ["ToolError", "describe_api_error", "guard", "translate"]


def _status_body(exc: ApiException) -> dict[str, Any]:
    """Parse the apiserver's Status object out of the response body."""
    body = getattr(exc, "body", None)
    if not body:
        return {}
    try:
        parsed = json.loads(body)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def describe_api_error(exc: ApiException, *, action: str) -> str:
    """Build an actionable message for an apiserver failure.

    ``action`` describes what was attempted, e.g. ``"get pod default/web-0"``.
    """
    status = _status_body(exc)
    message = status.get("message") or getattr(exc, "reason", None) or "unknown error"
    reason = status.get("reason", "")
    details = status.get("details") or {}
    code = getattr(exc, "status", None)

    if code == 403:
        # Name the missing permission so the caller can report the exact rule.
        group = details.get("group") or "core"
        resource = details.get("kind") or "resource"
        return (
            f"Forbidden: cannot {action}. The apiserver refused this request under RBAC: "
            f"{message}. The ServiceAccount this server runs as needs permission for "
            f"resource {resource!r} in API group {group!r}. This server does not grant "
            f"permissions; add the rule to the Role/ClusterRole bound to its ServiceAccount."
        )
    if code == 401:
        return (
            f"Unauthorized: the credentials this server presented were rejected while "
            f"trying to {action}. Its ServiceAccount token is missing, expired or invalid."
        )
    if code == 404:
        return (
            f"Not found: cannot {action}. {message}. "
            "Check the name, the namespace, and that the resource kind exists in this "
            "cluster (list_api_resources shows what is registered)."
        )
    if code == 405:
        return (
            f"Not allowed: cannot {action}. {message}. "
            "This verb is not supported on this resource; list_api_resources reports the "
            "verbs each resource accepts."
        )
    if code == 409:
        if reason == "AlreadyExists":
            return (
                f"Conflict: cannot {action} because the object already exists. {message}. "
                "Use apply_resource to create-or-update instead of create_resource."
            )
        return (
            f"Conflict: cannot {action}. {message}. "
            "Another writer changed this object concurrently, or another field manager owns "
            "the fields being set. Re-read the object and retry, or pass force_conflicts=true "
            "to apply_resource to take ownership of the contested fields."
        )
    if code == 410:
        return (
            f"Expired: cannot {action}. {message}. "
            "The continue token from the previous page is no longer valid; restart the "
            "listing without a continue token."
        )
    if code == 422:
        causes = details.get("causes") or []
        rendered = "; ".join(
            f"{c.get('field', '?')}: {c.get('message', '')}" for c in causes if isinstance(c, dict)
        )
        return (
            f"Rejected: cannot {action}. The apiserver refused the object as invalid: {message}."
            + (f" Causes: {rendered}." if rendered else "")
        )
    if code == 429:
        return (
            f"Throttled: cannot {action} right now. {message}. "
            "The apiserver is rate-limiting; retry after a short delay."
        )
    if code is not None and code >= 500:
        return (
            f"Server error: cannot {action}. The apiserver returned {code}: {message}. "
            "This is a cluster-side failure, not a problem with the request."
        )
    return f"Cannot {action}: {message} (HTTP {code})."


def translate(exc: Exception, *, action: str) -> Exception:
    """Map an exception raised while talking to the apiserver."""
    if isinstance(exc, ValidationError):
        return ToolError(str(exc))
    if isinstance(exc, ApiException):
        code = getattr(exc, "status", None)
        if code == 401:
            # Nothing the caller can rephrase; the deployment itself is broken.
            return MCPError(INTERNAL_ERROR, describe_api_error(exc, action=action))
        return ToolError(describe_api_error(exc, action=action))
    if isinstance(exc, TimeoutError):
        return ToolError(
            f"Timed out trying to {action}. The apiserver did not respond within the "
            "configured request timeout."
        )
    return exc


class guard:
    """Context manager that maps apiserver failures raised inside it.

    Usage::

        with guard(f"get pod {namespace}/{name}"):
            obj = await client.get(...)
    """

    __slots__ = ("_action",)

    def __init__(self, action: str) -> None:
        self._action = action

    def __enter__(self) -> guard:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> Literal[False]:
        if exc is None or not isinstance(exc, Exception):
            return False
        mapped = translate(exc, action=self._action)
        if mapped is exc:
            return False
        raise mapped from exc
