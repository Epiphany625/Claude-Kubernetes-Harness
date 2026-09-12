"""Running a command inside a container.

The apiserver's exec subresource takes ``command`` as an **argv array** and
documents it as "Not executed within a shell". That is the property this module
preserves end to end: the argv list validated in :mod:`kubemcp.validation` is
the argv list the container runtime execs, so quoting, ``;``, ``|`` and ``$()``
carry no special meaning anywhere in the path. A caller that wants shell
semantics has to ask for them in the open, as ``["sh", "-c", "..."]``.

The transport is a multiplexed websocket: byte 0 of every frame is a channel
number, and the rest is payload. ``kubernetes_asyncio``'s own helper collapses
stdout and stderr into one string and drops the error channel, which loses the
exit code -- the single most useful bit of an exec for a diagnosing agent. So
the socket is driven directly here to keep the three streams apart.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from kubernetes_asyncio.client import Configuration
from kubernetes_asyncio.client.api import CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

from kubemcp.k8s.client import KubeClient
from kubemcp.k8s.errors import ToolError, guard
from kubemcp.shaping import truncate_text

logger = logging.getLogger(__name__)

STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2
ERROR_CHANNEL = 3


class ExecResult:
    """Outcome of one exec."""

    __slots__ = ("exit_code", "stderr", "stdout", "timed_out", "truncated")

    def __init__(
        self,
        *,
        stdout: str,
        stderr: str,
        exit_code: int | None,
        truncated: bool,
        timed_out: bool,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code
        self.truncated = truncated
        self.timed_out = timed_out


def _parse_exit_code(payload: str) -> int | None:
    """Read the exit code out of an ERROR_CHANNEL status frame.

    The frame is a ``Status`` object: ``Success`` for exit 0, otherwise a
    ``NonZeroExitCode`` reason whose cause message holds the code.
    """
    try:
        status = json.loads(payload)
    except ValueError:
        return None
    if status.get("status") == "Success":
        return 0
    for cause in (status.get("details") or {}).get("causes") or []:
        if isinstance(cause, dict) and cause.get("reason") == "ExitCode":
            try:
                return int(cause.get("message", ""))
            except (TypeError, ValueError):
                continue
    return None


async def exec_in_pod(
    kube: KubeClient,
    *,
    namespace: str,
    pod: str,
    command: list[str],
    container: str | None = None,
    timeout_seconds: float | None = None,
    default_timeout: float,
    max_output_bytes: int,
) -> ExecResult:
    """Execute ``command`` in a container and collect its output."""
    timeout = timeout_seconds or default_timeout
    configuration: Configuration = kube.api_client.configuration

    target = f"{namespace}/{pod}" + (f" [{container}]" if container else "")
    action = f"exec {command[0]!r} in pod {target}"

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    exit_code: int | None = None
    collected = 0
    truncated = False
    timed_out = False
    budget = max_output_bytes

    ws_client = WsApiClient(configuration=configuration)
    try:
        core = CoreV1Api(ws_client)
        kwargs: dict[str, Any] = {
            "command": command,
            "stderr": True,
            "stdout": True,
            "stdin": False,
            "tty": False,
            "_preload_content": False,
        }
        if container:
            kwargs["container"] = container

        with guard(action):
            # The generated client is untyped; this is an async context manager
            # yielding the multiplexed websocket.
            connector: Any = await core.connect_get_namespaced_pod_exec(pod, namespace, **kwargs)

        async def pump() -> None:
            nonlocal exit_code, collected, truncated
            async with connector as websocket:
                async for message in websocket:
                    data = message.data
                    if isinstance(data, str):
                        data = data.encode("utf-8")
                    if not data:
                        continue
                    channel, payload = data[0], data[1:]
                    if not payload:
                        continue
                    text = payload.decode("utf-8", errors="replace")
                    if channel == ERROR_CHANNEL:
                        exit_code = _parse_exit_code(text)
                        continue
                    if collected >= budget:
                        truncated = True
                        continue
                    collected += len(payload)
                    if channel == STDOUT_CHANNEL:
                        stdout_parts.append(text)
                    elif channel == STDERR_CHANNEL:
                        stderr_parts.append(text)

        try:
            await asyncio.wait_for(pump(), timeout=timeout)
        except TimeoutError:
            # Partial output is still worth returning: a command that hung
            # after printing something usually printed the useful part.
            timed_out = True
            logger.info("exec timed out after %ss: %s", timeout, action)
    except ToolError:
        raise
    except Exception as exc:
        raise ToolError(
            f"Failed to {action}: {exc.__class__.__name__}. The pod must be Running with the "
            "target container started, and the apiserver must permit 'pods/exec'."
        ) from exc
    finally:
        await ws_client.close()

    stdout, stdout_cut = truncate_text("".join(stdout_parts), budget)
    stderr, stderr_cut = truncate_text("".join(stderr_parts), budget)
    return ExecResult(
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        truncated=truncated or stdout_cut or stderr_cut,
        timed_out=timed_out,
    )
