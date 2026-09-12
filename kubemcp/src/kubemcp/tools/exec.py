"""Running a command inside a container."""

from __future__ import annotations

from typing import Annotated

from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import ToolAnnotations
from pydantic import Field

from kubemcp import validation
from kubemcp.context import AppContext
from kubemcp.k8s.exec import exec_in_pod as run_exec
from kubemcp.models import ContainerName, ExecOutput, Namespace, ResourceName
from kubemcp.tools._common import kube_of, settings_of


def register(mcp: MCPServer) -> None:
    """Register the exec tool."""

    @mcp.tool(
        title="Execute command in pod",
        annotations=ToolAnnotations(
            read_only_hint=False,
            destructive_hint=True,
            idempotent_hint=False,
            open_world_hint=True,
        ),
        description=(
            "Run a command inside a running container and return its stdout, stderr and "
            "exit code. The command is an argv array and is executed directly, not "
            "through a shell, so pipes, redirects and globs have no effect unless you "
            'ask for a shell explicitly: ["sh", "-c", "ps aux | head"]. '
            "The pod must be Running. Output is capped and the command is killed at the "
            "timeout."
        ),
    )
    async def exec_in_pod(
        ctx: Context[AppContext],
        name: ResourceName,
        command: Annotated[
            list[str],
            Field(
                description=(
                    'Command as an argv array, e.g. ["cat", "/etc/resolv.conf"]. '
                    "Executed directly without a shell."
                ),
                examples=[["ls", "-la", "/app"], ["sh", "-c", "df -h | tail -5"]],
            ),
        ],
        namespace: Namespace = "default",
        container: ContainerName = None,
        timeout_seconds: Annotated[
            float | None,
            Field(gt=0, le=600, description="Kill the command after this long."),
        ] = None,
    ) -> ExecOutput:
        kube = kube_of(ctx)
        settings = settings_of(ctx)
        argv = validation.validate_command(command)

        result = await run_exec(
            kube,
            namespace=namespace,
            pod=name,
            command=argv,
            container=container,
            timeout_seconds=timeout_seconds,
            default_timeout=settings.exec_timeout_seconds,
            max_output_bytes=settings.max_exec_output_bytes,
        )
        return ExecOutput(
            pod=name,
            namespace=namespace,
            container=container,
            command=argv,
            exit_code=result.exit_code,
            stdout=result.stdout,
            stderr=result.stderr,
            timed_out=result.timed_out,
            truncated=result.truncated,
        )
