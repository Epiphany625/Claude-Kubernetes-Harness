from claude_agent_sdk import ClaudeAgentOptions, query, ToolPermissionContext, AssistantMessage, UserMessage, TextBlock, ResultMessage, PermissionResultAllow, PermissionResultDeny, ToolUseBlock, ToolResultBlock, AgentDefinition, HookMatcher
from mq.event import AlertState, Event, Status
from postgres.postgres import PostgresClient
from notifier.notifier import NotifierSet
from textwrap import shorten
import config.config as config
import json

from typing import Any, Callable

import dataclasses
import asyncio
import uuid
from contextlib import suppress
from .tools import *

def preview(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return shorten(text, width=240, placeholder=" …")

def report_outcome(result: str | None) -> str | None:
    """The `outcome` of the orchestrator's JSON report, or None when the result is not one.

    With background subagents a run yields several ResultMessages; only the last
    carries the report, the earlier ones are progress notes.
    """
    if not result:
        return None
    start, end = result.find("{"), result.rfind("}")
    if start == -1 or end < start:
        return None
    try:
        report = json.loads(result[start:end + 1])
    except json.JSONDecodeError:
        return None
    outcome = report.get("outcome") if isinstance(report, dict) else None
    return outcome if isinstance(outcome, str) else None

# hook that prevents kubectl writes until a human-in-the-loop approves. 
def guard_hook():
    pass
class AgentService:
    def __init__(self, config: config.AgentOptionsConfig, taskQueue: asyncio.Queue[Event], pgClient: PostgresClient, notifierSet: NotifierSet):
        self.agentOptions = ClaudeAgentOptions(
            tools=[*config.tools, *APPROVAL_TOOLS],
            allowed_tools=[*config.allowed_tools, *APPROVAL_TOOLS],
            system_prompt=config.system_prompt,
            model=config.model,
            disallowed_tools=config.disallowed_tools,
            max_turns=config.max_turns,
            effort=config.effort,
            mcp_servers=config.mcp_servers,
            agents={key: AgentDefinition(
                description=val.description, 
                prompt=val.prompt, 
                tools=val.tools, 
                model=val.model, 
                maxTurns=val.maxTurns, 
                effort=val.effort, 
                mcpServers=val.mcpServers
            ) for key, val in config.agents.items()}
        )
        self.logger = config.logger
        self.taskQueue = taskQueue
        self.worker = config.worker # number of workers.
        self.pgClient = pgClient
        self.notifierSet = notifierSet

        self.require_approval = config.require_approval # answers: does this run require human approval for write tools? 

    def _callback_for(self, require_approval: bool, sessionID: str) -> Callable:
        async def can_use_tool(
            tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
        ) -> PermissionResultAllow | PermissionResultDeny:
            if not require_approval:
                return PermissionResultAllow(updated_input=tool_input)

            if tool_name.startswith("mcp__kubemcp__"):
                if tool_name in KUBEMCP_READ_TOOLS:
                    return PermissionResultAllow(updated_input=tool_input)
                # check database to see whether the tool is already approved. 
                elif tool_name in await self.pgClient.approvalStore.getApprovedTools(sessionID):
                    return PermissionResultAllow(updated_input=tool_input)
                else:
                    self.logger.info(f"[agent] {tool_name} denied: awaiting human approval")
                    return PermissionResultDeny(message=(
                        f"{tool_name} writes to the cluster and needs human approval; it was not run. "
                        "Stop now: make no further writes and do not look for a workaround. "
                        "End your run and report status \"skipped\", listing in `skipped` every "
                        "write you have not applied yet -- this one first -- with its tool name, "
                        "exact arguments, intent and risk, and the reason \"awaiting human approval\". "
                        "The orchestrator submits them with ask_human_approval, and the session "
                        "resumes once a human approves."
                    ))

            if tool_name in APPROVAL_TOOLS:
                return PermissionResultAllow(updated_input=tool_input)

            return PermissionResultDeny(message="tool not allowed. ")

        return can_use_tool

    async def _start_agent(self):
        while True:
            event = await self.taskQueue.get()
            try:
                await self._handle_event(event)
            except Exception:
                # One bad event must not take down the TaskGroup: that cancels the
                # RabbitMQ task, which aio_pika swallows, and the harness hangs silently.
                self.logger.exception("[agent] failed handling event")
            finally:
                self.taskQueue.task_done()

    def _prompt_for(self, event: Event) -> str:
        # A sessionID means this event resumes a run that stopped for approval.
        # The session already holds the alert and the proposed write, so the
        # prompt only needs to say that the human approved it.
        if event.sessionID:
            return (
                "A human has approved the cluster write you proposed for this alert. "
                "Continue from where you stopped: apply the approved actions exactly "
                "as proposed, then verify and report as before.\n\n"
                f"{event.describe()}\n"
            )
        return (
            "fix this error / issue sent through alertmanager:\n\n"
            f"{event.describe()}\n"
        )

    async def _handle_event(self, event: Event):
        prompt = self._prompt_for(event)
        tool_names: dict[str, str] = {}

        # No session ID means a fresh event: create one and record it. Otherwise the
        # event was republished after a human approval and resumes that session.
        resume = event.sessionID
        sessionID = resume or str(uuid.uuid4())
        if not resume:
            await self.pgClient.eventStore.insertSessionID(event.eventID, sessionID)
        await self.pgClient.eventStore.updateEventState(event.eventID, Status.SOLVING)
        self.logger.info(f"[agent] event {event.eventID}: {'resuming' if resume else 'new'} session {sessionID}")
        self.logger.debug(f"[agent] prompt:\n{prompt}")

        options = dataclasses.replace(
                self.agentOptions,
                mcp_servers={
                    **self.agentOptions.mcp_servers,
                    APPROVAL_SERVER_NAME: approval_server_for(self.notifierSet, event.eventID, sessionID),
                },
                session_store=self.pgClient.claudeSessionStore, 
                can_use_tool=self._callback_for(self.require_approval, sessionID),
                # The CLI rejects --session-id together with --resume.
                resume=resume or None,
                session_id=None if resume else sessionID,
        )

        outcome: str | None = None
        # Once a human has been asked, the status belongs to the approval flow.
        approvalRequested = False
        try:
            async for message in query(prompt=prompt, options=options):
                if isinstance(message, (AssistantMessage, UserMessage)):
                    if isinstance(message, AssistantMessage) and message.error:
                        self.logger.info(f"[error] {message.error}")
                    if isinstance(message.content, str):
                        continue
                    for block in message.content:
                        if isinstance(block, TextBlock) and isinstance(message, AssistantMessage):
                            label = "subagent" if message.parent_tool_use_id else "assistant"
                            self.logger.info(f"[{label}] {preview(block.text)}")
                        elif isinstance(block, ToolUseBlock):
                            tool_names[block.id] = block.name
                            self.logger.info(f"[tool] {block.name}: {preview(block.input)}")
                        elif isinstance(block, ToolResultBlock):
                            name = tool_names.pop(block.tool_use_id, "tool")
                            self.logger.info(f"[{'error' if block.is_error else 'ok'}] {name}: {preview(block.content)}")
                            if name == ASK_HUMAN_APPROVAL_TOOL and not block.is_error and not approvalRequested:
                                approvalRequested = True
                                await self.pgClient.eventStore.updateEventState(event.eventID, Status.WAITING_HUMAN_APPROVAL)
                elif isinstance(message, ResultMessage):
                    outcome = report_outcome(message.result) or outcome
                    summary = (
                        f"[result] {'error' if message.is_error else 'done'} | turns: {message.num_turns}"
                        f" | time: {message.duration_ms / 1000:.1f}s"
                    )
                    if message.total_cost_usd is not None:
                        summary += f" | cost: ${message.total_cost_usd:.4f}"
                    self.logger.info(f"{summary}\n{message.result or 'No response returned.'}")
                    if message.errors:
                        self.logger.info("[result] errors: " + "; ".join(message.errors))
        except Exception:
            if not approvalRequested:
                with suppress(Exception):
                    await self.pgClient.eventStore.updateEventState(event.eventID, Status.FAILED_TO_RESOLVE)
            raise

        if approvalRequested:
            self.logger.info(f"[agent] event {event.eventID}: outcome={outcome}, status={Status.WAITING_HUMAN_APPROVAL}")
            return
        final = Status.RESOLVED if outcome == "resolved" else Status.FAILED_TO_RESOLVE
        await self.pgClient.eventStore.updateEventState(event.eventID, final)
        self.logger.info(f"[agent] event {event.eventID}: outcome={outcome}, status={final}")


    async def start(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for _ in range(self.worker):
                tg.create_task(self._start_agent())