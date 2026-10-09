from claude_agent_sdk import ClaudeAgentOptions, query, ToolPermissionContext, AssistantMessage, UserMessage, TextBlock, ResultMessage, PermissionResultAllow, PermissionResultDeny, ToolUseBlock, ToolResultBlock, AgentDefinition, HookMatcher
from mq.event import AlertState, Event, Status
from textwrap import shorten
import config.config as config
import json

from typing import Any, Callable

import dataclasses
import asyncio
from .tools import *

def preview(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return shorten(text, width=240, placeholder=" …")

# hook that prevents kubectl writes until a human-in-the-loop approves. 
def guard_hook():
    pass
class AgentService:
    def __init__(self, config: config.AgentOptionsConfig, taskQueue: asyncio.Queue[Event], sanity_check: bool = True):
        self.agentOptions = ClaudeAgentOptions(
            tools=config.tools,
            allowed_tools=config.allowed_tools, 
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
        self.sanity_check = sanity_check

        self.require_approval = config.require_approval
    
    def _callback_for(self, incident: str, require_approval: bool) -> Callable:
        async def can_use_tool(
            tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
        ) -> PermissionResultAllow | PermissionResultDeny:
            if tool_name.startswith("mcp__kubemcp__"):
                if (not self.require_approval) or tool_name in KUBEMCP_READ_TOOLS:
                    return PermissionResultAllow(updated_input=tool_input)
                else:
                    await asyncio.sleep(1)
                    print("_________permission result deny triggered for write tools")
                    return PermissionResultDeny(message="This tool requires human-in-the-loop approval. The tool use is notified to the human for their approval, and the session will be saved. Please terminate your run and explain that once the human has approved, the execution will resume. ")
            
            return PermissionResultDeny(message="tool not allowed. ")
                
            # TODO. finish. 
        return can_use_tool

    # returns agent options with an extra canUseTool callback. 
    def _withCanUseTool(self, event: Event) -> ClaudeAgentOptions:
        if event is not None:
            return dataclasses.replace(
                self.agentOptions,
                can_use_tool=self._callback_for(event.describe(), self.require_approval),
            )
        return dataclasses.replace(
                        self.agentOptions,
                        can_use_tool=self._callback_for("", self.require_approval),
        )

    async def _start_agent(self):
        if self.sanity_check:
            return await self._handle_event()
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

    async def _handle_event(self, event: Event | None = None):
        prompt = (
            "fix this error / issue sent through alertmanager:\n\n"
            f"{event.describe()}\n"
        ) if event is not None else "there is an image pull error in default namespace. fix"
        
        print(prompt)
        
        tool_names: dict[str, str] = {}
        self.logger.info(f"prompt: \n {prompt}")

        async for message in query(prompt=prompt, options=self._withCanUseTool(event)):
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
                        status = "error" if block.is_error else "ok"
                        self.logger.info(f"[{status}] {name}: {preview(block.content)}")
            elif isinstance(message, ResultMessage):
                self.logger.info("\nResponse:")
                self.logger.info(message.result or "No response returned.")
                if message.errors:
                    self.logger.info("Errors: " + "; ".join(message.errors))
                status = "Error" if message.is_error else "Done"
                summary = (
                    f"{status} | turns: {message.num_turns}"
                    f" | time: {message.duration_ms / 1000:.1f}s"
                )
                if message.total_cost_usd is not None:
                    summary += f" | cost: ${message.total_cost_usd:.4f}"
                self.logger.info(summary)


    async def start(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for _ in range(self.worker):
                tg.create_task(self._start_agent())