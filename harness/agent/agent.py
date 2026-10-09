from claude_agent_sdk import ClaudeAgentOptions, query, AssistantMessage, UserMessage, TextBlock, ResultMessage, ToolUseBlock, ToolResultBlock, AgentDefinition, HookMatcher
from mq.event import Event
from textwrap import shorten
import config.config as config
import json

from typing import Any

import asyncio
import dataclasses
from .tools import *
from .hooks import *

def preview(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return shorten(text, width=240, placeholder=" …")


class AgentService:
    def __init__(self, config: config.AgentOptionsConfig, taskQueue: asyncio.Queue[Event]):
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
            ) for key, val in config.agents.items()},
            # defer kubemcp writes until a human approves them.
            hooks={
                "PreToolUse": [HookMatcher(matcher=KUBEMCP_MATCHER, hooks=[kubemcp_tool_use_hook])]
            } if config.require_approval else None,
        )
        self.logger = config.logger
        self.taskQueue = taskQueue
        self.worker = config.worker # number of workers.
    
    async def _handle_sanity_run(self):
        async for message in query(prompt="Check errors in default namespace, using kubemcp tools. ", options=self.agentOptions):
            self._print_message(message)
            self._handle_message(message)

    async def _start_agent(self):
        if self.sanityRun:
            await self._handle_sanity_run()
            return 
    
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
    
    def _handle_message(self, message: Any):
        # a deferred tool call ends the run and is carried on the result message.
        if isinstance(message, ResultMessage) and message.deferred_tool_use:
            deferred = message.deferred_tool_use
            print(f"defer tool use called: session: {message.session_id} tool: {deferred.name} input: {preview(deferred.input)}")

    async def _handle_event(self, event: Event):
        prompt = (
            "fix this error / issue sent through alertmanager:\n\n"
            f"{event.describe()}\n"
        )
        
        self.logger.info(f"prompt: \n {prompt}")
        
        # if previous session exists, use previous session.
        options = self.agentOptions
        if event.sessionID:
            self.logger.info("resuming from a previous message. ")
            options = dataclasses.replace(options, resume=event.sessionID)

        async for message in query(prompt=prompt, options=options):
            self._print_message(message)
            self._handle_message(message)

    def _print_message(self, message: Any) -> None:
        if isinstance(message, (AssistantMessage, UserMessage)):
            if isinstance(message.content, str):
                return
            for block in message.content:
                if isinstance(block, TextBlock) and isinstance(message, AssistantMessage):
                    label = "subagent" if message.parent_tool_use_id else "assistant"
                    self.logger.info(f"[{label}] {preview(block.text)}")
                elif isinstance(block, ToolUseBlock):
                    self.logger.info(f"[tool] {block.name}: {preview(block.input)}")
                elif isinstance(block, ToolResultBlock):
                    status = "error" if block.is_error else "ok"
                    self.logger.info(f"[{status}] {preview(block.content)}")
        elif isinstance(message, ResultMessage):
            status = "Error" if message.is_error else "Done"
            self.logger.info(f"[result] {status} | turns: {message.num_turns} | {preview(message.result or 'No response returned.')}")


    async def start(self, sanityRun: bool = False) -> None:
        self.sanityRun = sanityRun
        async with asyncio.TaskGroup() as tg:
            for _ in range(self.worker):
                tg.create_task(self._start_agent())