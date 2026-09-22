from claude_agent_sdk import ClaudeAgentOptions, query, ToolPermissionContext, AssistantMessage, UserMessage, TextBlock, ResultMessage, PermissionResultAllow, ToolUseBlock, ToolResultBlock, AgentDefinition, HookMatcher
from mq.event import AlertState, Event, Status
from textwrap import shorten
import config.config as config
import json

from typing import Any

import dataclasses
from .approval_gate import ApprovalGate, Notifier

def preview(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return shorten(text, width=240, placeholder=" …")

# hook that prevents kubectl writes until a human-in-the-loop approves. 
def guard_hook():
    pass


class Harness:
    def __init__(self, config: config.AgentOptionsConfig):
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

        # tool approval check. 
        self.require_approval = config.require_approval
        notifier = Notifier()
        self.approvalGate = ApprovalGate(notifier, 15 * 60)
        notifier.on_decision = self.approvalGate.resolve

    # returns agent options with an extra canUseTool callback. 
    def withCanUseTool(self, event: Event) -> ClaudeAgentOptions:
        if self.agentOptions.can_use_tool is None:
            self.agentOptions = dataclasses.replace(
                self.agentOptions,
                can_use_tool=self.approvalGate.callback_for(event.describe(), self.require_approval),
            )
        return self.agentOptions


    async def start(self, event: Event) -> None:
        prompt = (
            "fix this error / issue sent through alertmanager:\n\n"
            f"{event.describe()}\n"
        )
        
        tool_names: dict[str, str] = {}
        print(f"prompt: \n {prompt}")

        async for message in query(prompt=prompt, options=self.withCanUseTool(event)):
            if isinstance(message, (AssistantMessage, UserMessage)):
                if isinstance(message, AssistantMessage) and message.error:
                    print(f"[error] {message.error}", flush=True)
                if isinstance(message.content, str):
                    continue
                for block in message.content:
                    if isinstance(block, TextBlock) and isinstance(message, AssistantMessage):
                        label = "subagent" if message.parent_tool_use_id else "assistant"
                        print(f"[{label}] {preview(block.text)}", flush=True)
                    elif isinstance(block, ToolUseBlock):
                        tool_names[block.id] = block.name
                        print(f"[tool] {block.name}: {preview(block.input)}", flush=True)
                    elif isinstance(block, ToolResultBlock):
                        name = tool_names.pop(block.tool_use_id, "tool")
                        status = "error" if block.is_error else "ok"
                        print(f"[{status}] {name}: {preview(block.content)}", flush=True)
            elif isinstance(message, ResultMessage):
                print("\nResponse:", flush=True)
                print(message.result or "No response returned.", flush=True)
                if message.errors:
                    print("Errors: " + "; ".join(message.errors), flush=True)
                status = "Error" if message.is_error else "Done"
                summary = (
                    f"{status} | turns: {message.num_turns}"
                    f" | time: {message.duration_ms / 1000:.1f}s"
                )
                if message.total_cost_usd is not None:
                    summary += f" | cost: ${message.total_cost_usd:.4f}"
                print(summary, flush=True)
