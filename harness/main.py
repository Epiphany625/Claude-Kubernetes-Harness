import asyncio
import json
from textwrap import shorten

from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    query,
)

if __package__:
    from .config.config import build_harness
else:
    from config.config import build_harness


def preview(value: object) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return shorten(text, width=240, placeholder=" …")


async def main() -> None:
    options = build_harness()
    prompt = "Can you help me check and analyze my minikube cluster? Also report if there is anything wrong with it. "
    print(f"Prompt: {prompt}", flush=True)
    tool_names: dict[str, str] = {}

    async for message in query(prompt=prompt, options=options):
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


if __name__ == "__main__":
    asyncio.run(main())
