import asyncio
from typing import Any, Final

from claude_agent_sdk import create_sdk_mcp_server, tool

KUBEMCP_READ_TOOLS: Final = set(
        ["mcp__kubemcp__list_resources",
        "mcp__kubemcp__get_resource",
        "mcp__kubemcp__describe_resource",
        "mcp__kubemcp__get_pod_logs",
        "mcp__kubemcp__list_events",
        "mcp__kubemcp__get_top_metrics",
        "mcp__kubemcp__list_api_resources",
        "mcp__kubemcp__get_cluster_info"
        ]
)

# The Agent SDK takes custom tools only through an in-process MCP server, so
# Claude sees this tool as mcp__approval__ask_human_approval.
APPROVAL_SERVER_NAME: Final = "approval"
ASK_HUMAN_APPROVAL_TOOL: Final = f"mcp__{APPROVAL_SERVER_NAME}__ask_human_approval"

ASK_HUMAN_APPROVAL_SCHEMA: Final = {
    "type": "object",
    "properties": {
        "summary": {
            "type": "string",
            "description": "One sentence on what is broken and why these writes fix it.",
        },
        "actions": {
            "type": "array",
            "description": "The cluster writes awaiting approval, in the order they will run.",
            "items": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string", "description": "kubemcp tool name, e.g. mcp__kubemcp__patch_resource"},
                    "arguments": {"type": "object", "description": "exact arguments the tool will be called with"},
                    "intent": {"type": "string", "description": "what this write achieves"},
                    "risk": {"type": "string", "enum": ["low", "medium", "high"]},
                },
                "required": ["tool", "arguments", "intent", "risk"],
            },
            "minItems": 1,
        },
    },
    "required": ["summary", "actions"],
}

# Placeholder: the request is not persisted yet.
@tool(
    "ask_human_approval",
    "Submit the cluster writes the executor was refused to a human for approval. "
    "Call it once with every pending write, then end the run: the session resumes "
    "after the human approves.",
    ASK_HUMAN_APPROVAL_SCHEMA,
)
async def ask_human_approval(args: dict[str, Any]) -> dict[str, Any]:
    await asyncio.sleep(1)
    tools = ", ".join(action["tool"] for action in args["actions"])
    return {"content": [{"type": "text", "text": (
        f"Approval requested for: {tools}. Nothing has been applied. "
        "End your run now with outcome \"awaiting_approval\"; "
        "the session resumes once a human approves."
    )}]}

APPROVAL_SERVER: Final = create_sdk_mcp_server(name=APPROVAL_SERVER_NAME, tools=[ask_human_approval])
APPROVAL_TOOLS: Final = set([ASK_HUMAN_APPROVAL_TOOL])

KUBEMCP_NO_TOOLS: Final = set([])
