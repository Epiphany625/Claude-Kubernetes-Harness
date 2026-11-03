from typing import Any, Final

from claude_agent_sdk import McpSdkServerConfig, create_sdk_mcp_server, tool
from pydantic import ValidationError

from notifier.notifier import Action, NotifierSet

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

# Built per event so the tool can hand the notifier the event it belongs to.
def approval_server_for(notifierSet: NotifierSet, event_id: str, session_id: str) -> McpSdkServerConfig:
    @tool(
        "ask_human_approval",
        "Submit the cluster writes the executor was refused to a human for approval. "
        "Call it once with every pending write, then end the run: the session resumes "
        "after the human approves.",
        ASK_HUMAN_APPROVAL_SCHEMA,
    )
    async def ask_human_approval(args: dict[str, Any]) -> dict[str, Any]:
        # The SDK does not enforce the input schema, so a malformed call goes back to Claude to retry.
        try:
            actions = [Action.model_validate(action) for action in args["actions"]]
        except ValidationError as e:
            return {"content": [{"type": "text", "text": f"Invalid actions: {e}"}], "is_error": True}

        try:
            notifierSet.notify(args["summary"], actions, event_id, session_id)
        except Exception as e:
            return {"content": [{"type": "text", "text": (
                f"No human could be notified ({e}). Nothing has been applied and no approval is pending. "
                "End your run now and report every write as skipped."
            )}], "is_error": True}
        tools = ", ".join(action.tool for action in actions)
        return {"content": [{"type": "text", "text": (
            f"Approval requested for: {tools}. Nothing has been applied. "
            "End your run now with outcome \"awaiting_approval\"; "
            "the session resumes once a human approves."
        )}]}

    return create_sdk_mcp_server(name=APPROVAL_SERVER_NAME, tools=[ask_human_approval])

APPROVAL_TOOLS: Final = set([ASK_HUMAN_APPROVAL_TOOL])

KUBEMCP_NO_TOOLS: Final = set([])
