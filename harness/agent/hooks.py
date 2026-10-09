from claude_agent_sdk import HookContext, HookInput, HookJSONOutput
from .tools import *

# PreToolUse hook: kubemcp reads run as usual, anything else (writes) is deferred.
# A deferred call stops the run and comes back on ResultMessage.deferred_tool_use,
# so a human can approve it and the session can be resumed.
async def kubemcp_tool_use_hook(input_data: HookInput, tool_use_id: str | None, context: HookContext) -> HookJSONOutput:

    if input_data["tool_name"] in KUBEMCP_NO_TOOLS:
        return {"hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": "allow",
                    }
                }
    
    print(input_data.get("agent_id"))
    print(input_data.get("agent_type"))
    
    return {
        "continue_": False,
        "stopReason": "Blocked: subagent tried a forbidden action",
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": "write to the cluster requires human approval",
        }
    }
