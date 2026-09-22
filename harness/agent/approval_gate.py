from claude_agent_sdk.types import (
    PermissionResultAllow, 
    PermissionResultDeny, 
    ToolPermissionContext
)
from typing import *
import asyncio
import json
import uuid
from dataclasses import dataclass
from .tools import * 

@dataclass
class ApprovalRequest:
    id: str
    incident: str
    tool_name: str
    tool_input: Dict[str, Any]

@dataclass
class RequestDecision:
    approved: bool
    by: str
    note: str | None = None

# a small notifier for testing. In the future, this will be replaced by a real notifier, like an email / Slack msg. 
class Notifier:
    def __init__(self) -> None:
        # a callback function that will execute after a human decision is made. 
        self.on_decision: Callable[..., bool] | None = None
        # a set of tasks.
        self._tasks: set[asyncio.Task] = set()

    # Main functionality: send out request & await human approval. 
    async def request_human_approval(self, request: ApprovalRequest):
        print(f"\n=== APPROVAL NEEDED [{request.id}] ===", flush=True)
        print(f"incident: {request.incident}")
        print(f"tool:     {request.tool_name}")
        # Show the raw input, not the agent's summary of it.
        print(json.dumps(request.tool_input, indent=2, ensure_ascii=False), flush=True)
        task = asyncio.create_task(self._ask(request.id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _ask(self, request_id: str) -> None:
        answer = await asyncio.to_thread(input, f"Approve {request_id}? [y/N] ")
        # invoke callback
        if self.on_decision:
            self.on_decision(request_id, answer.strip().lower() == "y", by="console")



class ApprovalGate:
    def __init__(self, notifier: Notifier, timeout_seconds: float):
        self.timeout_seconds = timeout_seconds 
        self.notifier = notifier

        # futures waiting for decisions. 
        self._waiting: dict[str, asyncio.Future[RequestDecision]] = {}

    def resolve(self, request_id: str, approved: bool, by: str, note: str | None = None) -> bool:
            fut = self._waiting.get(request_id)
            if fut is None or fut.done():
                return False
            fut.set_result(RequestDecision(approved=approved, by=by, note=note))
            return True

    def callback_for(self, incident: str, require_approval: bool) -> Callable:
        async def can_use_tool(
            tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
        ) -> PermissionResultAllow | PermissionResultDeny:

            if not require_approval:
                return PermissionResultAllow(updated_input=tool_input)

            # allow all read tools
            if tool_name in KUBEMCP_READ_TOOLS:
                return PermissionResultAllow(updated_input=tool_input)

            req = ApprovalRequest(
                            id=uuid.uuid4().hex[:12],
                            incident=incident,
                            tool_name=tool_name,
                            tool_input=tool_input,
                        )            
            fut: asyncio.Future[RequestDecision] = asyncio.get_running_loop().create_future()
            self._waiting[req.id] = fut

            try:
                await self.notifier.request_human_approval(req)
                decision = await asyncio.wait_for(fut, timeout=self.timeout_seconds)
            except asyncio.TimeoutError:
                print(f"[gate] {req.id} expired", flush=True)
                return PermissionResultDeny(
                    message=(
                        f"No human approved this within {self.timeout_seconds / 60:.0f} minutes. "
                        "Do not retry it. Summarize the change you wanted and why."
                    )
                )
            except Exception as exc:  # notifier down etc. -> never run an unapproved write
                print(f"[gate] {req.id} failed: {exc!r}", flush=True)
                return PermissionResultDeny(
                    message="The approval system is unavailable; the change was not made."
                )
            finally:
                self._waiting.pop(req.id, None)

            verdict = "approved" if decision.approved else "rejected"
            print(f"[gate] {req.id} {verdict} by {decision.by}", flush=True)

            if decision.approved:
                # Return the exact input the human saw: what runs == what was approved.
                return PermissionResultAllow(updated_input=tool_input)

            # Claude reads this message, so phrase it as an instruction.
            reason = f": {decision.note}" if decision.note else "."
            return PermissionResultDeny(
                message=f"{decision.by} rejected this change{reason} Do not retry the same change."
            )
        return can_use_tool

