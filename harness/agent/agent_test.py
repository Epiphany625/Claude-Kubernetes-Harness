
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import config.config as config

import pytest
from langfuse import Langfuse
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from postgres.postgres_mock import PostgresClientMock
from mq.event import Status

from claude_agent_sdk import (
    AssistantMessage,
    UserMessage,
    TextBlock,
    ToolUseBlock,
    ToolResultBlock,
    ResultMessage,
    ToolPermissionContext,
    PermissionResultAllow,
    PermissionResultDeny,
)

from agent import agent as agent_module
from agent.tools import *

# One client for the whole module: @observe resolves the client globally, and a
# second Langfuse instance would make that lookup ambiguous.
EXPORTER = InMemorySpanExporter()
LANGFUSE = Langfuse(
    public_key="pk-agent-test",
    secret_key="sk-agent-test",
    host="http://localhost",
    tracer_provider=TracerProvider(),
    span_exporter=EXPORTER,
)


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch):
    EXPORTER.clear()
    logger = Mock()
    notifier = Mock()

    cfg = config.AgentOptionsConfig(
        tools=[],
        allowed_tools=[],
        disallowed_tools=[],
        system_prompt="You are a Kubernetes agent",
        model="sonnet",
        max_turns=5,
        effort="low",
        mcp_servers={},
        agents={},
        logger=logger,
        worker=1,
        require_approval=True,
    )

    monkeypatch.setattr(
        agent_module,
        "approval_server_for",
        lambda *args: {
            "type": "stdio",
            "command": "noop",
            "args": [],
        },
    )
    monkeypatch.setattr(
        agent_module,
        "APPROVAL_TOOLS",
        ["mcp__approval__ask_human_approval"],
    )

    return agent_module.AgentService(
        config=cfg,
        taskQueue=asyncio.Queue(),
        pgClient=PostgresClientMock(), # tests that assert on calls swap in fake_pg().
        notifierSet=notifier,
        langfuse=LANGFUSE,
    )


def fake_event(session_id=""):
    return SimpleNamespace(
        eventID="alert-123",
        sessionID=session_id,
        describe=lambda: "ImagePullBackOff in default",
    )


def result_message(result="Issue diagnosed", is_error=False):
    return ResultMessage(
        subtype="success",
        duration_ms=100,
        duration_api_ms=80,
        is_error=is_error,
        num_turns=2,
        session_id="test-session",
        result=result,
        total_cost_usd=0.0,
    )


def report(status):
    return f'```json\n{{"alert": "PodImagePullError", "status": "{status}"}}\n```'


def fake_pg(approvedTools=None):
    return SimpleNamespace(
        eventStore=SimpleNamespace(
            insertSessionID=AsyncMock(),
            updateEventState=AsyncMock(),
        ),
        approvalStore=SimpleNamespace(
            getApprovedTools=AsyncMock(return_value=set(approvedTools or ())),
        ),
        claudeSessionStore=None,
    )


def statuses(pg):
    return [call.args for call in pg.eventStore.updateEventState.await_args_list]


def replay(monkeypatch, messages, error=None):
    async def fake_query(*, prompt, options):
        for message in messages:
            yield message
        if error is not None:
            raise error

    monkeypatch.setattr(agent_module, "query", fake_query)


def spans():
    """Finished Langfuse spans by name, each with its parent's name and its attributes."""
    LANGFUSE.flush()
    finished = EXPORTER.get_finished_spans()
    names = {s.context.span_id: s.name for s in finished}
    return {
        s.name: SimpleNamespace(
            parent=names.get(s.parent.span_id) if s.parent else None,
            type=s.attributes.get("langfuse.observation.type"),
            level=s.attributes.get("langfuse.observation.level"),
            status_message=s.attributes.get("langfuse.observation.status_message"),
            input=s.attributes.get("langfuse.observation.input"),
            output=s.attributes.get("langfuse.observation.output"),
            model=s.attributes.get("langfuse.observation.model.name"),
            session=s.attributes.get("session.id"),
        )
        for s in finished
    }


@pytest.mark.asyncio
async def test_handle_event(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    calls = []
    permissions = []
    tool_name = "mcp__kubemcp__rollout_restart"
    service.pgClient = pg = fake_pg({tool_name})

    async def fake_query(*, prompt, options):
        calls.append({
            "prompt": prompt,
            "options": options,
        })

        # Agent attempts tool call
        tool_use = ToolUseBlock(
            id="tool-1",
            name=tool_name,
            input={"namespace": "default", "name": "web"},
        )
        yield AssistantMessage(model="mock", content=[tool_use])

        # CLI asks permission before running the tool
        permissions.append(await options.can_use_tool(
            tool_use.name, tool_use.input, ToolPermissionContext(),
        ))

        # Tool returns result
        yield UserMessage(
            content=[
                ToolResultBlock(
                    tool_use_id="tool-1",
                    content="deployment.apps/web restarted",
                    is_error=False,
                )
            ]
        )

        # Agent analyzes result
        yield AssistantMessage(
            model="mock",
            content=[
                TextBlock(
                    text="Image pull failed. Check registry."
                )
            ],
        )

        # Final result
        yield result_message()

    monkeypatch.setattr(
        agent_module,
        "query",
        fake_query,
    )

    event = fake_event()
    await service._handle_event(event)

    # Agent should be called once
    assert len(calls) == 1

    prompt = calls[0]["prompt"]
    options = calls[0]["options"]

    assert "ImagePullBackOff" in prompt

    # New session
    assert options.resume is None
    assert options.session_id is not None
    pg.eventStore.insertSessionID.assert_awaited_once_with(
        "alert-123", options.session_id,
    )
    pg.approvalStore.getApprovedTools.assert_awaited_once_with(
        options.session_id,
    )
    assert len(permissions) == 1
    assert isinstance(permissions[0], PermissionResultAllow)

    # Approval MCP server registered
    assert (
        agent_module.APPROVAL_SERVER_NAME
        in options.mcp_servers
    )

    # Tool call, assistant text and result are recorded under the agent span.
    recorded = spans()
    assert set(recorded) == {"claude_agent", tool_name, "claude-agent", "claude-agent-result"}

    tool = recorded[tool_name]
    assert tool.type == "tool"
    assert tool.parent == "claude_agent"
    assert json.loads(tool.input) == {"namespace": "default", "name": "web"}
    assert tool.output == "deployment.apps/web restarted"
    assert tool.level is None

    generation = recorded["claude-agent"]
    assert generation.type == "generation"
    assert generation.parent == "claude_agent"
    assert generation.model == "mock"
    assert generation.output == "Image pull failed. Check registry."

    result = recorded["claude-agent-result"]
    assert result.type == "generation"
    assert result.parent == "claude_agent"
    assert result.output == "Issue diagnosed"
    assert result.status_message == Status.UNKNOWN

    assert {s.session for s in recorded.values()} == {options.session_id}

    # "Issue diagnosed" is no report, so the status is unknown.
    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.UNKNOWN),
    ]


@pytest.mark.asyncio
async def test_status_from_each_result(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        result_message("I've launched the investigator."),
        result_message(report("resolved")),
    ])

    await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.UNKNOWN),
        ("alert-123", Status.RESOLVED),
    ]


@pytest.mark.asyncio
async def test_approval_request_waits_for_human(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="mock", content=[
            ToolUseBlock(id="ask-1", name=ASK_HUMAN_APPROVAL_TOOL, input={}),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="ask-1", content="Approval requested", is_error=False),
        ]),
        result_message(report("waiting_human_approval")),
    ])

    await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.WAITING_HUMAN_APPROVAL),
    ]


@pytest.mark.asyncio
async def test_failed_tool_is_recorded_as_error(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="mock", content=[
            ToolUseBlock(id="ask-1", name=ASK_HUMAN_APPROVAL_TOOL, input={}),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="ask-1", content="No human could be notified", is_error=True),
        ]),
        result_message(report("failed_to_resolve"), is_error=True),
    ])

    await service._handle_event(fake_event())

    recorded = spans()
    assert recorded[ASK_HUMAN_APPROVAL_TOOL].level == "ERROR"
    assert recorded[ASK_HUMAN_APPROVAL_TOOL].output == "No human could be notified"
    assert recorded["claude-agent-result"].level == "ERROR"
    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.FAILED_TO_RESOLVE),
    ]


@pytest.mark.asyncio
async def test_subagent_nests_under_spawning_tool(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="opus", content=[
            ToolUseBlock(id="task-1", name="Task", input={"subagent_type": "diagnoser"}),
        ]),
        AssistantMessage(model="haiku", parent_tool_use_id="task-1", content=[
            TextBlock(text="Looking at pods."),
        ]),
        AssistantMessage(model="haiku", parent_tool_use_id="task-1", content=[
            ToolUseBlock(id="pods-1", name="mcp__kubemcp__pods_list", input={}),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="pods-1", content="web-0 CrashLoopBackOff"),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="task-1", content="web-0 is crash looping"),
        ]),
        result_message(report("resolved")),
    ])

    await service._handle_event(fake_event())

    recorded = spans()
    assert recorded["Task"].parent == "claude_agent"
    assert recorded["claude-subagent"].parent == "Task"
    assert recorded["claude-subagent"].model == "haiku"
    assert recorded["mcp__kubemcp__pods_list"].parent == "Task"
    assert recorded["Task"].output == "web-0 is crash looping"


@pytest.mark.asyncio
async def test_messages_without_text_record_no_generation(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="mock", content=[
            ToolUseBlock(id="tool-1", name="mcp__kubemcp__pods_list", input={}),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="tool-1", content="[]"),
        ]),
    ])

    await service._handle_event(fake_event())

    assert set(spans()) == {"claude_agent", "mcp__kubemcp__pods_list"}


@pytest.mark.asyncio
async def test_tool_without_result_is_closed_with_warning(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="mock", content=[
            ToolUseBlock(id="tool-1", name="mcp__kubemcp__pods_delete", input={"name": "web-0"}),
        ]),
    ], error=RuntimeError("CLI died"))

    with pytest.raises(RuntimeError):
        await service._handle_event(fake_event())

    tool = spans()["mcp__kubemcp__pods_delete"]
    assert tool.level == "WARNING"
    assert tool.status_message == "no tool result"


@pytest.mark.asyncio
async def test_resumed_session_is_solving_again(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [result_message(report("resolved"))])

    await service._handle_event(fake_event(session_id="session-1"))

    pg.eventStore.insertSessionID.assert_not_awaited()
    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.RESOLVED),
    ]
    # The resumed run joins the original run's Langfuse session.
    assert {s.session for s in spans().values()} == {"session-1"}


@pytest.mark.asyncio
async def test_crash_marks_event_failed(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [], error=RuntimeError("CLI died"))

    with pytest.raises(RuntimeError):
        await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.FAILED_TO_RESOLVE),
    ]


@pytest.mark.asyncio
async def test_crash_after_approval_request_keeps_waiting(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        result_message(report("waiting_human_approval")),
    ], error=RuntimeError("CLI died"))

    with pytest.raises(RuntimeError):
        await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.WAITING_HUMAN_APPROVAL),
    ]


@pytest.mark.asyncio
async def test_crash_after_later_status_marks_failed(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    # Only the latest status counts: a run that moved past waiting and then crashed failed.
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        result_message(report("waiting_human_approval")),
        result_message("still working"),
    ], error=RuntimeError("CLI died"))

    with pytest.raises(RuntimeError):
        await service._handle_event(fake_event())

    assert statuses(pg)[-1] == ("alert-123", Status.FAILED_TO_RESOLVE)


@pytest.mark.asyncio
async def test_crash_while_marking_failed_still_raises(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    pg.eventStore.updateEventState.side_effect = [None, ConnectionError("db down")]
    replay(monkeypatch, [], error=RuntimeError("CLI died"))

    with pytest.raises(RuntimeError, match="CLI died"):
        await service._handle_event(fake_event())


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (report("resolved"), Status.RESOLVED),
        ('{"status": "waiting_human_approval"}', Status.WAITING_HUMAN_APPROVAL),
        ('{"status": "converging"}', Status.UNKNOWN),
        ("I've launched the investigator.", Status.UNKNOWN),
        ("{not json}", Status.UNKNOWN),
        ('{"status": 3}', Status.UNKNOWN),
        ('["resolved"]', Status.UNKNOWN),
        ("", Status.UNKNOWN),
        (None, Status.UNKNOWN),
    ],
)
def test_report_status(result, expected):
    assert agent_module.report_status(result) == expected


@pytest.mark.asyncio
async def test_permission_read_allowed(service: agent_module.AgentService):
    sessionID = "session-1"
    service.pgClient = pg = fake_pg()
    callback = service._callback_for(
        require_approval=True,
        sessionID=sessionID,
    )
    for tool in KUBEMCP_READ_TOOLS:
        result = await callback(
            tool,
            {"namespace": "default"},
            ToolPermissionContext(),
        )
        assert isinstance(result, PermissionResultAllow)
    pg.approvalStore.getApprovedTools.assert_not_awaited()


@pytest.mark.asyncio
async def test_write_denied_without_approval(service: agent_module.AgentService):
    service.pgClient = pg = fake_pg()
    sessionID = "session-1"

    callback = service._callback_for(
        require_approval=True,
        sessionID=sessionID,
    )
    for tool in KUBEMCP_APPROVAL_TOOLS:
        result = await callback(
            tool,
            {"name": "my-pod"},
            ToolPermissionContext(),
        )

        assert isinstance(result, PermissionResultDeny)
        assert "awaiting human approval" in result.message

    getApprovedTools = pg.approvalStore.getApprovedTools
    assert getApprovedTools.await_count == len(KUBEMCP_APPROVAL_TOOLS)
    for call in getApprovedTools.await_args_list:
        assert call.args == (sessionID,)



@pytest.mark.asyncio
async def test_write_allowed_after_approval(service: agent_module.AgentService):
    sessionID = "session-1"
    service.pgClient = PostgresClientMock()
    service.pgClient.approvalStore.approvedTools[sessionID] = set()

    for tool in KUBEMCP_APPROVAL_TOOLS:
        service.pgClient.approvalStore.approvedTools[sessionID].add(tool)

        callback = service._callback_for(
            require_approval=True,
            sessionID=sessionID,
        )

        result = await callback(
            tool,
            {"name": "my-pod"},
            ToolPermissionContext(),
        )

        assert isinstance(result, PermissionResultAllow)
        assert result.updated_input == {"name": "my-pod"}
        service.pgClient.approvalStore.approvedTools[sessionID] = set()



@pytest.mark.asyncio
async def test_other_tool_denied(service: agent_module.AgentService):
    service.pgClient = pg = fake_pg()
    callback = service._callback_for(
        require_approval=True,
        sessionID="session-1",
    )

    result = await callback(
        "UnknownTool",
        {},
        ToolPermissionContext(),
    )

    assert isinstance(result, PermissionResultDeny)
    assert result.message == "tool not allowed. "


@pytest.mark.asyncio
async def test_approval_disabled(service: agent_module.AgentService):
    service.pgClient = pg = fake_pg()

    callback = service._callback_for(
        require_approval=False,
        sessionID="session-1",
    )

    result = await callback(
        "mcp__kubemcp__delete_pod",
        {"name": "my-pod"},
        ToolPermissionContext(),
    )

    assert isinstance(result, PermissionResultAllow)
    pg.approvalStore.getApprovedTools.assert_not_awaited()
