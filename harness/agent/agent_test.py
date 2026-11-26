
import asyncio
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from claude_agent_sdk import ClaudeAgentOptions

import config.config as config

import pytest
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


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch):
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
        pgClient=None, # will be defined in individual tests. 
        notifierSet=notifier,
    )


def fake_event(session_id=""):
    return SimpleNamespace(
        eventID="alert-123",
        sessionID=session_id,
        describe=lambda: "ImagePullBackOff in default",
    )


def result_message(result="Issue diagnosed"):
    return ResultMessage(
        subtype="success",
        duration_ms=100,
        duration_api_ms=80,
        is_error=False,
        num_turns=2,
        session_id="test-session",
        result=result,
        total_cost_usd=0.0,
    )


def report(outcome):
    return f'```json\n{{"alert": "PodImagePullError", "outcome": "{outcome}"}}\n```'


def fake_pg():
    return SimpleNamespace(
        insertSessionID=AsyncMock(),
        updateEventState=AsyncMock(),
        getApprovedTools=AsyncMock(return_value=set()),
    )


def statuses(pg):
    return [call.args for call in pg.updateEventState.await_args_list]


def replay(monkeypatch, messages, error=None):
    async def fake_query(*, prompt, options):
        for message in messages:
            yield message
        if error is not None:
            raise error

    monkeypatch.setattr(agent_module, "query", fake_query)



@pytest.mark.asyncio
async def test_handle_event(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    calls = []
    permissions = []
    tool_name = "mcp__kubemcp__rollout_restart"
    pg = SimpleNamespace(
            insertSessionID=AsyncMock(),
            updateEventState=AsyncMock(),
            getApprovedTools=AsyncMock(return_value={tool_name}),
    )
    service.pgClient = pg

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
    service.pgClient.insertSessionID.assert_awaited_once_with(
        "alert-123", options.session_id,
    )
    service.pgClient.getApprovedTools.assert_awaited_once_with(
        options.session_id,
    )
    assert len(permissions) == 1
    assert isinstance(permissions[0], PermissionResultAllow)

    # Approval MCP server registered
    assert (
        agent_module.APPROVAL_SERVER_NAME
        in options.mcp_servers
    )

    # Verify tool call and response logging
    logs = [
        str(call.args[0])
        for call in service.logger.info.call_args_list
    ]

    assert any(
        f"[tool] {tool_name}" in log
        for log in logs
    )

    assert any(
        f"[ok] {tool_name}" in log
        for log in logs
    )

    assert any(
        "Issue diagnosed" in log
        for log in logs
    )

    # "Issue diagnosed" is no report, so the event counts as not resolved.
    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.FAILED_TO_RESOLVE),
    ]


@pytest.mark.asyncio
async def test_resolved_outcome_from_last_result(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        result_message("I've launched the investigator."),
        result_message(report("resolved")),
    ])

    await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
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
        result_message(report("awaiting_approval")),
    ])

    await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.WAITING_HUMAN_APPROVAL),
    ]


@pytest.mark.asyncio
async def test_failed_approval_request_is_not_waiting(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [
        AssistantMessage(model="mock", content=[
            ToolUseBlock(id="ask-1", name=ASK_HUMAN_APPROVAL_TOOL, input={}),
        ]),
        UserMessage(content=[
            ToolResultBlock(tool_use_id="ask-1", content="No human could be notified", is_error=True),
        ]),
        result_message(report("escalated")),
    ])

    await service._handle_event(fake_event())

    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.FAILED_TO_RESOLVE),
    ]


@pytest.mark.asyncio
async def test_resumed_session_is_solving_again(service: agent_module.AgentService, monkeypatch: pytest.MonkeyPatch):
    service.pgClient = pg = fake_pg()
    replay(monkeypatch, [result_message(report("resolved"))])

    await service._handle_event(fake_event(session_id="session-1"))

    pg.insertSessionID.assert_not_awaited()
    assert statuses(pg) == [
        ("alert-123", Status.SOLVING),
        ("alert-123", Status.RESOLVED),
    ]


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


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (report("resolved"), "resolved"),
        ('{"outcome": "converging"}', "converging"),
        ("I've launched the investigator.", None),
        ("{not json}", None),
        ('{"outcome": 3}', None),
        ("", None),
        (None, None),
    ],
)
def test_report_outcome(result, expected):
    assert agent_module.report_outcome(result) == expected


@pytest.mark.asyncio
async def test_permission_read_allowed(service: agent_module.AgentService):
    sessionID = "session-1"
    pg = SimpleNamespace(
            insertSessionID=AsyncMock(),
            getApprovedTools=AsyncMock(return_value=[]),
    )
    service.pgClient = pg
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
    service.pgClient.getApprovedTools.assert_not_awaited()


@pytest.mark.asyncio
async def test_write_denied_without_approval(service: agent_module.AgentService):
    pg = SimpleNamespace(
            insertSessionID=AsyncMock(),
            getApprovedTools=AsyncMock(return_value=[]),
        )
    service.pgClient = pg
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

    getApprovedTools = service.pgClient.getApprovedTools
    assert getApprovedTools.await_count == len(KUBEMCP_APPROVAL_TOOLS)
    for call in getApprovedTools.await_args_list:
        assert call.args == (sessionID,)



@pytest.mark.asyncio
async def test_write_allowed_after_approval(service: agent_module.AgentService):
    sessionID = "session-1"
    service.pgClient = PostgresClientMock()
    service.pgClient.approvedTools[sessionID] = set()
    
    for tool in KUBEMCP_APPROVAL_TOOLS:
        service.pgClient.approvedTools[sessionID].add(tool)

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
        service.pgClient.approvedTools[sessionID] = set()



@pytest.mark.asyncio
async def test_other_tool_denied(service: agent_module.AgentService):
    pg = SimpleNamespace(
            insertSessionID=AsyncMock(),
            getApprovedTools=AsyncMock(return_value=[]),
        )
    service.pgClient = pg
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
    pg = SimpleNamespace(
            insertSessionID=AsyncMock(),
            getApprovedTools=AsyncMock(return_value=[]),
        )
    service.pgClient = pg
    
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
    service.pgClient.getApprovedTools.assert_not_awaited()
