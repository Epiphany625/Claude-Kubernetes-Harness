import json
import os
from pathlib import Path
from typing import *
from urllib.parse import quote

from . import prompts
from dataclasses import dataclass

# basic configs
CONFIG_FILE_PATH: Final = Path(__file__).with_name("config.json")
ENV_PREFIX: Final = "HARNESS"

# name of the agents
INVESTIGATOR: Final = "investigator"
EXECUTOR: Final = "executor"
VERIFIER: Final = "verifier"
AGENT_NAMES: Final = (INVESTIGATOR, EXECUTOR, VERIFIER)

@dataclass
class RabbitMQConfig:
    """Resolved RabbitMQ settings; timeouts are measured in seconds."""

    url: str
    exchange: str
    queue: str
    routingPrefix: str
    connectTimeout: float

@dataclass
class SubagentConfig:
    description: str
    prompt: str
    tools: List[str]
    model: str
    maxTurns: int
    effort: str
    mcpServers: List[str]
@dataclass
class AgentOptionsConfig:
    model: str
    system_prompt: str
    tools: List[str]
    allowed_tools: List[str]
    disallowed_tools: List[str]
    max_turns: int
    effort: str
    mcp_servers: Dict[Any, Any]
    require_approval: bool
    agents: Dict[str, SubagentConfig]

def load_value(key: str, default_value: Any = None) -> Any:
    """Look up a dotted key in the environment, then JSON, then the default.

    Environment names use an uppercase prefix and underscore-separated keys,
    e.g. HARNESS_AGENTS_INVESTIGATOR_MAXTURNS. Values are decoded as JSON when
    possible so lists, objects, numbers, booleans and null retain their types.
    Plain strings such as model names need no JSON quoting.
    """
    nested_keys = key.split(".")
    env_key = "_".join((ENV_PREFIX, *nested_keys)).upper()
    if env_key in os.environ:
        env_value = os.environ[env_key]
        try:
            return json.loads(env_value)
        except json.JSONDecodeError:
            return env_value

    try:
        with Path(CONFIG_FILE_PATH).open(encoding="utf-8") as config_file:
            value = json.load(config_file)
    except FileNotFoundError:
        value = {}

    if not isinstance(value, Mapping):
        raise ValueError("The harness config must contain a JSON object")

    for nested_key in nested_keys:
        if not isinstance(value, Mapping) or nested_key not in value:
            break
        value = value[nested_key]
    else:
        return value

    return default_value

def build_rabbitmq() -> RabbitMQConfig:
    """Build RabbitMQ settings with an escaped AMQP URL and fixed timeouts."""
    host = load_value("amqp.host", "")
    if not isinstance(host, str) or not host.strip():
        raise ValueError("amqp.host must be set")
    port = load_value("amqp.port", 5672)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("amqp.port must be an integer between 1 and 65535")

    username = str(load_value("amqp.username", ""))
    password = str(load_value("amqp.password", ""))
    vhost = str(load_value("amqp.vhost", "/"))
    credentials = ""
    if username:
        credentials = f"{quote(username, safe='')}:{quote(password, safe='')}@"
    # IPv6 literals need brackets when followed by a port.
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    url = f"amqp://{credentials}{host}:{port}/{quote(vhost, safe='')}"

    return RabbitMQConfig(
        url=url,
        exchange=load_value("amqp.exchange", "alerts"),
        queue=load_value("amqp.queue", "agent.events"),
        routingPrefix=load_value("amqp.routingPrefix", "alert"),
        connectTimeout=10.0,
    )


def build_harness() -> AgentOptionsConfig:
    """Build the orchestrator and its three subagents from harness config."""
    agent_prompts = {
        INVESTIGATOR: (prompts._INVESTIGATOR_DESCRIPTION, prompts._INVESTIGATOR_PROMPT),
        EXECUTOR: (prompts._EXECUTOR_DESCRIPTION, prompts._EXECUTOR_PROMPT),
        VERIFIER: (prompts._VERIFIER_DESCRIPTION, prompts._VERIFIER_PROMPT),
    }
    agents = {}
    for name in AGENT_NAMES:
        description, prompt = agent_prompts[name]
        agents[name] = SubagentConfig(
            description=description,
            prompt=prompt,
            tools=load_value(f"agents.{name}.tools", []),
            model=load_value(f"agents.{name}.model", "opus"),
            maxTurns=load_value(f"agents.{name}.maxTurns", 20),
            effort=load_value(f"agents.{name}.effort", "high"),
            mcpServers=load_value(f"agents.{name}.mcpServers", []),
        )

    return AgentOptionsConfig(
        model=load_value("orchestrator.model", "opus"),
        system_prompt=prompts._ORCHESTRATOR_PROMPT,
        tools=load_value("orchestrator.tools"),
        allowed_tools=load_value("orchestrator.allowedTools", ["Agent", "Task"]),
        disallowed_tools=load_value(
            "orchestrator.disallowedTools", ["Bash", "Write", "Edit", "NotebookEdit"]
        ),
        max_turns=load_value("orchestrator.maxTurns", 30),
        effort=load_value("orchestrator.effort", "high"),
        mcp_servers=load_value("mcpServers", {}),
        require_approval=load_value("policies.requireapproval", False),
        agents=agents,
    )

@dataclass
class HarnessConfig:
    rabbitMQConfig: RabbitMQConfig
    agentOptionsConfig: AgentOptionsConfig

def load_config() -> HarnessConfig:
    return HarnessConfig(
        rabbitMQConfig=build_rabbitmq(), 
        agentOptionsConfig=build_harness(),
    )
