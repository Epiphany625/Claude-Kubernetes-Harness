# kubemcp — working notes

An MCP server that serves Kubernetes reads, writes and diagnostics to AI agents.
The consumer is the `harness/` service (Claude Agent SDK), calling over the
network from another pod.

## ⚠️ The MCP Python SDK is 2.x — your training prior is probably 1.x

This is the single most common way to break this codebase. The API changed at
2.0, and the old names are confidently wrong rather than missing:

| Don't write | Write |
|---|---|
| `from mcp.server.fastmcp import FastMCP` | `from mcp.server import MCPServer` |
| `FastMCP("name")` | `MCPServer(name="...")` |
| `ctx = server.get_context()` | declare `ctx: Context[AppContext]` as a tool parameter |
| `FastMCPError` | `MCPServerError` / `ToolError` |
| `result.isError`, `tool.inputSchema` | `result.is_error`, `tool.input_schema` (snake_case) |
| `MCPServer(port=..., host=...)` | pass those to `run()` / `streamable_http_app()` |
| `ToolAnnotations(readOnlyHint=True)` | `ToolAnnotations(read_only_hint=True)` |

Exception imports: `from mcp.server.mcpserver.exceptions import ToolError`,
`from mcp.shared.exceptions import MCPError`.

Docs: <https://py.sdk.modelcontextprotocol.io/>. Verify against the installed
wheel before trusting recall — `uv run python -c "import mcp, inspect; ..."`.

## Commands

```bash
uv sync                                   # install, including dev deps
uv run kubemcp                            # run (streamable HTTP on :8080)
uv run pytest                             # unit + integration; NO cluster needed
uv run pytest -m e2e                      # live-cluster suite (needs minikube/kind)
uv run ruff format . && uv run ruff check --fix .
uv run mypy src                           # strict; must stay clean
docker build -t kubemcp:dev .             # plain Docker, no BuildKit needed
```

`uv run pytest` must pass with no cluster running. If a change makes the
ordinary suite require a cluster, the change is wrong, not the suite.

## Layout

```
src/kubemcp/
  config.py       KUBEMCP_* settings (pydantic-settings)
  server.py       MCPServer construction, lifespan, /healthz + /readyz, transport
  validation.py   every input rule  ← the server's only guard
  shaping.py      pruning, per-kind projections, truncation
  models.py       Annotated parameter types + result schemas
  context.py      AppContext (what the lifespan yields)
  k8s/            client, discovery, error mapping, logs, exec
  tools/          the 15 tools, grouped by concern
tests/
  fake_apiserver.py   a real aiohttp server that behaves like an apiserver
  unit/               pure functions, no I/O
  integration/        real MCP protocol → real k8s client → fake apiserver
  e2e/                real cluster, marked `e2e`, deselected by default
```

## Invariants

Four rules hold everywhere. Each exists because breaking it produced a real bug.

1. **Validation lives in the parameter type, not the tool body.** Declare
   parameters with the `Annotated` aliases in `models.py` (`Namespace`, `Kind`,
   `LabelSelector`, …). They carry both the published JSON-Schema constraints and
   the semantic validators, so a tool cannot forget to validate.
   `ValidationError` subclasses *both* `ValueError` (pydantic) and `ToolError`
   (the SDK forwards it verbatim) — anything else raised in a tool body is
   masked to "Error executing tool X" and the caller learns nothing.

2. **Every apiserver call goes through `KubeClient`.** It applies the
   concurrency ceiling, the timeout, the error mapping, *and*
   `raise_for_status()`. That last one is not optional: `kubernetes_asyncio`'s
   dynamic client issues requests with `_preload_content=False`, and the REST
   layer only checks the HTTP status when that flag is true — so it returns the
   apiserver's `Status` error body as though it were the object you asked for. A
   404 would otherwise reach the caller as a *successful* result describing a
   nonexistent object.

3. **Every response goes through `shaping.py`.** The caller is a language model;
   a raw Pod is 10–15k tokens of `managedFields`. Summary view runs about 9% the
   size of full. Truncation is always reported (`truncated`, `dropped`) — a
   silently short list reads as "that's everything".

4. **Tool config comes from `settings_of(ctx)`, never `kube.settings`.** The
   client owns transport concerns only. Reading tool config off the client let a
   differently-configured client override the server's default namespace, which
   sent a write to the wrong namespace.

## Adding a tool

Use the `add-mcp-tool` skill in `.claude/skills/`. In short: parameters from
`models.py` aliases → a result model → register in the right `tools/` module →
`ToolAnnotations` (destructive work *must* be flagged) → a unit test for any new
pure logic and an integration test through the MCP client.

Tool descriptions are prompts, not docs. Say when to reach for the tool and what
to do next, the way `describe_resource` and `get_pod_logs` do.

## Known upstream quirks

Each of these cost real debugging time; they are worked around in-tree:

- `LazyDiscoverer.__aiter__` iterates a dict without `.values()`, yielding
  **strings** instead of `Resource` objects. `k8s/discovery.py` reads the
  discovery endpoints directly instead.
- The dynamic client never raises on HTTP errors — see invariant 2.
- `WsApiClient` with `_preload_content=True` merges stdout and stderr and drops
  the error channel, losing the exit code. `k8s/exec.py` drives the websocket
  itself to keep the three channels apart.
- The pod log subresource answers **406** to `Accept: text/plain`; send `*/*`.
  Only the live e2e run caught this, so the fake apiserver now reproduces it.
- `aioresponses` does not support aiohttp 3.14, which is why tests use a real
  local aiohttp server rather than an HTTP mock.

## Deployment gotchas

- **Host allowlist.** The transport's DNS-rebinding protection rejects unknown
  `Host` headers. In-cluster the Host is the Service DNS name, so
  `KUBEMCP_ALLOWED_HOSTS` must include it or *every* request 4xxs.
- **No built-in health endpoint.** `/healthz` and `/readyz` are `custom_route`s.
  Liveness must never touch the apiserver, or one control-plane blip restarts
  every replica at once. Readiness reads the client through the `_Runtime`
  holder the lifespan fills in — without that handoff `/readyz` answers
  "starting" forever and the pod never receives traffic.

## Out of scope

- **No permission checks.** Whether a given agent may take an action is decided
  elsewhere; this server only serves MCP requests. The ServiceAccount's RBAC is
  the real boundary and lives in `ops/`. Do not add an authorization layer here.
- **No `watch` or `port_forward`.** Long-lived and stateful, which fits badly in
  a request/response tool and worse behind a stateless, scaled service.
