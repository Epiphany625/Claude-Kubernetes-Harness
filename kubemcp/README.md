# kubemcp

An [MCP](https://modelcontextprotocol.io) server that gives AI agents access to a
Kubernetes cluster: reads, writes, and the diagnostics needed to work out why
something is broken.

It is the cluster-access layer of the Claude Kubernetes Harness. The `harness/`
service runs an agent loop in a pod and calls this server over the network to see
and change cluster state; `operator/` owns the CKH CRD, which these tools reach
without any code here naming it.

**It does exactly one job: serve MCP requests.** It performs no agent
authorization — deciding whether a given agent may take a given action belongs
elsewhere. The real enforcement boundary is the RBAC bound to its ServiceAccount,
which `ops/` defines.

---

## Quickstart

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                 # install
uv run kubemcp          # serve streamable HTTP on 0.0.0.0:8080/mcp
```

Credentials are resolved in this order: `KUBEMCP_KUBECONFIG` if set → the pod's
ServiceAccount token when running in-cluster → `~/.kube/config`. If none works
the server exits at startup with a message naming what it tried and what to set,
rather than a traceback.

Talk to it:

```python
import asyncio
from mcp import Client


async def main():
    async with Client("http://127.0.0.1:8080/mcp") as client:
        result = await client.call_tool(
            "describe_resource",
            {
                "api_version": "v1",
                "kind": "Pod",
                "name": "web-0",
                "namespace": "default",
            },
        )
        print(result.structured_content)


asyncio.run(main())
```

For a local MCP client that speaks stdio instead:

```bash
KUBEMCP_TRANSPORT=stdio uv run kubemcp
```

---

## Tools

Resources are addressed by `apiVersion` + `kind` through the dynamic client, so
one small set of tools covers everything the cluster serves — built-in resources
and custom resources alike. A CRD becomes reachable the moment it is registered.

| Tool | Kind | Required arguments |
|---|---|---|
| `list_api_resources` | read | — |
| `get_cluster_info` | read | — |
| `list_resources` | read | `api_version`, `kind` |
| `get_resource` | read | `api_version`, `kind`, `name` |
| `describe_resource` | read | `api_version`, `kind`, `name` |
| `get_pod_logs` | read | `name` |
| `list_events` | read | — |
| `get_top_metrics` | read | — |
| `create_resource` | write | `manifest` |
| `apply_resource` | write | `manifest` |
| `patch_resource` | write | `api_version`, `kind`, `name`, `patch` |
| `scale_resource` | write | `api_version`, `kind`, `name`, `replicas` |
| `rollout_restart` | write | `kind`, `name` |
| `delete_resource` | **destructive** | `api_version`, `kind`, `name` |
| `exec_in_pod` | **destructive** | `name`, `command` |

Every tool carries MCP `ToolAnnotations` (`read_only_hint`, `destructive_hint`,
`idempotent_hint`) so a host can gate the destructive ones without parsing names.
Every mutating tool accepts `dry_run=true`.

Three worth knowing about:

- **`describe_resource`** is the diagnostic entry point. It returns the object,
  its recent events, and plain-language observations — *"Container 'app' is
  crash-looping (7 restarts). The reason it exited is in the previous
  container's logs: call get_pod_logs with previous=true"* — in one round trip
  instead of three.
- **`get_pod_logs`** takes `previous=true`, which is the only way to see why a
  CrashLoopBackOff pod died: the current container hasn't produced the failure yet.
- **`exec_in_pod`** takes an **argv list**, never a shell string. The apiserver
  execs it directly ("Not executed within a shell"), so `;`, `|` and `$()` carry
  no meaning anywhere in the path. Ask for a shell explicitly if you want one:
  `["sh", "-c", "ps aux | head"]`.

### Deliberate non-goals

`watch` and `port_forward` are not implemented. Both are long-lived and stateful,
which fits badly in a request/response tool and worse behind a stateless,
horizontally-scaled service. An agent can poll `list_resources` instead.

### Responses are shaped for a language model

A raw Pod is 10–15k tokens, mostly `managedFields` and a
`last-applied-configuration` annotation restating the whole spec. Left alone, a
few list calls bury the caller's context. So:

- `managedFields` and `last-applied-configuration` are always stripped.
- `view="summary"` (the default) projects each kind down to the fields someone
  would actually look at. Measured against a live cluster, a summarised Pod is
  **9% the size** of the full object. `view="full"` returns the pruned whole object.
- Every result is byte-budgeted, and truncation is *always reported*
  (`truncated`, `dropped`). A silently short list reads as "that's everything",
  which is worse than saying the result is incomplete.

### Errors are written to be acted on

Kubernetes failures are translated into messages the caller can do something
with. A 403 names the verb and resource that were refused, which is exactly what
the harness needs to report a missing RBAC rule:

```
Forbidden: cannot list Pod in namespace prod. The apiserver refused this request
under RBAC: pods is forbidden: User "system:serviceaccount:ckh:kubemcp" cannot
list resource "pods" in API group "" in the namespace "prod". The ServiceAccount
this server runs as needs permission for resource 'pods' in API group 'core'.
This server does not grant permissions; add the rule to the Role/ClusterRole
bound to its ServiceAccount.
```

A 409 on apply suggests `force_conflicts`; a 410 explains the stale continue
token; a 404 names the object and points at `list_api_resources`.

---

## Configuration

All settings are environment variables prefixed `KUBEMCP_`. List values accept
either a comma-separated string or JSON, so Helm values work directly.

| Variable | Default | Purpose |
|---|---|---|
| `KUBEMCP_TRANSPORT` | `streamable-http` | `streamable-http` or `stdio`. |
| `KUBEMCP_HOST` | `0.0.0.0` | Bind address. Must not be `127.0.0.1` in a container. |
| `KUBEMCP_PORT` | `8080` | Listen port. |
| `KUBEMCP_MCP_PATH` | `/mcp` | Path the MCP endpoint is served at. |
| **`KUBEMCP_ALLOWED_HOSTS`** | *(empty = any)* | **Host header allowlist — see below.** |
| `KUBEMCP_ALLOWED_ORIGINS` | *(empty = any)* | Origin allowlist; only matters for browser callers. |
| `KUBEMCP_KUBECONFIG` | *(unset)* | Path to a kubeconfig. Unset → in-cluster, then `~/.kube/config`. |
| `KUBEMCP_KUBE_CONTEXT` | *(unset)* | kubeconfig context. Ignored in-cluster. |
| `KUBEMCP_DEFAULT_NAMESPACE` | `default` | Namespace used when a call omits one. |
| `KUBEMCP_REQUEST_TIMEOUT_SECONDS` | `30` | Per-request apiserver timeout. |
| `KUBEMCP_MAX_CONCURRENT_REQUESTS` | `16` | In-flight apiserver requests, so one agent can't swamp it. |
| `KUBEMCP_MAX_RESPONSE_BYTES` | `96000` | Byte ceiling on one tool result. |
| `KUBEMCP_MAX_LOG_BYTES` | `64000` | Byte ceiling on a log fetch. |
| `KUBEMCP_MAX_EXEC_OUTPUT_BYTES` | `32000` | Byte ceiling on exec output. |
| `KUBEMCP_MAX_LIST_ITEMS` | `200` | Hard ceiling on items from a list call. |
| `KUBEMCP_EXEC_TIMEOUT_SECONDS` | `60` | Wall-clock ceiling on one exec. |
| `KUBEMCP_FIELD_MANAGER` | `kubemcp` | Field manager recorded for server-side apply. |
| `KUBEMCP_LOG_LEVEL` | `INFO` | `DEBUG`…`CRITICAL`. |

### ⚠️ `KUBEMCP_ALLOWED_HOSTS` — the most likely cause of a failed first deploy

The MCP streamable-HTTP transport rejects unrecognised `Host` headers as
DNS-rebinding protection. In a cluster the Host header is the Service DNS name,
so unless it is allowlisted **every request from the harness is rejected** — the
pod looks healthy and serves nothing.

```yaml
env:
  - name: KUBEMCP_ALLOWED_HOSTS
    value: "kubemcp.ckh-system.svc.cluster.local:8080,kubemcp:8080"
```

Leaving it empty means "accept any Host", which is fine for local development;
the server logs a warning at startup so it is never silently the case in
production.

### Health probes

`/healthz` and `/readyz` are plain HTTP, outside the MCP protocol, so a kubelet
can call them without speaking MCP.

- `/healthz` — liveness. Never touches the apiserver: if it did, one control-plane
  blip would restart every replica at once.
- `/readyz` — readiness. Pings the apiserver, so a replica that has lost the
  control plane stops receiving traffic.

### Scaling

The server runs stateless (`stateless_http=True`, `json_response=True`): every
request is self-contained, so replicas sit behind an ordinary Service with **no
session affinity required**.

### RBAC

The server holds no permissions of its own — it can do exactly what its
ServiceAccount can. For the full tool surface, the ClusterRole in `ops/` needs:

```yaml
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods/log", "pods/exec"]
    verbs: ["get", "create"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["nodes", "pods"]
    verbs: ["get", "list"]
```

Narrow this to taste: anything the ServiceAccount cannot do simply returns a 403
that names the missing permission. Restricting the blast radius belongs here, in
RBAC — not in the server.

---

## Development

```bash
uv sync                                    # install with dev dependencies
uv run pytest                              # unit + integration (NO cluster needed)
uv run pytest -m e2e                       # live-cluster suite
uv run pytest --cov=kubemcp --cov-report=term-missing
uv run ruff format . && uv run ruff check --fix .
uv run mypy src                            # strict mode, kept clean
```

### Tests

274 unit and integration tests run with **no cluster running**, plus 6 e2e tests
that need one.

| Suite | What it exercises | Needs a cluster |
|---|---|---|
| `tests/unit/` | Validators, shaping/projection/truncation, error mapping, config. Pure functions. | no |
| `tests/integration/` | The **real MCP protocol** (in-process `Client`: initialize → tools/list → tools/call) → the **real** `kubernetes_asyncio` client → a **real** local HTTP server that behaves like an apiserver. | no |
| `tests/e2e/` | The same tools against a live cluster: apply, describe, logs, exec with exit codes, delete. Marked `e2e`, deselected by default. | yes |

The integration layer fakes only the cluster's *contents*, not the client: path
construction, query-parameter encoding, serialization and error handling all run
exactly as they would in production. (`aioresponses` doesn't support aiohttp 3.14,
and a real server turned out to be the better tool anyway — it can reproduce the
apiserver's *refusals*, which is where the interesting bugs live.)

Running e2e:

```bash
minikube start
uv run pytest -m e2e
```

Each e2e test creates a throwaway namespace and deletes it afterwards, whatever
the outcome.

### Docker

```bash
docker build -t kubemcp:dev .
docker run --rm -p 8080:8080 \
  -e KUBEMCP_ALLOWED_HOSTS="127.0.0.1:8080,localhost:8080" \
  -e KUBEMCP_KUBECONFIG=/kube/config \
  -v "$HOME/.kube:/kube:ro" \
  kubemcp:dev

curl localhost:8080/healthz
```

Multi-stage build: `uv sync --frozen --no-dev` into a virtualenv, then a
`python:3.12-slim` runtime carrying only that venv and `src/`. Runs as
non-root UID 65532. No BuildKit-only syntax, so it builds with any Docker.

In-cluster, drop the kubeconfig mount entirely — the ServiceAccount token is
picked up automatically.

---

## How it is built

| Concern | Choice | Why |
|---|---|---|
| MCP | [`mcp`](https://pypi.org/project/mcp/) 2.2 (official Python SDK) | Type hints become the tool schema; Pydantic return types become the `outputSchema` and are validated before results leave the server. |
| Kubernetes | [`kubernetes-asyncio`](https://pypi.org/project/kubernetes-asyncio/) 36.1 | Native async, so a concurrent server doesn't depend on a thread pool. Ships the dynamic client (any GVK, including CRDs), websocket exec, and in-cluster config. |
| Validation | Pydantic 2 + `validation.py` | Constraints are published in the tool schema *and* enforced server-side. |
| Transport | Starlette + uvicorn | What the SDK's streamable-HTTP app is built on. |
| Tooling | uv, ruff, mypy (strict), pytest + anyio | anyio rather than pytest-asyncio because the SDK is anyio-based; mixing them causes event-loop mismatches. |

### Code map

```
src/kubemcp/
  config.py       KUBEMCP_* settings
  server.py       MCPServer construction, lifespan, health routes, transport
  validation.py   every input rule — the server's only guard
  shaping.py      pruning, per-kind projections, truncation
  models.py       Annotated parameter types + result schemas
  k8s/            client, discovery, error mapping, logs, exec
  tools/          the 15 tools, grouped by concern
```

Four invariants hold throughout, each because breaking it produced a real bug:
validation lives in the parameter type rather than the tool body; every apiserver
call goes through `KubeClient`; every response goes through `shaping.py`; tool
configuration comes from the request context, not the client. `CLAUDE.md` explains
each, along with the upstream `kubernetes_asyncio` quirks worked around in-tree.

### Working on this with Claude Code

`CLAUDE.md` carries the architecture, the invariants, and — prominently — the
MCP SDK 1.x→2.x API delta, since an agent working from a stale prior will
confidently write `FastMCP` and `get_context()`.

`.claude/` adds:

- **skills** — `add-mcp-tool` (the recipe for adding a tool correctly) and
  `debug-k8s-api` (reproducing an apiserver interaction and turning it into a
  permanent test).
- **hooks** — ruff format + autofix on each Python file written; a guard that
  blocks destructive `kubectl`/`helm` commands when the current context is not a
  local disposable cluster; and the unit suite on stop.
