---
name: add-mcp-tool
description: Add a new tool to the kubemcp server, or change an existing one. Use whenever work involves exposing new Kubernetes capability through MCP, editing anything in src/kubemcp/tools/, or adding a parameter or result field to a tool. Covers the parameter/result/registration/test sequence and the invariants a tool must satisfy.
---

# Adding a tool to kubemcp

A tool is not finished when it returns the right data. It is finished when a
language model that has never seen this codebase can pick it correctly, call it
correctly, and recover when it fails.

Work in this order. Each step has a rule that exists because breaking it caused
a real bug.

## 1. Decide whether it should be a tool at all

The generic tools (`list_resources`, `get_resource`, `apply_resource`,
`patch_resource`, …) already reach **every** resource the cluster serves,
including CRDs. Before adding anything, check the new capability is not already
expressible as a generic call.

Add a dedicated tool only when it earns its place:

- the shape genuinely differs (logs, exec, metrics are subresources with their
  own semantics, not objects), **or**
- it saves the caller a sequence of round trips it would otherwise hand-assemble
  (`describe_resource` folds object + events + analysis into one), **or**
- it is error-prone to construct by hand (`rollout_restart` stamps a specific
  annotation; a model would get it subtly wrong).

Every tool's schema sits in the caller's context on *every* request. A tool that
duplicates a generic call makes every other call more expensive.

## 2. Parameters — from `models.py`, never hand-rolled

```python
from kubemcp.models import ApiVersion, Kind, Namespace, OptionalNamespace, View
```

These `Annotated` aliases carry the JSON-Schema constraints published to the
caller *and* the semantic validators. Declaring `namespace: Namespace` is what
makes it validated; there is nothing to remember.

Need a new kind of parameter?

1. Write the rule in `validation.py` as a `validate_*` function that raises
   `ValidationError` with a message naming the offending value and the rule it
   broke ("namespace 'Foo' must be lowercase", not "invalid input").
2. Wrap it as an alias in `models.py` with `_validator(...)`, or `_optional(...)`
   when `None` is allowed.
3. Add a valid/invalid table test in `tests/unit/test_validation.py`.

Never call `validate_*` in a tool body when an alias would do — that is how a
parameter ends up validated on one path and not another.

Bound every number with `Field(ge=..., le=...)`. Use `Literal[...]` for enums.

## 3. Result — a Pydantic model in `models.py`

The return annotation becomes the tool's `outputSchema` and the SDK validates
every result against it, so the caller gets typed data alongside readable text.
Result models set `extra="forbid"`; when building one from a projection, filter
to the model's fields (see `_event_item` in `tools/workloads.py`) so the shaping
layer stays free to add keys.

## 4. Register it

Put it in the `tools/` module that matches its concern, inside that module's
`register(mcp)`. A new module must be added to `_MODULES` in
`tools/__init__.py`.

```python
@mcp.tool(
    title="Human readable",
    annotations=ToolAnnotations(
        read_only_hint=True,  # does not change cluster state
        destructive_hint=False,  # MUST be True for delete/exec-shaped tools
        idempotent_hint=True,
    ),
    description="...",
)
async def my_tool(ctx: Context[AppContext], name: ResourceName) -> MyResult:
    kube = kube_of(ctx)
    settings = settings_of(ctx)  # never kube.settings
    ...
```

`annotations` is how a host decides what to gate. Getting `destructive_hint`
wrong on a mutating tool is a real safety bug, not a documentation slip.

## 5. Write the description as a prompt

It is read by a model deciding what to do next, not by someone browsing docs.
Include:

- **when to reach for it**, relative to the alternatives
  — *"prefer describe_resource when diagnosing, since it also returns events"*
- **the non-obvious argument that matters**
  — *"for a crash-looping pod, pass previous=true: the current container has not
    produced the failure yet"*
- **what the caller should do with a degraded result**
  — *"a result marked truncated is incomplete; narrow the query"*

## 6. Go through `KubeClient`

Call `kube.get/create/apply/patch/delete/request`, always with an `action=`
string describing the attempt in plain words (`f"get {kind} {ns}/{name}"`). It
becomes the subject of the error message, and it is the difference between
"Not found: cannot get Pod default/web-0" and an unexplained failure.

Never touch `kube.dynamic` directly: `KubeClient` is where the concurrency
ceiling, timeout, error mapping and the `Status`-body check are applied.

## 7. Shape the output

Anything derived from a cluster object goes through `shape()` / `project()`.
Cap lists with `shape_items()` and report `truncated`/`dropped`. If the new kind
deserves a projection, add it to `_PROJECTIONS` in `shaping.py` with a unit
test; the generic fallback already handles CRD-shaped objects.

## 8. Test both layers

- `tests/unit/` — any new pure function (validator, projection, hint).
- `tests/integration/` — the tool through the real MCP client, against the fake
  apiserver. At minimum: the happy path, one invalid-input case asserting the
  error text is useful, and one apiserver-failure case
  (`cluster.fail_forbidden(...)`).

If the tool hits an endpoint `tests/fake_apiserver.py` does not serve yet, add
it there — and make it behave like the real apiserver, including its refusals.
The fake accepting something the real one rejects is exactly how the `Accept:
text/plain` log bug survived until the e2e run.

## 9. Verify

```bash
uv run ruff format . && uv run ruff check --fix . && uv run mypy src
uv run pytest                 # must pass with NO cluster running
uv run pytest -m e2e          # if a cluster is available
```

Then confirm the tool surface still reads well end to end:

```bash
uv run python -c "
import asyncio
from kubemcp.server import build_server
from kubemcp.config import Settings
async def main():
    for t in await build_server(Settings(_env_file=None)).list_tools():
        print(f'{t.name:22} {(t.description or \"\")[:70]}')
asyncio.run(main())"
```
