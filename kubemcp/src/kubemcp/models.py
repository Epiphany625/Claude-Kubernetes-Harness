"""Tool parameter types and result schemas.

Two halves:

* **Parameter aliases** -- ``Annotated`` types carrying both the JSON-Schema
  constraints the MCP SDK publishes to the caller *and* the semantic validators
  from :mod:`kubemcp.validation`. Declaring a parameter as ``Namespace`` is
  therefore enough to get it bounded, pattern-checked and documented; no tool
  has to remember to validate it. Inside a pydantic validator the convention is
  to raise ``ValueError``, which the SDK reports as an argument-validation
  failure -- an anticipated error the caller can correct, not a crash.

* **Result models** -- return annotations become the tool's ``outputSchema``,
  and the SDK validates every result against it before it leaves the server.
  The caller gets typed, checked data alongside the human-readable text.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from kubemcp import validation

# ---------------------------------------------------------------------------
# Parameter aliases
# ---------------------------------------------------------------------------


def _validator(func: Any) -> Any:
    """Adapt a validation.* function into a pydantic BeforeValidator.

    ``ValidationError`` subclasses ``ValueError``, which is exactly what
    pydantic expects, so the message reaches the caller intact.
    """
    return BeforeValidator(func)


def _optional(func: Any) -> Any:
    """Like :func:`_validator`, but lets ``None`` through untouched.

    Optional parameters still have to be validated when present -- an omitted
    namespace is fine, a malformed one is not.
    """

    def check(value: Any) -> Any:
        return value if value is None else func(value)

    return BeforeValidator(check)


ApiVersion = Annotated[
    str,
    _validator(validation.validate_api_version),
    Field(
        description=(
            "API group and version of the resource, e.g. 'v1' for core resources, "
            "'apps/v1' for Deployments, or a custom resource's group such as "
            "'ckh.io/v1alpha1'. Use list_api_resources to discover it."
        ),
        examples=["v1", "apps/v1"],
    ),
]

Kind = Annotated[
    str,
    _validator(validation.validate_kind),
    Field(
        description="Resource kind in PascalCase, e.g. 'Pod', 'Deployment', 'StatefulSet'.",
        examples=["Pod", "Deployment"],
    ),
]

ResourceName = Annotated[
    str,
    _validator(validation.validate_name),
    Field(description="Object name.", examples=["web-0"]),
]

Namespace = Annotated[
    str,
    _validator(validation.validate_namespace),
    Field(description="Namespace.", examples=["default"]),
]

OptionalNamespace = Annotated[
    str | None,
    _optional(validation.validate_namespace),
    Field(
        default=None,
        description=(
            "Namespace. Omit to use the server's default namespace. Ignored for "
            "cluster-scoped kinds such as Node or Namespace."
        ),
    ),
]

ContainerName = Annotated[
    str | None,
    _optional(validation.validate_container_name),
    Field(
        default=None,
        description=(
            "Container name. Omit when the pod has exactly one container; required "
            "when it has several."
        ),
    ),
]

LabelSelector = Annotated[
    str | None,
    _optional(validation.validate_label_selector),
    Field(
        default=None,
        description=(
            "Label selector, e.g. 'app=web', 'app in (web,api)', 'tier!=cache' or "
            "'!deprecated'. Comma-separated clauses are ANDed."
        ),
        examples=["app=web", "app in (web,api)"],
    ),
]

FieldSelector = Annotated[
    str | None,
    _optional(validation.validate_field_selector),
    Field(
        default=None,
        description=(
            "Field selector, e.g. 'status.phase=Running' or 'spec.nodeName=node-1'. "
            "Only '=', '==' and '!=' are supported."
        ),
        examples=["status.phase=Running"],
    ),
]

View = Annotated[
    Literal["summary", "full"],
    Field(
        default="summary",
        description=(
            "'summary' (default) returns the diagnostically useful fields and is far "
            "cheaper to read; 'full' returns the whole object minus server bookkeeping. "
            "Prefer 'summary' unless you need a specific field it omits."
        ),
    ),
]


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


class Result(BaseModel):
    """Base for every tool result."""

    model_config = ConfigDict(extra="forbid")


class ResourceRef(Result):
    """Identity of one object."""

    api_version: str
    kind: str
    name: str
    namespace: str | None = None


class ResourceResult(Result):
    """One resource."""

    api_version: str
    kind: str
    name: str
    namespace: str | None = None
    resource_version: str | None = None
    object: dict[str, Any] = Field(description="The object, under the requested view.")


class ResourceListResult(Result):
    """A page of resources."""

    api_version: str
    kind: str
    namespace: str | None = Field(
        default=None, description="Namespace listed, or null when listing cluster-wide."
    )
    count: int = Field(description="Number of items returned in this page.")
    items: list[dict[str, Any]]
    continue_token: str | None = Field(
        default=None,
        description=(
            "Pass back as 'continue_token' to fetch the next page. Null means this is "
            "the last page."
        ),
    )
    truncated: bool = Field(
        default=False,
        description=(
            "True when items were dropped to stay within the response budget. Narrow "
            "the query with a selector or a smaller limit rather than trusting a "
            "truncated list to be complete."
        ),
    )
    dropped: int = Field(default=0, description="How many items were dropped by truncation.")


class MutationResult(Result):
    """Outcome of a write."""

    action: str = Field(description="What was done: created, configured, deleted, scaled...")
    api_version: str
    kind: str
    name: str
    namespace: str | None = None
    resource_version: str | None = None
    dry_run: bool = False
    object: dict[str, Any] | None = Field(
        default=None, description="The resulting object, summarised."
    )


class LogResult(Result):
    """Container logs."""

    pod: str
    namespace: str
    container: str | None = None
    previous: bool = False
    lines: int
    truncated: bool = Field(
        default=False,
        description="True when the head was dropped to fit the budget; the tail is kept.",
    )
    logs: str


class EventItem(Result):
    """One event, flattened."""

    type: str | None = None
    reason: str | None = None
    message: str | None = None
    object: str | None = None
    namespace: str | None = None
    count: int | None = None
    first_seen: str | None = None
    last_seen: str | None = None


class EventListResult(Result):
    """Events, newest first."""

    namespace: str | None = None
    count: int
    items: list[EventItem]
    truncated: bool = False


class DescribeResult(Result):
    """An object together with the context needed to explain its state."""

    api_version: str
    kind: str
    name: str
    namespace: str | None = None
    summary: dict[str, Any] = Field(description="Projected view of the object.")
    events: list[EventItem] = Field(
        default_factory=list, description="Recent events for this object, newest first."
    )
    hints: list[str] = Field(
        default_factory=list,
        description="Plain-language observations about what looks wrong, if anything.",
    )


class ExecOutput(Result):
    """Result of running a command in a container."""

    pod: str
    namespace: str
    container: str | None = None
    command: list[str]
    exit_code: int | None = Field(
        default=None,
        description="Process exit status. Null when the apiserver reported none.",
    )
    stdout: str
    stderr: str
    timed_out: bool = False
    truncated: bool = False


class ApiResourceItem(Result):
    """One resource type served by the cluster."""

    kind: str
    api_version: str
    name: str
    namespaced: bool
    verbs: list[str]
    short_names: list[str] = Field(default_factory=list)
    subresources: list[str] = Field(default_factory=list)
    custom_resource: bool = False


class ApiResourceListResult(Result):
    """Everything this cluster serves."""

    count: int
    items: list[ApiResourceItem]


class ClusterInfoResult(Result):
    """Cluster version and node health."""

    version: str | None = None
    platform: str | None = None
    nodes_total: int = 0
    nodes_ready: int = 0
    nodes_not_ready: list[str] = Field(default_factory=list)


class MetricsItem(Result):
    """Resource usage for one node or pod."""

    name: str
    namespace: str | None = None
    cpu: str | None = None
    memory: str | None = None
    containers: list[dict[str, Any]] = Field(default_factory=list)


class MetricsResult(Result):
    """Live usage from metrics.k8s.io."""

    scope: Literal["nodes", "pods"]
    namespace: str | None = None
    count: int
    items: list[MetricsItem]
