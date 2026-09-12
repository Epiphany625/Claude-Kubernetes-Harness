"""Runtime configuration, sourced from ``KUBEMCP_*`` environment variables.

Everything here is deployment wiring: where to listen, which hostnames to accept,
how hard to lean on the apiserver, and how much output a single tool call may
return. Nothing here decides *who* may call a tool -- that is not this server's
job (see README, "Non-goals").
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

Transport = Literal["streamable-http", "stdio"]

# pydantic-settings JSON-decodes complex fields straight from the environment,
# before any validator runs, so `KUBEMCP_ALLOWED_HOSTS=a.svc,b.svc` would fail
# to parse. NoDecode suppresses that and hands the raw string to the validator
# below -- which matters because environment variables are how Helm configures
# this, and a plain comma-separated list is what anyone would write there.
HostList = Annotated[list[str], NoDecode]


class Settings(BaseSettings):
    """Server settings. Every field is overridable via ``KUBEMCP_<FIELD>``."""

    model_config = SettingsConfigDict(
        env_prefix="KUBEMCP_",
        env_file=".env",
        extra="ignore",
    )

    # -- Serving ------------------------------------------------------------
    transport: Transport = "streamable-http"
    host: str = Field(
        default="0.0.0.0",  # noqa: S104 - a container must bind all interfaces
        description="Bind address. In a container this must be 0.0.0.0, not 127.0.0.1.",
    )
    port: int = Field(default=8080, ge=1, le=65535)
    mcp_path: str = Field(default="/mcp", description="Path the MCP endpoint is served at.")

    allowed_hosts: HostList = Field(
        default_factory=list,
        description=(
            "Host header allowlist for the transport's DNS-rebinding protection. "
            "MUST include the in-cluster Service name, e.g. "
            "'kubemcp.default.svc.cluster.local:8080', or every request is rejected. "
            "Empty means 'allow any', which the server applies explicitly."
        ),
    )
    allowed_origins: HostList = Field(
        default_factory=list,
        description="Origin allowlist. Only relevant to browser callers; empty means any.",
    )

    # -- Kubernetes ---------------------------------------------------------
    kubeconfig: str | None = Field(
        default=None,
        description=(
            "Path to a kubeconfig. Unset means in-cluster config, falling back to ~/.kube/config."
        ),
    )
    kube_context: str | None = Field(
        default=None, description="kubeconfig context to use. Ignored when running in-cluster."
    )
    default_namespace: str = Field(
        default="default",
        description="Namespace used when a tool call omits one.",
    )
    request_timeout_seconds: float = Field(
        default=30.0, gt=0, le=600, description="Per-request timeout against the apiserver."
    )
    max_concurrent_requests: int = Field(
        default=16,
        ge=1,
        le=512,
        description="Ceiling on in-flight apiserver requests, so one agent cannot swamp it.",
    )

    # -- Output budgets -----------------------------------------------------
    # An LLM consumes these responses, so size is a correctness concern: an
    # un-budgeted list call can bury the caller's context. Every limit below is
    # enforced with an explicit truncation marker so the caller knows to narrow.
    max_response_bytes: int = Field(
        default=96_000,
        ge=1_000,
        le=5_000_000,
        description="Byte ceiling on a single tool result before truncation.",
    )
    max_log_bytes: int = Field(
        default=64_000, ge=1_000, le=5_000_000, description="Byte ceiling on a log fetch."
    )
    max_exec_output_bytes: int = Field(
        default=32_000, ge=1_000, le=5_000_000, description="Byte ceiling on exec stdout+stderr."
    )
    max_list_items: int = Field(
        default=200, ge=1, le=5_000, description="Hard ceiling on items returned by a list call."
    )
    exec_timeout_seconds: float = Field(
        default=60.0, gt=0, le=600, description="Wall-clock ceiling on a single exec."
    )

    # -- Misc ---------------------------------------------------------------
    field_manager: str = Field(
        default="kubemcp",
        description="Field manager recorded for server-side apply.",
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("allowed_hosts", "allowed_origins", mode="before")
    @classmethod
    def _split_csv(cls, value: object) -> object:
        """Accept ``a,b,c`` as well as a JSON list, since Helm values are strings."""
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("mcp_path")
    @classmethod
    def _leading_slash(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("mcp_path must start with '/'")
        return value


def load_settings() -> Settings:
    """Build settings from the environment."""
    return Settings()
