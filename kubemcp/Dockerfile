# ---------------------------------------------------------------------------
# Build stage: resolve dependencies with uv into a self-contained virtualenv.
#
# Deliberately plain: no BuildKit-only syntax (`--mount=type=cache`), so this
# builds with any Docker, buildx present or not. Layer ordering already gives
# most of the caching benefit.
# ---------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies are installed before the source is copied, so an edit to the
# application does not invalidate the dependency layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY src/ src/
COPY README.md ./
RUN uv sync --frozen --no-dev

# ---------------------------------------------------------------------------
# Runtime stage: interpreter plus the built virtualenv, nothing else.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="kubemcp" \
      org.opencontainers.image.description="Kubernetes MCP server for AI agents" \
      org.opencontainers.image.source="https://github.com/Epiphany625/Claude-Kubernetes-Harness" \
      org.opencontainers.image.licenses="Apache-2.0"

# Unprivileged, and with no home directory or login shell to be borrowed.
RUN groupadd --system --gid 65532 kubemcp \
    && useradd --system --uid 65532 --gid kubemcp --no-create-home --shell /usr/sbin/nologin kubemcp

WORKDIR /app

COPY --from=builder --chown=65532:65532 /app/.venv /app/.venv
COPY --from=builder --chown=65532:65532 /app/src /app/src

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # A container must bind every interface; 127.0.0.1 would be unreachable
    # from another pod.
    KUBEMCP_HOST=0.0.0.0 \
    KUBEMCP_PORT=8080

USER 65532:65532
EXPOSE 8080

# Liveness and readiness are HTTP endpoints (/healthz, /readyz) so Kubernetes
# probes them directly; no shell-based HEALTHCHECK is needed or wanted here.
ENTRYPOINT ["python", "-m", "kubemcp"]
