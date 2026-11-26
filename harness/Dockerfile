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

# harness declares no [build-system], so uv treats it as a virtual project:
# `uv sync` installs the dependencies and never builds or installs the project
# itself. The source can therefore be copied in the runtime stage, and editing
# a handler leaves this layer alone -- which matters more here than usual,
# because it weighs in at several hundred megabytes (see below).
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# claude-agent-sdk does not call the API directly: it spawns the Claude Code
# CLI and speaks stream-json to it. The SDK ships platform-specific wheels that
# bundle that binary, and `uv sync` resolves the manylinux one here, so the
# image needs no Node.js and no `npm install -g @anthropic-ai/claude-code`.
# Fail the build loudly if that ever stops being true, rather than shipping an
# image that raises CLINotFoundError on its first alert.
RUN test -x /app/.venv/lib/python3.12/site-packages/claude_agent_sdk/_bundled/claude

# ---------------------------------------------------------------------------
# Runtime stage: interpreter plus the built virtualenv, nothing else.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="harness" \
      org.opencontainers.image.description="Consumes alert events from RabbitMQ and runs the Claude agent loop against the cluster" \
      org.opencontainers.image.source="https://github.com/Epiphany625/Claude-Kubernetes-Harness" \
      org.opencontainers.image.licenses="Apache-2.0"

# Unprivileged, and with no login shell to be borrowed. Unlike the other
# services this one does get a home directory: the Claude Code CLI keeps its
# state under $HOME/.claude and exits if it cannot write there.
RUN groupadd --system --gid 65532 harness \
    && useradd --system --uid 65532 --gid harness \
        --create-home --home-dir /home/harness \
        --shell /usr/sbin/nologin harness

# psycopg is the pure-Python build, which loads the system libpq at import.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder --chown=65532:65532 /app/.venv /app/.venv
COPY --chown=65532:65532 main.py ./
COPY --chown=65532:65532 agent/ agent/
COPY --chown=65532:65532 config/ config/
COPY --chown=65532:65532 mq/ mq/
COPY --chown=65532:65532 harness/ harness/
COPY --chown=65532:65532 notifier/ notifier/
COPY --chown=65532:65532 postgres/ postgres/

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/harness

USER 65532:65532

# No EXPOSE and no HEALTHCHECK: the harness opens an outbound AMQP connection
# and listens on nothing, so there is no port to publish and no endpoint for a
# probe to hit. Liveness is "the process is still running"; when the consumer
# hits a handler or connection error it exits, RabbitMQ requeues whatever was
# outstanding, and the Deployment's restart policy brings it back.
ENTRYPOINT ["python", "main.py"]
