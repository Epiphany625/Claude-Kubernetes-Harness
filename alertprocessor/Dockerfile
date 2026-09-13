# ---------------------------------------------------------------------------
# Build stage.
#
# Deliberately plain: no BuildKit-only syntax (`--mount=type=cache`), so this
# builds with any Docker, buildx present or not. Layer ordering already gives
# most of the caching benefit.
# ---------------------------------------------------------------------------
FROM golang:1.27-bookworm AS builder

WORKDIR /src

# Dependencies before source, so editing a handler does not re-download the
# module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

# CGO_ENABLED=0 produces a static binary, which is what lets the runtime stage
# be `scratch`-adjacent with no libc to keep patched.
#
# -trimpath strips local filesystem paths out of panics and stack traces;
# without it every trace leaks the builder's directory layout.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/alertprocessor \
        ./cmd/alertprocessor

# ---------------------------------------------------------------------------
# Runtime stage: the binary, CA certificates, and nothing else.
#
# distroless/static rather than alpine: no shell, no package manager, no busybox
# for anything that gets in to borrow. The CA bundle is the one thing actually
# needed, because Supabase is reached over TLS and a static binary with no
# /etc/ssl/certs cannot verify it -- which fails as an opaque x509 error at
# connect time.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

LABEL org.opencontainers.image.title="alertprocessor" \
      org.opencontainers.image.description="Receives Alertmanager webhooks, records events in PostgreSQL, publishes to RabbitMQ" \
      org.opencontainers.image.source="https://github.com/Epiphany625/Claude-Kubernetes-Harness" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /out/alertprocessor /usr/local/bin/alertprocessor

# A container must bind every interface; 127.0.0.1 would be unreachable from the
# Alertmanager pod.
ENV ALERTPROCESSOR_HTTP_ADDR=":8080"

# 65532 is distroless's `nonroot`. Matches runAsUser in the Deployment.
USER 65532:65532
EXPOSE 8080

# Liveness and readiness are HTTP endpoints (/healthz, /readyz) that Kubernetes
# probes directly. A shell-based HEALTHCHECK would need a shell, which is
# exactly what this base image is chosen for not having.
ENTRYPOINT ["/usr/local/bin/alertprocessor"]
