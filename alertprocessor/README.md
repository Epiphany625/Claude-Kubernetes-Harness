# alertprocessor

Receives alerts from Alertmanager, records each one as an **event** in
PostgreSQL with status `waiting`, and publishes it to RabbitMQ for the
[harness](../harness) to act on.

It is the front door of the agent pipeline. Everything downstream assumes that
if an alert fired, there is a row for it.

Language: Go.

---

## Architecture

```
  Prometheus                    evaluates PrometheusRules, fires alerts
      │                         ops/alerting/prometheusrule-ckh.yaml
      ▼
  Alertmanager                  groups, deduplicates, inhibits
      │                         ops/alerting/alertmanagerconfig.yaml
      │  POST /api/v1/alerts    one delivery = MANY alerts
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  alertprocessor                                                  │
│                                                                  │
│   httpapi    decode → project each alert into an Event            │
│      │                                                           │
│      ▼                                                           │
│   processor  for each alert, in this order:                      │
│      │                                                           │
│      │  1. INSERT ... ON CONFLICT  ──────────►  PostgreSQL       │
│      │       status = 'waiting'                  table: event    │
│      │       returns the row's event_id                          │
│      │                                                           │
│      │  2. skip if published_at is already set                   │
│      │                                                           │
│      │  3. publish, wait for confirm  ────────►  RabbitMQ        │
│      │       exchange: alerts (topic)            agent.events    │
│      │       key: alert.<state>.<severity>                       │
│      │                                                           │
│      │  4. UPDATE published_at = now()  ──────►  PostgreSQL      │
│      │                                                           │
│      ▼                                                           │
│   200 if every alert succeeded, 500 if any did not               │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                            harness/ consumes agent.events,
                            moves the row through solving →
                            waiting_human_approval → resolved
```

### An alert is not an event

`alert_id` is Alertmanager's fingerprint: one identity for as long as the alert
keeps the same labels. `event_id` is one _notification_ about that alert — one
unit of work for the agent. A single alert produces several events over its life:

| What happened                             | Row                                                                     |
| ----------------------------------------- | ----------------------------------------------------------------------- |
| `KubePodCrashLooping` fires at 10:00      | `event_id=ev-001, alert_id=a1b2, alert_status=firing, status=waiting`   |
| Alertmanager retries the same delivery    | _no new row_ — deduplicated                                             |
| Still firing at 14:00 (`repeat_interval`) | _no new row_ — deduplicated                                             |
| Resolves at 14:30                         | `event_id=ev-002, alert_id=a1b2, alert_status=resolved, status=waiting` |
| Fires again at 18:00                      | `event_id=ev-003, alert_id=a1b2, alert_status=firing, status=waiting`   |

Deduplication is a unique index on `(alert_id, alert_status, starts_at)`. It
absorbs retries and repeat notifications, and lets through the things that are
genuinely new.

### Delivery guarantees

**Record before publish, always.** A message naming an `event_id` with no row
behind it would leave the harness chasing a reference that does not exist. A row
that was recorded but never published is the better failure: visible
(`published_at IS NULL`), queryable, replayable.

**At-least-once to the queue.** If the publish confirms but the `published_at`
update fails, the sender retries and the event is published twice. The message's
`message_id` is the `event_id` and is stable across retries, so **consumers must
deduplicate on `message_id`.**

**HTTP status is a control signal.** Alertmanager retries a 5xx and gives up on
a 4xx, so:

| Situation                                | Code | Why                                                      |
| ---------------------------------------- | ---- | -------------------------------------------------------- |
| All alerts processed                     | 200  | —                                                        |
| Batch was empty                          | 200  | Alertmanager never sends empty batches; nothing to retry |
| Body will not parse                      | 400  | Retrying will not make it parse                          |
| Any alert failed on Postgres or RabbitMQ | 500  | Exactly what a retry fixes                               |

A partial failure returns 500 for the whole batch. The retry is safe: the alerts
that already succeeded short-circuit on `published_at`.

**Publishes use `mandatory` + publisher confirms.** Without `mandatory`, a
message with no queue bound to its routing key is silently discarded and the
publish still confirms — the service would report success for an event nobody
will ever receive. That is the most likely failure before `harness/` exists.

---

## Layout

```
cmd/alertprocessor/     wiring, signals, graceful shutdown
internal/
  config/               ALERTPROCESSOR_* env parsing and validation
  alertmanager/         webhook payload types, fingerprint fallback, projection
  event/                Event, the status vocabulary, routing keys
  store/                Store interface, pgx implementation, embedded migration
    migrations/         0001_event.sql -- the schema's single source of truth
  queue/                Publisher interface, amqp091 implementation
  processor/            record → publish → mark, and the reasoning for that order
  httpapi/              routes, webhook handler, probes
  obs/                  slog setup
test/
  e2e/                  live-cluster suite (-tags=e2e)
  testdata/             canned Alertmanager payloads
```

# File Explanations

1. Entry point: `cmd/alertprocessor/main.go`: Initialize main resources (database connection, queue publisher connection, a process, an http service) and supporting resources (logger, Prometheus metrics).
2. Main loop: `internal/httpapi` package defines liveness check endpoint, readiness check endpoint, and most importantly, webhook entry endpoint (`webhook.go`). It receives a raw JSON payload from `AlertManager`, converts the payload into recognizable events(`ToEvents()`), insert to postgres, and publish to rabbitmq, and mark it as published.
3. Everything else are authentication, validation, metrics reporting, postgres client, etc.

---

## Running it

### Locally

#### rabbit mq:

````
export ALERTPROCESSOR_AMQP_USERNAME=<>
export ALERTPROCESSOR_AMQP_PASSWORD=<>

# to get these values
kubectl -n ckh get secret agent-rabbitmq-default-user \
  -o jsonpath='{.data.username}' | base64 --decode; echo
kubectl -n ckh get secret agent-rabbitmq-default-user \
  -o jsonpath='{.data.password}' | base64 --decode; echo

# open up port forward with dashboard:
kubectl -n ckh port-forward svc/agent-rabbitmq 15672:15672
kubectl -n ckh port-forward svc/agent-rabbitmq 5672:5672

```
### In the cluster

```bash
# 1. Secrets. Copy the template, fill in your Supabase DSN, apply it.
cp ../ops/alertprocessor/secret.example.yaml ../ops/alertprocessor/secret.yaml
$EDITOR ../ops/alertprocessor/secret.yaml
kubectl apply -f ../ops/alertprocessor/secret.yaml

# 2. Image into minikube, then the manifests.
make load
make deploy

# 3. Point Alertmanager at the webhook.
make route

# 4. Confirm the operator actually merged the route.
make am-config     # look for the `alertprocessor` receiver, not just `"null"`
````

`make help` lists the rest.

### Cluster prerequisites

RabbitMQ is provided by the [cluster operator](https://github.com/rabbitmq/cluster-operator),
which needs cert-manager:

```bash
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace --set crds.enabled=true

kubectl apply -f https://github.com/rabbitmq/cluster-operator/releases/latest/download/cluster-operator.yml
kubectl apply -f ../ops/agent-rabbitmq.yaml
```

The service reads the broker credentials straight from the operator-managed
`agent-rabbitmq-default-user` Secret, so there is nothing to copy. To look at
them yourself:

```bash
kubectl -n ckh get secret agent-rabbitmq-default-user \
  -o jsonpath='{.data.username}' | base64 --decode; echo
kubectl -n ckh get secret agent-rabbitmq-default-user \
  -o jsonpath='{.data.password}' | base64 --decode; echo

make rabbit-ui    # management UI on http://localhost:15672
```

Prometheus and Alertmanager come from the `kube-prometheus-stack` Helm release in
the `monitoring` namespace, installed out of band.

---

## Configuration

Environment only, all prefixed `ALERTPROCESSOR_`. Non-secret values live in
[ops/alertprocessor/configmap.yaml](../ops/alertprocessor/configmap.yaml);
secrets in the Secret.

For Postgres and RabbitMQ you may give a whole URL **or** discrete parts, and the
parts override the URL. That is what lets a Secret supply only the password while
the rest stays readable in a ConfigMap — and it means nothing has to URL-escape a
generated password inside a YAML string.

### HTTP

| Variable             | Default          | Notes                                           |
| -------------------- | ---------------- | ----------------------------------------------- |
| `HTTP_ADDR`          | `:8080`          |                                                 |
| `WEBHOOK_PATH`       | `/api/v1/alerts` | Must match the `url` in the AlertmanagerConfig  |
| `MAX_BODY_BYTES`     | `8388608`        | 8 MiB. Bounds memory on a large batch           |
| `HTTP_READ_TIMEOUT`  | `15s`            |                                                 |
| `HTTP_WRITE_TIMEOUT` | `30s`            | Must exceed `AMQP_PUBLISH_TIMEOUT`              |
| `HTTP_IDLE_TIMEOUT`  | `60s`            |                                                 |
| `SHUTDOWN_TIMEOUT`   | `20s`            | In-flight webhooks are drained                  |

### PostgreSQL

| Variable                                                        | Default   | Notes                                                 |
| --------------------------------------------------------------- | --------- | ----------------------------------------------------- |
| `POSTGRES_DSN`                                                  | —         | `postgresql://user:pass@host:port/db?sslmode=require` |
| `POSTGRES_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DATABASE` | —         | Override parts of the DSN                             |
| `POSTGRES_SSLMODE`                                              | `require` | Defaulted on; Supabase refuses plaintext              |
| `POSTGRES_MAX_CONNS`                                            | `10`      |                                                       |
| `POSTGRES_CONNECT_TIMEOUT`                                      | `10s`     |                                                       |
| `POSTGRES_QUERY_TIMEOUT`                                        | `5s`      | Per statement                                         |
| `DB_AUTO_MIGRATE`                                               | `true`    | Applies the embedded schema at startup                |

If you would rather the service not hold DDL rights, set `DB_AUTO_MIGRATE=false`
and run [`internal/store/migrations/0001_event.sql`](internal/store/migrations/0001_event.sql)
yourself.

### RabbitMQ

| Variable                                                     | Default        | Notes                                            |
| ------------------------------------------------------------ | -------------- | ------------------------------------------------ |
| `AMQP_URL`                                                   | —              | `amqp://user:pass@host:port/vhost`               |
| `AMQP_HOST` / `_PORT` / `_USERNAME` / `_PASSWORD` / `_VHOST` | —              | Override parts of the URL                        |
| `AMQP_EXCHANGE`                                              | `alerts`       | Topic exchange                                   |
| `AMQP_QUEUE`                                                 | `agent.events` | Quorum queue                                     |
| `AMQP_ROUTING_KEY_PREFIX`                                    | `alert`        | Keys are `<prefix>.<state>.<severity>`           |
| `AMQP_DECLARE_TOPOLOGY`                                      | `true`         | Set false once `harness/` declares its own queue |
| `AMQP_CONFIRM_TIMEOUT`                                       | `5s`           |                                                  |
| `AMQP_PUBLISH_TIMEOUT`                                       | `8s`           | Must stay under `HTTP_WRITE_TIMEOUT`             |

### Logging

| Variable     | Default | Notes                            |
| ------------ | ------- | -------------------------------- |
| `LOG_LEVEL`  | `info`  | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json`  | `text` for a terminal            |

---

## The event table

```sql
event_id      UUID PRIMARY KEY   -- one notification, one unit of agent work
alert_id      TEXT NOT NULL      -- Alertmanager's fingerprint
status        TEXT NOT NULL      -- waiting | solving | waiting_human_approval
                                 --   | resolved | failed_to_resolve
alert_status  TEXT NOT NULL      -- firing | resolved  (what Prometheus says)
alert_name, severity, namespace, resource, summary, description,
runbook_url, generator_url, starts_at, ends_at
labels        JSONB              -- the full label set
annotations   JSONB
raw_alert     JSONB              -- the alert as received, for replay
received_at, published_at, created_at, updated_at

UNIQUE (alert_id, alert_status, starts_at)   -- the deduplication key
```

**This service only ever writes `waiting`.** The rest of the vocabulary is
declared in one place — [`internal/event/event.go`](internal/event/event.go) and
the `CHECK` constraint in the migration — and belongs to `harness/`. A unit test
fails if the two ever disagree.

`status` is a `CHECK`-constrained `TEXT` rather than a PostgreSQL enum, so adding
a state later is an ordinary migration.

## The queue message

```
exchange      alerts            (topic, durable)
routing key   alert.firing.critical
queue         agent.events      (quorum, durable)

message_id      = event_id      ← deduplicate on this
correlation_id  = alert_id
content_type    = application/json
delivery_mode   = 2 (persistent)
headers         alert_name, alert_status, severity, namespace, resource, status
body            the full Event, including labels, annotations and raw_alert
```

---

## Testing

Three levels. The first must always pass with nothing running.

```bash
make test              # unit: no cluster, no database, no Docker
make test-integration  # real Postgres + RabbitMQ in throwaway containers
make test-e2e          # the deployed pipeline, end to end
make lint              # gofmt + vet across every build tag
```

**Unit** (`go test ./...`) covers payload decoding and its refusals, fingerprint
stability under Go's randomized map iteration, the projection's defaults, config
parsing and every validation error, and the handler end to end against fake
store and publisher — including partial-batch failure, auth rejection, publish
failure after a successful insert, and the already-published short-circuit.

_If a change makes this suite need a cluster, the change is wrong, not the suite._

**Integration** (`-tags=integration`, needs Docker) runs the SQL and the AMQP
protocol for real: that the migration is re-runnable, that JSONB round-trips,
that the deduplication index deduplicates a replayed notification and does _not_
deduplicate a resolution or a re-fire, that concurrent duplicate inserts converge
on one row, that publishes are confirmed, that an unroutable publish is an error,
and that a publish recovers after the connection drops.

**E2E** (`-tags=e2e`) needs the pipeline deployed and
`ALERTPROCESSOR_POSTGRES_DSN` pointing at the same database the service uses. It
injects an alert into the **Alertmanager API** — not the webhook — so it is the
only test that proves the `ops/` routing is right:

```bash
export ALERTPROCESSOR_POSTGRES_DSN='postgresql://...'
make test-e2e
```

### End to end, a real alert through the whole chain

To watch it happen rather than assert on it:

```bash
make smoke          # applies a PrometheusRule with expr: vector(1)
make logs           # the delivery arrives within ~60s (30s eval + 30s groupWait)
```

```sql
SELECT event_id, alert_id, status, alert_status, published_at
  FROM event
 WHERE alert_name = 'AlertProcessorPipelineSmokeTest';
```

`make rabbit-ui` → queue `agent.events` holds a message whose `message_id` is
that `event_id`.

```bash
make smoke-clean    # delete the rule; it is the one alert meant to be firing
```

---

## Troubleshooting

Work backwards along the chain. The `alert-pipeline-debug` skill in
[`.claude/skills/`](.claude/skills/) has the full version.

**An alert fired but no row appeared.**

1. Did Alertmanager route it, or swallow it into `"null"`?

   ```bash
   make am-config     # look for the alertprocessor receiver
   ```

   If the only receiver is `"null"`, the patch or the AlertmanagerConfig did not
   take. Apply `ops/alerting/alertmanager-patch.yaml` **before** the
   AlertmanagerConfig — the patch sets the label selector that makes the
   AlertmanagerConfig eligible at all.

   Use `make am-config` rather than a hand-typed `kubectl get secret`: the
   generated config lives in the `...-generated` Secret (not the one Helm wrote)
   and since prometheus-operator 0.78 it is **gzipped** under
   `alertmanager.yaml.gz`. Reading the uncompressed key returns nothing useful.

2. Does the merged route carry an injected `namespace="ckh"` matcher? Then
   `alertmanagerConfigMatcherStrategy` is still at its default and only alerts
   _about ckh_ can ever reach you. This is the single most common failure, and it
   looks like a working pipeline if you test with a ckh-labelled alert.

3. Was it `Watchdog` or `InfoInhibitor`? Those are excluded on purpose by the
   `alertname !~` matcher on our route — they fire permanently and are not
   actionable. Use `make smoke` for a smoke test.

4. Did the webhook arrive at all?
   ```bash
   make logs
   kubectl -n monitoring logs -l app.kubernetes.io/name=alertmanager --tail=50
   ```

**Rows exist but `published_at` is NULL.** The insert worked and the handoff did
not. Almost always an unroutable publish — the exchange exists, nothing is bound
to it. Check the queue and the `alert.#` binding in `make rabbit-ui`. The service
logs this explicitly rather than letting it pass.

**`PRECONDITION_FAILED` on startup.** The queue exists with different arguments
than the service declares — usually a classic queue where it wants a quorum
queue. Delete the queue, or set `AMQP_DECLARE_TOPOLOGY=false` and let whoever owns
it declare it.

---

## Related

- [ops/README.md](../ops/README.md) — the manifests and the apply order
- [CLAUDE.md](CLAUDE.md) — invariants, and the mistakes they exist to prevent
- [harness/](../harness) — consumes `agent.events` and owns the other statuses
