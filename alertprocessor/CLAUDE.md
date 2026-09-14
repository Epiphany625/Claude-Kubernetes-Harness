# alertprocessor — working notes

Receives Alertmanager webhooks, records each alert as an event row in PostgreSQL
with status `waiting`, publishes it to RabbitMQ. The consumer is `harness/`,
which owns every status after `waiting`.

Architecture and configuration are in [README.md](README.md). This file is what
you need in your head before changing anything.

## Commands

```bash
make test               # unit only; NO cluster, NO database, NO Docker
make test-integration   # real Postgres + RabbitMQ via testcontainers (needs Docker)
make test-e2e           # the deployed pipeline (needs a cluster + a DSN)
make lint               # gofmt + go vet across all three build tags
make load && make restart   # rebuild, push into minikube, roll the deployment
make am-config          # print the Alertmanager config the operator generated
make smoke              # fire a real alert through the whole chain
```

`make test` must pass with nothing running. If a change makes the ordinary suite
require a cluster or Docker, the change is wrong, not the suite. Anything needing
a real dependency goes behind `-tags=integration`; anything needing the cluster
goes behind `-tags=e2e`.

## Invariants

Six rules. Each exists because breaking it produces a specific, real failure.

1. **Record before publish. Never the other way around.**
   A message carrying an `event_id` with no row behind it leaves the harness
   chasing a reference that does not exist. A row with `published_at IS NULL` is
   the strictly better failure: visible, queryable, replayable. The order lives
   in `processor.Process` and the comment there is load-bearing.

2. **`waiting` is the only status this service writes.**
   The other four are declared in `internal/event/event.go` so the vocabulary has
   one definition, and they belong to `harness/`. If you find yourself writing
   `solving` here, the logic is in the wrong service.

3. **The deduplication key is a contract, not an implementation detail.**
   `UNIQUE (alert_id, alert_status, starts_at)` is what makes the pipeline safe
   to retry. Widening the tuple (adding `received_at`, say) silently disables
   deduplication and the harness starts solving the same alert every four hours.
   Narrowing it (dropping `alert_status`) silently swallows resolutions and the
   harness never learns a problem went away. Both compile. Both pass a casual
   test. `TestDeduplication` in the integration suite is what catches them.

4. **`RecordEvent` returns the stored row's `event_id`, and the caller must use
   it.** On a duplicate it differs from the one the projection generated. Publish
   the generated one and `message_id` names a row nobody can find — reintroducing
   exactly the dangling reference invariant 1 exists to prevent. `ON CONFLICT ...
   DO UPDATE` rather than `DO NOTHING` is what makes the statement return it at
   all.

5. **Liveness must never touch a dependency.**
   `/healthz` answers from process state alone; `/readyz` is where Postgres and
   RabbitMQ get checked. Put a dependency in the liveness probe and a database
   blip restarts every replica at once — turning a recoverable outage into a
   reconnect storm against something already struggling.

6. **Publish with `mandatory` and wait for the confirm.**
   Without `mandatory`, a message with no queue bound to its routing key is
   silently discarded *and the publish still confirms*. Without confirms,
   `Publish` returns once the bytes hit the socket, which says nothing about
   whether the broker stored them — and `published_at` is written on the strength
   of that return value. Both together are what make `published_at` mean
   something.

## Layout

```
cmd/alertprocessor/     wiring, signals, graceful shutdown
internal/
  config/       ALERTPROCESSOR_* parsing; reports EVERY problem, not the first
  alertmanager/ payload types, fingerprint fallback, projection to Event
  event/        Event, the status vocabulary, routing keys
  store/        Store interface, pgx impl, go:embed migration
    migrations/ 0001_event.sql  ← the schema's single source of truth
  queue/        Publisher interface, amqp091 impl with confirms + lazy reconnect
  processor/    record → skip-if-published → publish → mark
  httpapi/      routes, webhook handler, probes
  obs/          slog setup
test/e2e/       live-cluster suite
test/testdata/  canned Alertmanager payloads
```

`Store` and `Publisher` are interfaces so the handler and processor can be unit
tested against fakes. There is exactly one production implementation of each; the
interfaces exist for testability, not for a second backend.

## Things that will bite you

### The Alertmanager namespace matcher

By default the prometheus-operator injects `namespace="ckh"` into any route
derived from an AlertmanagerConfig in the `ckh` namespace. Cluster-wide alerts
then match nothing and fall through to the chart's `"null"` receiver. **Silently.**

`ops/alerting/alertmanager-patch.yaml` sets
`alertmanagerConfigMatcherStrategy: None` to stop it, and narrows
`alertmanagerConfigSelector` to compensate for what `None` opens up. Apply the
patch *before* the AlertmanagerConfig — the patch sets the label selector that
makes the AlertmanagerConfig eligible at all.

The trap: if you smoke-test with an alert labelled `namespace=ckh`, the broken
configuration passes.

### Merged routes are PREPENDED, and always get `continue: true`

Two operator behaviours that are easy to assume backwards. Confirmed by reading
the generated config, not the CR:

- An AlertmanagerConfig's route is inserted **before** the chart's own routes,
  not after. So the chart's `alertname = "Watchdog" → "null"` entry does *not*
  shield us from Watchdog — ours is evaluated first.
- The operator writes `continue: true` on the merged route whatever the CR says.
  `continue: false` in `ops/alerting/alertmanagerconfig.yaml` would be ignored.

Consequence: a bare catch-all route here receives everything, including the
synthetic always-firing alerts. `Watchdog` is a deadman's switch whose *absence*
is the signal, and `InfoInhibitor` exists only to drive inhibit rules; handing
either to an agent means a problem that is permanently open and unsolvable. The
route carries `alertname !~ "Watchdog|InfoInhibitor"` for exactly this, and
removing those matchers quietly fills the event table with unsolvable work.

This also means Watchdog is not usable as a smoke test either way. Use
`make smoke`.

### Reading the generated Alertmanager config

`make am-config`, not a hand-typed kubectl. Two things catch people out:

- The config the operator *generates* lives in the
  `...-alertmanager-generated` Secret. The one Helm wrote is a different object
  and is not what Alertmanager runs.
- Since prometheus-operator 0.78 the generated config is **gzipped**, under the
  key `alertmanager.yaml.gz`. Reading `.data.alertmanager\.yaml` gets you an
  empty value and a confusing template error, not a useful message.

### `sslmode` defaults to `require`, deliberately

pgx's own default is `prefer`, which falls back to plaintext against a server
that would have accepted TLS. Supabase refuses plaintext, so `prefer` turns a
configuration error into an opaque connection failure. `config.resolveDSN` sets
`require` when the DSN omits it.

### One webhook delivery is many alerts

Alertmanager groups. A delivery about twelve crash-looping pods is one POST and
must become twelve rows. `Payload.ToEvents` is the only place that fans out;
anything treating a delivery as a single alert loses eleven of twelve.

Relatedly: `maxAlerts: 0` in the AlertmanagerConfig is load-bearing. A nonzero
value makes Alertmanager *drop* alerts from the batch and replace them with a
summary line — the service records fewer events than fired, and nothing errors.
The handler logs loudly when `truncatedAlerts > 0` because nothing here can
recover them.

### Fingerprint stability

`event.Fingerprint` is the fallback when a sender omits Alertmanager's own
`fingerprint`. It sorts the label keys before hashing because Go randomizes map
iteration: an order-dependent hash would give the same alert a different
`alert_id` on every delivery, and the deduplication index would never match
anything. `TestFingerprintIsStable` hashes the same map 200 times.

## Adding a field to the event contract

Use the `add-event-field` skill in `.claude/skills/`. In short: the migration,
the `Event` struct, the INSERT column list *and* its parameter positions, the
`GetEvent` scan, and a test — in that order, and the parameter positions are
where this goes wrong.

## Out of scope

- **No status transitions beyond `waiting`.** Invariant 2. This service does not
  know what solving an alert means.
- **No consuming.** It publishes; `harness/` consumes. A service that does both
  ends up with the queue as an internal detail rather than a boundary.
- **No alert enrichment.** Do not call the Kubernetes API here to decorate an
  event. The harness has `kubemcp` for that, with an RBAC boundary this service
  deliberately does not have — note `automountServiceAccountToken: false` in the
  Deployment.
- **No retry loop for failed publishes.** Alertmanager is the retry mechanism;
  the 500 response is how you use it. An in-process queue would add durability
  this service does not have and would lose its contents on the next rollout.
