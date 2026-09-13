---
name: alert-pipeline-debug
description: Debug the alert pipeline when an alert fired but nothing arrived - no row in the event table, no message on agent.events, or a row stuck with published_at NULL. Also covers webhooks answering 401 or 500, duplicate events, and confirming that Alertmanager is actually routing to the processor. Use whenever the symptom is "the alert did not make it through".
---

# Debugging the alert pipeline

Most failures here are not in the Go code. They are in the gap between what
Alertmanager decided to do with an alert and what you assume it did. Alertmanager
drops alerts silently and by design — an unmatched route is not an error, it is a
route to `"null"`.

So: **work backwards from the database, and prove each hop rather than assuming
it.**

## First: is it one of these five?

Each has cost real debugging time. Check them before going deeper.

| Symptom | Cause |
|---|---|
| No rows at all, service healthy, no webhook in the logs | Alertmanager is routing to `"null"`. Either the patch was not applied, or the AlertmanagerConfig label does not match the selector the patch sets. |
| Rows appear for `ckh` alerts but never for `kube-system` or `default` | `alertmanagerConfigMatcherStrategy` is still at its default, so the operator injected `namespace="ckh"` into the route. This is the #1 failure and it looks like a working pipeline if you only test with ckh-labelled alerts. |
| Your smoke test using `Watchdog` never arrives | Correct behaviour, but not for the reason you expect. Our route is *prepended* before the chart's `Watchdog → "null"` entry, so the chart does not shield us; the `alertname !~ "Watchdog\|InfoInhibitor"` matcher on our own route is what excludes it. Use `make smoke`. |
| `make am-config` shows an empty config, or kubectl errors with "invalid value; expected string" | You are reading `.data.alertmanager\.yaml`, but prometheus-operator >= 0.78 stores it **gzipped** under `alertmanager.yaml.gz`. Use `make am-config`, which handles both. |
| Rows exist but `published_at` is always NULL | Unroutable publish: the exchange exists, nothing is bound to it. The service logs this explicitly — grep for `unroutable`. |
| `prepared statement ... already exists`, only sometimes | Pointed at the Supabase pooler (6543) with `POSTGRES_POOL_MODE=session`. Set it to `transaction`. |
| Every alert fails with `failed to encode args[N]: ... cannot find encode plan`, or the server answers `invalid input syntax for type json` | A `jsonb` parameter reached pgx as a map or `[]byte`. `transaction` mode sends statements unprepared, so pgx never learns the parameter is jsonb and falls back to the Go type: a map matches nothing, `[]byte` matches *bytea* (hex). Serialise to `string` in `RecordEvent`. Works in `session` mode either way, which is how it ships. |
| A rebuilt image rolls out successfully and still runs the old code | `minikube image load` keeps the image already in the cluster when the tag is unchanged, and `minikube image rm` refuses while a container uses it. `make load` builds inside minikube's daemon for this reason — check `minikube ssh -- docker images alertprocessor` against your local build id. |

## The chain, hop by hop

Stop at the first hop that fails; everything after it is a consequence.

### 1. Did Prometheus fire the alert?

```bash
kubectl -n monitoring port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090
# then: http://localhost:9090/alerts
```

If the rule is not listed at all, Prometheus is not selecting your
PrometheusRule. `ruleSelector` is `matchLabels: {release: monitoring}` — the
label is mandatory, and a rule without it is accepted by the API and silently
ignored.

```bash
kubectl -n ckh get prometheusrule alertprocessor -o jsonpath='{.metadata.labels}'
```

If it is listed but `inactive`, the expression is not matching. Evaluate it by
hand in the Prometheus UI.

### 2. Did Alertmanager route it, or swallow it?

This is where it usually breaks. Read the config the **operator generated**, not
the one Helm wrote — they are different objects and only the generated one is
what Alertmanager runs. It is gzipped, so:

```bash
make am-config
# which is, for a modern operator:
kubectl -n monitoring get secret \
  alertmanager-monitoring-kube-prometheus-alertmanager-generated \
  -o jsonpath='{.data.alertmanager\.yaml\.gz}' | base64 -d | gunzip
```

Read it for three things:

- **Is there an `alertprocessor` receiver at all?** The merged receiver is named
  `<namespace>/<config-name>/<receiver-name>`, so look for
  `ckh/alertprocessor/alertprocessor` rather than a bare `alertprocessor`. If the
  only receiver is `"null"`, the merge did not happen. Apply
  `ops/alerting/alertmanager-patch.yaml` *first*, then the AlertmanagerConfig —
  the patch sets the label selector that makes the AlertmanagerConfig eligible.

- **Does the merged route carry a `namespace="ckh"` matcher you did not write?**
  The operator injects it unless `alertmanagerConfigMatcherStrategy: None` is
  set. With it, only alerts *about* ckh can ever reach you.

- **Where is our route in the list, and what matchers does it carry?** The
  operator **prepends** merged routes and forces `continue: true` on them. So
  ours is first and shadows nothing — which means any exclusion has to be an
  explicit matcher on our own route, not a reliance on the chart's routes
  catching something first.

Check the operator accepted the AlertmanagerConfig at all:

```bash
kubectl -n monitoring logs -l app.kubernetes.io/name=prometheus-operator --tail=100 \
  | grep -i alertmanagerconfig
```

A rejected AlertmanagerConfig is logged there and nowhere else.

### 3. Did the webhook reach the service?

```bash
make logs
# looking for: "webhook delivery received" with group_key and alerts=N
```

Nothing? Check it from Alertmanager's side:

```bash
kubectl -n monitoring logs -l app.kubernetes.io/name=alertmanager --tail=100 \
  | grep -iE 'notify|webhook|error'
```

`dial tcp: lookup alertprocessor.ckh.svc.cluster.local: no such host` means the
Service name or namespace does not match the `url` in the AlertmanagerConfig.
Those two strings are a contract; nothing validates them together.

Reproduce the call yourself, bypassing Alertmanager:

```bash
kubectl -n ckh port-forward svc/alertprocessor 8080:8080
curl -sS -X POST http://localhost:8080/api/v1/alerts \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $(kubectl -n ckh get secret alertprocessor-webhook-auth \
        -o jsonpath='{.data.token}' | base64 -d)" \
  --data @test/testdata/firing-batch.json | jq
```

If that works and Alertmanager's call does not, the difference is auth or
routing, not the service.

### 4. Did the insert happen?

```sql
SELECT event_id, alert_id, alert_name, status, alert_status,
       received_at, published_at
  FROM event
 ORDER BY received_at DESC
 LIMIT 20;
```

A row with `published_at` set is a complete success. A row with it NULL means
step 5 failed.

### 5. Did the message reach the queue?

```bash
make rabbit-ui    # http://localhost:15672
```

Check, in this order:

1. Does the exchange `alerts` exist?
2. Does the queue `agent.events` exist?
3. **Is there a binding from `alerts` to `agent.events` on `alert.#`?** A missing
   binding is the common one, and it is what produces `ErrUnroutable`.
4. Is anything consuming? Messages accumulating is fine before `harness/` exists.

```bash
make logs | grep -iE 'unroutable|nack|confirm'
```

## Reading the service's own signals

The service is scraped by the same Prometheus it serves.

```promql
rate(alertprocessor_alerts_received_total[5m])            # alerts arriving
rate(alertprocessor_events_recorded_total[5m])            # by inserted/duplicate
rate(alertprocessor_events_published_total[5m])           # by published/skipped
rate(alertprocessor_event_failures_total[5m])             # by stage
alertprocessor_dependency_up                              # postgres / rabbitmq
```

`event_failures_total` by `stage` is the fastest way to split "the pipeline is
broken" into which half:

- `record` — Postgres. Check `/readyz` and the DSN.
- `publish` — RabbitMQ. Check for `unroutable` in the logs first.
- `mark_published` — the message went out but the row was not updated. Rare; the
  event is on the queue twice after the retry, which is why consumers dedupe on
  `message_id`.

## Symptom-specific

### Every webhook answers 401

`ALERTPROCESSOR_WEBHOOK_TOKEN` and the `alertprocessor-webhook-auth` Secret have
diverged. The AlertmanagerConfig reads that Secret and the Deployment mounts the
same key, so they can only differ if one was edited alone.

```bash
kubectl -n ckh get secret alertprocessor-webhook-auth -o jsonpath='{.data.token}' | base64 -d
kubectl -n ckh exec deploy/alertprocessor -- printenv ALERTPROCESSOR_WEBHOOK_TOKEN
```

Note the pod does not pick up a Secret change until it restarts: `make restart`.

### Webhooks answer 500 and Alertmanager keeps retrying

That is the design — 500 means "retry me". Find which stage failed in the
response body or the logs. The retry is safe: already-processed alerts
short-circuit on `published_at`.

### The same alert produces a new row every few hours

The deduplication key is wrong or the index is missing.

```sql
SELECT indexdef FROM pg_indexes WHERE tablename = 'event';
-- expect: UNIQUE (alert_id, alert_status, starts_at)
```

If rows differ only in `event_id` and `received_at`, the index is not there.

### A resolution never produces a row

The opposite error: the key was narrowed and `alert_status` dropped out of it, so
a resolution collides with its own firing row. `TestDeduplication` in the
integration suite covers both directions.

### The pod is Running but not Ready

`/readyz` checks Postgres and RabbitMQ; `/healthz` checks neither. So Running but
not Ready means a dependency is down, not that the process is wedged.

```bash
kubectl -n ckh port-forward svc/alertprocessor 8080:8080 &
curl -s localhost:8080/readyz | jq    # names the failing dependency
```

## Turning a reproduction into a test

Once you know what broke, put it where it cannot break again:

- A projection or validation bug → a unit test in the relevant package. It must
  not need Docker.
- A SQL or AMQP behaviour → `-tags=integration`, in
  `internal/store/postgres_integration_test.go` or
  `internal/queue/rabbitmq_integration_test.go`.
- An `ops/` routing bug → `test/e2e/pipeline_test.go`. This is the only level
  that can catch one. `TestAlertmanagerRoutesToTheProcessor` already asserts
  against the two most common misconfigurations; extend it rather than adding a
  parallel check.
