---
name: add-event-field
description: Add or change a field on the event contract - a column in the event table, a field on the Event struct, or a property on the RabbitMQ message. Use whenever work touches internal/store/migrations/, the Event struct, the INSERT statement, or anything harness/ reads. Covers the ordered sequence, the parameter-position trap, and what makes a change safe for the consumer.
---

# Changing the event contract

The `Event` struct is three contracts at once: a table in PostgreSQL, a JSON body
on RabbitMQ, and the thing `harness/` reads. A change that satisfies the Go
compiler can still break the other two silently.

Work in this order. Skipping a step does not fail a build.

## 0. Decide whether it belongs in a column

Every alert already arrives complete: `labels`, `annotations` and `raw_alert` are
stored in full. A new column earns its place only when something needs to
**filter, sort or index** on the value:

- `WHERE severity = 'critical'` — yes, a column.
- "the harness wants to display the runbook link" — no, it is already in
  `annotations`.

`raw_alert` exists precisely so a field you did not promote today can be promoted
later without re-firing the alert.

## 1. The migration

Add a **new** file — `internal/store/migrations/0002_<what>.sql`. Never edit
`0001_event.sql`: an existing database has already run it, and `Migrate` has no
version table to notice a change.

```sql
ALTER TABLE event ADD COLUMN IF NOT EXISTS cluster TEXT;
CREATE INDEX IF NOT EXISTS event_cluster_idx ON event (cluster);
```

Every statement must be idempotent (`IF NOT EXISTS` throughout). Re-running the
migrations is the documented recovery path, and `TestMigrateIsIdempotent` runs
them four times.

**A new column must be nullable or have a default.** `NOT NULL` with no default
fails against a table that already has rows, and the failure surfaces as a
CrashLoopBackOff at startup rather than as a migration error you can read.

> If a migration ever needs to be non-idempotent — a backfill, a destructive
> `ALTER` — stop and add a `schema_version` table first. The current
> no-version-table design is a deliberate trade for a schema that only ever grows
> additively, and it stops being right at exactly that point.

## 2. The Event struct

`internal/event/event.go`:

```go
Cluster string `json:"cluster,omitempty"`
```

**The JSON tag is the queue contract.** Renaming an existing tag renames the
field for every consumer, and nothing will tell you. Adding a field with
`omitempty` is safe: an older consumer ignores what it does not know.

## 3. The projection

`internal/alertmanager/payload.go`, in `Alert.ToEvent`:

```go
Cluster: labels["cluster"],
```

If the value comes from a label that may be spelled several ways, use
`firstNonEmpty(labels, "cluster", "cluster_name")` — rule authors are
inconsistent, and kube-prometheus-stack's own rules are inconsistent with each
other.

## 4. The INSERT — where this goes wrong

`recordEventSQL` in `internal/store/postgres.go` has three parallel lists that
must stay aligned:

1. the column list
2. the `$N` placeholders
3. the argument order in the `RecordEvent` call

```sql
INSERT INTO event (
    event_id, alert_id, ..., raw_alert, received_at, cluster
) VALUES (
    $1, $2, ..., $17, $18, $19
)
```

```go
err := s.pool.QueryRow(ctx, recordEventSQL,
    ev.EventID,        // $1
    ...
    ev.ReceivedAt,     // $18
    nullable(ev.Cluster), // $19  ← added at the END
)
```

**Append at the end. Never insert in the middle.** Inserting a column mid-list
shifts every `$N` after it, and because most of these columns are `TEXT`,
Postgres accepts the shifted arguments happily. You get rows where `summary`
holds the namespace and `description` holds the runbook URL — a silent data
corruption with no error anywhere. It is caught only by reading a row back, which
is why step 6 exists.

Use `nullable(...)` for any optional string. An empty string and an absent value
are the same thing here, and storing `''` makes `WHERE cluster IS NULL` miss rows
that have no cluster.

**A `jsonb` column takes a `string`, never a map or `[]byte`.** Use
`jsonObject(...)` for a `map[string]string`; marshal anything else yourself. This
looks like pedantry in `session` mode, where it works either way. Under
`POSTGRES_POOL_MODE=transaction` pgx sends statements unprepared, never learns
the parameter is jsonb, and falls back to the Go type: a map matches nothing
(`cannot find encode plan`) and `[]byte` matches *bytea*, which text-encodes as
hex and comes back as `invalid input syntax for type json`. `string` is encoded
verbatim by both modes.

## 5. The read path

`GetEvent` in the same file has its own column list and `Scan` argument order,
with the same alignment requirement. It is only used by tests — but those tests
are what prove step 4 was done right, so it cannot be skipped.

Wrap optional columns in `COALESCE(col, '')` to scan into a `string`, or scan
into a `*string`.

## 6. The tests

Three, minimum:

**Unit** — the projection, in `internal/alertmanager/payload_test.go`:

```go
if ev.Cluster != "minikube" {
    t.Errorf("Cluster = %q", ev.Cluster)
}
```

**Integration** — the round trip, in `TestRecordEventRoundTrip`. This is the one
that catches a `$N` misalignment, and the only one that can:

```go
if got.Cluster != ev.Cluster {
    t.Errorf("cluster = %q, want %q", got.Cluster, ev.Cluster)
}
```

Assert on a **distinctive value**, not `"test"`. If every field is `"test"`, a
shifted parameter list passes.

For a `jsonb` column, add the same assertion to
`TestRecordEventInTransactionPoolMode` (or build the store with
`newStoreInMode(t, config.PoolModeTransaction)`). The round-trip test alone runs
in `session` mode only, which is exactly the mode where a wrong parameter type is
invisible.

**Queue** — if consumers will read it, assert it survives in
`TestPublishAndConsume`.

## If you are changing the deduplication key

Do not, without reading this.

`UNIQUE (alert_id, alert_status, starts_at)` is what makes the pipeline safe to
retry, and both directions of getting it wrong are silent:

- **Widening** it (adding `received_at`, or your new column) disables
  deduplication. Every Alertmanager retry and every `repeat_interval`
  re-notification becomes a new event, and the harness solves the same alert
  every four hours.
- **Narrowing** it (dropping `alert_status`) makes a resolution collide with its
  own firing row. The harness never learns the problem went away.

Both compile. Both pass a single-insert test. `TestDeduplication` in
`internal/store/postgres_integration_test.go` covers all four arrival patterns —
retry, resolution, re-fire, different alert — and is the test to run.

Changing the key also means `recordEventSQL`'s `ON CONFLICT (...)` target has to
change to match, or the statement fails at runtime with "there is no unique or
exclusion constraint matching the ON CONFLICT specification".

## If you are adding a status

`internal/event/event.go` and the `CHECK` constraint in the migration are two
halves of one vocabulary:

1. Add the constant and put it in `AllStatuses()`.
2. Add a migration that replaces the constraint:
   ```sql
   ALTER TABLE event DROP CONSTRAINT IF EXISTS event_status_valid;
   ALTER TABLE event ADD CONSTRAINT event_status_valid CHECK (status IN (
       'waiting', 'solving', 'waiting_human_approval',
       'resolved', 'failed_to_resolve', 'your_new_status'
   ));
   ```
3. `TestStatusesMatchMigration` reads `0001_event.sql` specifically. If the
   constraint moves to a later migration, update that test to read the file that
   now defines it — do not delete it.

Spellings are snake_case. The value travels through SQL predicates, AMQP headers
and JSON, all of which make a space awkward.

And remember invariant 2: **this service only writes `waiting`.** A new status is
something you are declaring on behalf of `harness/`, not something this service
will ever set.

## Verify

```bash
make lint
make test                # must pass with nothing running
make test-integration    # the one that catches a $N misalignment
```

Then read an actual row back, because a shifted parameter list is a data bug, not
a test failure:

```sql
SELECT * FROM event ORDER BY received_at DESC LIMIT 1;
```

Check the values are in the columns they belong in.

## Tell the consumer

`harness/` reads these rows and these messages. An additive change with
`omitempty` is safe to ship alone. A rename, a type change, or a change to the
deduplication key is not — note it in `README.md`'s event-table section, which is
where the consumer's author will look.
