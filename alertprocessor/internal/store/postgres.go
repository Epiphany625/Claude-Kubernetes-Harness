package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

// migrationFS carries the schema into the binary so a fresh database -- a new
// Supabase project, or a throwaway container in the integration suite --
// converges on the same shape without anything external to fetch.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

// PostgresStore is the production Store.
type PostgresStore struct {
	pool         *pgxpool.Pool
	queryTimeout time.Duration
}

var _ Store = (*PostgresStore)(nil)

// Open connects, verifies the connection, and optionally applies the schema.
//
// It returns a closed-over Close rather than requiring the caller to remember
// pool ownership, because a half-open pool on a failed migration is a leak that
// only shows up under crash-looping restarts.
func Open(ctx context.Context, cfg config.PostgresConfig, log *slog.Logger) (*PostgresStore, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres at %s: %w", config.Redacted(cfg.DSN), err)
	}

	s := &PostgresStore{pool: pool, queryTimeout: cfg.QueryTimeout}

	if cfg.AutoMigrate {
		if err := s.Migrate(ctx, log); err != nil {
			pool.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	} else {
		log.Info("schema migration disabled; assuming the event table exists",
			"env", config.EnvPrefix+"DB_AUTO_MIGRATE")
	}

	return s, nil
}

// Close releases the pool.
func (s *PostgresStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Pool exposes the underlying pool for tests that need to assert on rows.
func (s *PostgresStore) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies every embedded .sql file in name order.
//
// There is no migration-version table. The files are written to be idempotent
// (CREATE ... IF NOT EXISTS throughout), so re-running them is the recovery path
// rather than something to guard against. That trade stops being right once a
// migration needs to ALTER or backfill; at that point this grows a schema_version
// table, and the skill in .claude/skills/add-event-field says so.
func (s *PostgresStore) Migrate(ctx context.Context, log *slog.Logger) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		// Each file runs as one implicit transaction, so a file that fails
		// partway leaves no half-applied schema behind.
		if _, err := s.pool.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		log.Debug("migration applied", "file", name)
	}
	log.Info("schema up to date", "migrations", len(names))
	return nil
}

// MigrationSQL returns the embedded text of one migration file, so tests can
// assert the Go constants agree with the SQL rather than trusting that they do.
func MigrationSQL(name string) (string, error) {
	b, err := migrationFS.ReadFile("migrations/" + name)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// recordEventSQL inserts a notification, converging on the existing row when it
// has already been recorded.
//
// Three details carry the weight:
//
//   - ON CONFLICT ... DO UPDATE, not DO NOTHING. DO NOTHING returns no row, so a
//     retry would learn nothing -- not even the event_id it needs in order to
//     republish. The UPDATE is a deliberate no-op touch of updated_at whose only
//     purpose is to make RETURNING fire.
//
//   - The conflict target is the deduplication index, named explicitly by its
//     columns so a schema change that drops the index fails this statement
//     loudly instead of degrading to duplicate rows.
//
//   - `xmax = 0` distinguishes an insert from an update. It reads the tuple's
//     transaction id: zero means this statement created the row. It is the
//     cheapest way to get the answer, and there is no portable alternative.
const recordEventSQL = `
INSERT INTO event (
    event_id, alert_id, status, alert_status, alert_name, severity, namespace,
    resource, summary, description, runbook_url, generator_url,
    starts_at, ends_at, labels, annotations, raw_alert, received_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18
)
ON CONFLICT (alert_id, alert_status, starts_at) DO UPDATE
    SET updated_at = now()
RETURNING event_id, (xmax = 0) AS inserted, published_at IS NOT NULL AS already_published
`

// RecordEvent implements Store.
func (s *PostgresStore) RecordEvent(ctx context.Context, ev event.Event) (RecordResult, error) {
	if !ev.Status.Valid() {
		// Caught here rather than by the CHECK constraint so the error names the
		// field and the allowed values instead of surfacing as SQLSTATE 23514.
		return RecordResult{}, fmt.Errorf("status %q is not a valid event status", ev.Status)
	}

	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	// The jsonb parameters are serialised to string so pgx encodes them verbatim.
	labels, err := jsonObject(ev.Labels)
	if err != nil {
		return RecordResult{}, fmt.Errorf("encode labels for alert %s: %w", ev.AlertID, err)
	}
	annotations, err := jsonObject(ev.Annotations)
	if err != nil {
		return RecordResult{}, fmt.Errorf("encode annotations for alert %s: %w", ev.AlertID, err)
	}
	raw := string(ev.RawAlert)
	if raw == "" {
		raw = "{}"
	}

	var out RecordResult
	err = s.pool.QueryRow(ctx, recordEventSQL,
		ev.EventID,
		ev.AlertID,
		string(ev.Status),
		string(ev.AlertState),
		ev.AlertName,
		nullable(ev.Severity),
		nullable(ev.Namespace),
		nullable(ev.Resource),
		nullable(ev.Summary),
		nullable(ev.Description),
		nullable(ev.RunbookURL),
		nullable(ev.GeneratorURL),
		ev.StartsAt,
		ev.EndsAt,
		labels,
		annotations,
		raw,
		ev.ReceivedAt,
	).Scan(&out.EventID, &out.Inserted, &out.AlreadyPublished)
	if err != nil {
		return RecordResult{}, fmt.Errorf("record event for alert %s: %w", ev.AlertID, err)
	}
	return out, nil
}

// MarkPublished implements Store.
func (s *PostgresStore) MarkPublished(ctx context.Context, eventID string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	tag, err := s.pool.Exec(ctx,
		`UPDATE event SET published_at = now(), updated_at = now() WHERE event_id = $1`,
		eventID)
	if err != nil {
		return fmt.Errorf("mark event %s published: %w", eventID, err)
	}
	if tag.RowsAffected() == 0 {
		// The message is already on the queue at this point, so this is not
		// recoverable by retrying -- but it does mean the row will look
		// unpublished forever, so it must not pass silently.
		return fmt.Errorf("mark event %s published: no such row", eventID)
	}
	return nil
}

// Ping implements Store.
func (s *PostgresStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres unreachable: %w", err)
	}
	return nil
}

// GetEvent reads one row back. Not part of the Store interface -- this service
// never reads events during normal operation, that is harness/'s job -- but the
// integration suite needs it to assert what was actually written.
func (s *PostgresStore) GetEvent(ctx context.Context, eventID string) (event.Event, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	const q = `
SELECT event_id, alert_id, status, alert_status, alert_name,
       COALESCE(severity, ''), COALESCE(namespace, ''), COALESCE(resource, ''),
       COALESCE(summary, ''), COALESCE(description, ''),
       COALESCE(runbook_url, ''), COALESCE(generator_url, ''),
       starts_at, ends_at, labels, annotations, raw_alert, received_at
  FROM event WHERE event_id = $1`

	var ev event.Event
	var status, alertStatus string
	err := s.pool.QueryRow(ctx, q, eventID).Scan(
		&ev.EventID, &ev.AlertID, &status, &alertStatus, &ev.AlertName,
		&ev.Severity, &ev.Namespace, &ev.Resource,
		&ev.Summary, &ev.Description,
		&ev.RunbookURL, &ev.GeneratorURL,
		&ev.StartsAt, &ev.EndsAt, &ev.Labels, &ev.Annotations, &ev.RawAlert, &ev.ReceivedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return event.Event{}, false, nil
	}
	if err != nil {
		return event.Event{}, false, fmt.Errorf("get event %s: %w", eventID, err)
	}
	ev.Status = event.Status(status)
	ev.AlertState = event.AlertState(alertStatus)
	return ev, true, nil
}

// jsonObject renders a string map as a JSON object for a NOT NULL jsonb column.
//
// The empty case has to be spelled out: json.Marshal of a nil map is the literal
// `null`, which the column would accept -- jsonb null is a value, not SQL NULL --
// leaving rows where `labels->>'severity'` and `labels IS NULL` are both
// useless. `{}` is what an alert with no labels means.
func jsonObject(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// nullable maps "" to SQL NULL. An empty string and an absent value are the same
// thing for every optional column here, and storing ” would make
// `WHERE severity IS NULL` miss rows that have no severity.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
