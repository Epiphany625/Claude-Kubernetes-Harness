package store

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
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
