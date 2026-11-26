package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/jackc/pgx/v5"
)

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

const getEventSQL = `
SELECT event_id, alert_id, COALESCE(session_id::text, ''), status, alert_status, alert_name,
       COALESCE(severity, ''), COALESCE(namespace, ''), COALESCE(resource, ''),
       COALESCE(summary, ''), COALESCE(description, ''),
       COALESCE(runbook_url, ''), COALESCE(generator_url, ''),
       starts_at, ends_at, labels, annotations, raw_alert, received_at
  FROM event WHERE event_id = $1`

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

// GetEvent implements Store.
func (s *PostgresStore) GetEvent(ctx context.Context, eventID string) (event.Event, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	var ev event.Event
	var status, alertStatus string
	err := s.pool.QueryRow(ctx, getEventSQL, eventID).Scan(
		&ev.EventID, &ev.AlertID, &ev.SessionID, &status, &alertStatus, &ev.AlertName,
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

