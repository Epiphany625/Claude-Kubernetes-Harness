// Package store persists events to PostgreSQL.
package store

import (
	"context"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

// Store is the persistence contract the processor depends on. It is an
// interface so the handler and processor can be unit-tested without a database;
// PostgresStore is the only production implementation.
type Store interface {
	// RecordEvent inserts ev, or returns the existing row if this notification
	// has already been recorded. See RecordResult for why both cases succeed.
	RecordEvent(ctx context.Context, ev event.Event) (RecordResult, error)

	// MarkPublished records that the broker has confirmed the message for
	// eventID. Called only after a confirmed publish.
	MarkPublished(ctx context.Context, eventID string) error

	// Ping reports whether the database is reachable. Used by /readyz only --
	// never by /healthz.
	Ping(ctx context.Context) error
}

// RecordResult is what an insert attempt tells the caller.
//
// The important field is EventID, which is authoritative whether or not the
// insert created a row. On a duplicate, the *existing* row's id comes back, so
// a retried notification republishes with the same message_id the first attempt
// used, and a consumer that deduplicates on message_id recognises it.
type RecordResult struct {
	// EventID of the row now in the table -- freshly inserted or pre-existing.
	EventID string

	// Inserted is false when this notification was already recorded. Not an
	// error: Alertmanager retrying is normal operation, and the correct response
	// to it is to converge on the same row.
	Inserted bool

	// AlreadyPublished is true when the existing row already has published_at
	// set, meaning a previous attempt got a broker confirmation. The processor
	// uses this to skip republishing, which is what keeps ordinary retries from
	// duplicating messages on the queue.
	AlreadyPublished bool
}
