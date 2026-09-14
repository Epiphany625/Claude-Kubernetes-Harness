//go:build integration

// Integration tests for the Postgres store, against a real PostgreSQL in a
// throwaway container.
//
// Behind a build tag so `go test ./...` needs nothing running. Run with:
//
//	go test -tags=integration ./internal/store/...
//
// What these cover that the unit tests cannot: the SQL itself. The ON CONFLICT
// clause, the xmax trick, JSONB round-tripping and -- above all -- whether the
// deduplication index deduplicates the things it should and lets through the
// things it should.
package store_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// newStore starts one Postgres container for the test and returns a store with
// the schema applied. The container is torn down by t.Cleanup.
func newStore(t *testing.T) *store.PostgresStore {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("events"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container (is Docker running?): %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	s, err := store.Open(ctx, config.PostgresConfig{
		DSN:            dsn,
		MaxConns:       5,
		ConnectTimeout: 15 * time.Second,
		QueryTimeout:   10 * time.Second,
		AutoMigrate:    true,
	}, quietLogger())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func sampleEvent(alertID string, state event.AlertState, startsAt time.Time) event.Event {
	return event.Event{
		// A fresh id per call, mirroring the projection: every delivery of the
		// same notification arrives carrying a different event_id, and the store
		// is what decides which one is authoritative.
		EventID:    uuid.NewString(),
		AlertID:    alertID,
		Status:     event.StatusWaiting,
		AlertState: state,
		AlertName:  "KubePodCrashLooping",
		Severity:   "warning",
		Namespace:  "ckh",
		Resource:   "pod/worker-0",
		Summary:    "Pod is crash looping.",
		StartsAt:   startsAt,
		ReceivedAt: time.Now().UTC(),
		Labels: map[string]string{
			"alertname": "KubePodCrashLooping",
			"namespace": "ckh",
			"pod":       "worker-0",
		},
		Annotations: map[string]string{"summary": "Pod is crash looping."},
		RawAlert:    json.RawMessage(`{"status":"firing","labels":{"alertname":"KubePodCrashLooping"}}`),
	}
}

// ---------------------------------------------------------------------------

func TestMigrateIsIdempotent(t *testing.T) {
	s := newStore(t)
	// Re-running the migration is the documented recovery path, so it has to be
	// safe. Open already ran it once.
	for i := 0; i < 3; i++ {
		if err := s.Migrate(context.Background(), quietLogger()); err != nil {
			t.Fatalf("re-running the migration failed on attempt %d: %v", i+2, err)
		}
	}
}

func TestRecordEventRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	ev := sampleEvent("fp-roundtrip", event.AlertFiring, time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC))
	res, err := s.RecordEvent(ctx, ev)
	if err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	if !res.Inserted {
		t.Error("first insert should report Inserted")
	}
	if res.AlreadyPublished {
		t.Error("a fresh row cannot already be published")
	}
	if res.EventID != ev.EventID {
		t.Errorf("EventID = %q, want %q", res.EventID, ev.EventID)
	}

	got, found, err := s.GetEvent(ctx, res.EventID)
	if err != nil || !found {
		t.Fatalf("GetEvent: found=%v err=%v", found, err)
	}

	if got.Status != event.StatusWaiting {
		t.Errorf("status = %q, want waiting", got.Status)
	}
	if got.AlertState != event.AlertFiring {
		t.Errorf("alert_status = %q, want firing", got.AlertState)
	}
	if got.AlertID != "fp-roundtrip" {
		t.Errorf("alert_id = %q", got.AlertID)
	}
	if got.Resource != "pod/worker-0" {
		t.Errorf("resource = %q", got.Resource)
	}
	if !got.StartsAt.Equal(ev.StartsAt) {
		t.Errorf("starts_at = %v, want %v", got.StartsAt, ev.StartsAt)
	}
	// JSONB must survive intact -- the harness reads these to decide what to do.
	if got.Labels["pod"] != "worker-0" || len(got.Labels) != 3 {
		t.Errorf("labels did not round-trip: %v", got.Labels)
	}
	if got.Annotations["summary"] != "Pod is crash looping." {
		t.Errorf("annotations did not round-trip: %v", got.Annotations)
	}
	var raw map[string]any
	if err := json.Unmarshal(got.RawAlert, &raw); err != nil {
		t.Errorf("raw_alert is not valid JSON: %v", err)
	}
	if got.EndsAt != nil {
		t.Errorf("ends_at = %v, want NULL for a firing alert", *got.EndsAt)
	}
}

// The heart of the deduplication design. Each subtest is one arrival pattern
// Alertmanager actually produces.
func TestDeduplication(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	firedAt := time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

	// First arrival.
	first, err := s.RecordEvent(ctx, sampleEvent("fp-dedup", event.AlertFiring, firedAt))
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !first.Inserted {
		t.Fatal("the first arrival must insert")
	}

	t.Run("a webhook retry converges on the same row", func(t *testing.T) {
		// Same alert, same state, same start -- a different event_id, because
		// the projection generates one per delivery.
		retry := sampleEvent("fp-dedup", event.AlertFiring, firedAt)
		res, err := s.RecordEvent(ctx, retry)
		if err != nil {
			t.Fatalf("retry insert: %v", err)
		}
		if res.Inserted {
			t.Error("a retry must not create a second row")
		}
		// The critical property: the EXISTING row's id comes back, so the
		// republished message names a row that exists.
		if res.EventID != first.EventID {
			t.Errorf("EventID = %q, want the existing row's %q", res.EventID, first.EventID)
		}
		if res.EventID == retry.EventID {
			t.Error("the freshly generated id was used instead of the stored one")
		}
	})

	t.Run("a resolution is a new event", func(t *testing.T) {
		// Only alert_status differs. Deduplicating this would mean the harness
		// never learns the problem went away.
		res, err := s.RecordEvent(ctx, sampleEvent("fp-dedup", event.AlertResolved, firedAt))
		if err != nil {
			t.Fatalf("resolve insert: %v", err)
		}
		if !res.Inserted {
			t.Error("a resolution must create its own row")
		}
		if res.EventID == first.EventID {
			t.Error("the resolution reused the firing row")
		}
	})

	t.Run("a re-fire after resolving is a new event", func(t *testing.T) {
		// Same alert, firing again, but with a later startsAt.
		refiredAt := firedAt.Add(4 * time.Hour)
		res, err := s.RecordEvent(ctx, sampleEvent("fp-dedup", event.AlertFiring, refiredAt))
		if err != nil {
			t.Fatalf("re-fire insert: %v", err)
		}
		if !res.Inserted {
			t.Error("a re-fire with a later startsAt must create a new row")
		}
	})

	t.Run("a different alert is a new event", func(t *testing.T) {
		res, err := s.RecordEvent(ctx, sampleEvent("fp-other", event.AlertFiring, firedAt))
		if err != nil {
			t.Fatalf("other-alert insert: %v", err)
		}
		if !res.Inserted {
			t.Error("a different alert_id must create a new row")
		}
	})
}

// AlreadyPublished is what stops repeat_interval re-notifications from putting
// the same event on the queue every four hours.
func TestAlreadyPublishedSurfacesOnDuplicate(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	firedAt := time.Date(2026, 9, 13, 11, 0, 0, 0, time.UTC)
	first, err := s.RecordEvent(ctx, sampleEvent("fp-published", event.AlertFiring, firedAt))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Before marking: a duplicate should say "not yet published", so the caller
	// republishes and heals a run that died between insert and confirm.
	dup, err := s.RecordEvent(ctx, sampleEvent("fp-published", event.AlertFiring, firedAt))
	if err != nil {
		t.Fatalf("duplicate insert: %v", err)
	}
	if dup.AlreadyPublished {
		t.Error("AlreadyPublished is true before anything was published")
	}

	if err := s.MarkPublished(ctx, first.EventID); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}

	// After marking: the same duplicate must now short-circuit.
	dup2, err := s.RecordEvent(ctx, sampleEvent("fp-published", event.AlertFiring, firedAt))
	if err != nil {
		t.Fatalf("duplicate insert after publish: %v", err)
	}
	if !dup2.AlreadyPublished {
		t.Error("AlreadyPublished is false after MarkPublished; " +
			"repeat notifications would republish the same event forever")
	}
	if dup2.Inserted {
		t.Error("the duplicate created a row")
	}
}

func TestMarkPublishedUnknownRow(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	// The message is on the queue by the time this runs, so it is not
	// recoverable -- but it must not pass silently, or the row looks
	// unpublished forever and a replay job sends a third copy.
	err := s.MarkPublished(ctx, "00000000-0000-4000-8000-000000000000")
	if err == nil {
		t.Fatal("MarkPublished on a nonexistent row returned nil")
	}
}

// The CHECK constraint is the last line of defence for the status vocabulary.
// RecordEvent rejects bad values first, with a better message.
func TestInvalidStatusIsRejected(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	ev := sampleEvent("fp-badstatus", event.AlertFiring, time.Now().UTC())
	ev.Status = "definitely not a status"

	_, err := s.RecordEvent(ctx, ev)
	if err == nil {
		t.Fatal("an invalid status was accepted")
	}
}

// Concurrent deliveries of the same notification -- two replicas, or
// Alertmanager retrying while the first attempt is still running. Exactly one
// insert must win and both callers must agree on the event_id.
func TestConcurrentDuplicatesConvergeOnOneRow(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	const n = 8
	firedAt := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	type result struct {
		res store.RecordResult
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() {
			res, err := s.RecordEvent(ctx, sampleEvent("fp-concurrent", event.AlertFiring, firedAt))
			results <- result{res, err}
		}()
	}

	inserted := 0
	ids := map[string]bool{}
	for i := 0; i < n; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent RecordEvent: %v", r.err)
		}
		if r.res.Inserted {
			inserted++
		}
		ids[r.res.EventID] = true
	}

	if inserted != 1 {
		t.Errorf("%d of %d concurrent inserts reported Inserted, want exactly 1", inserted, n)
	}
	if len(ids) != 1 {
		t.Errorf("callers disagree on the event_id: %v", ids)
	}
}

func TestPing(t *testing.T) {
	s := newStore(t)
	if err := s.Ping(context.Background()); err != nil {
		t.Errorf("Ping on a healthy database: %v", err)
	}
}

// An alert can legitimately carry no labels or no annotations. A nil map
// marshals to the JSON literal `null`, which a jsonb column accepts -- jsonb
// null is a value, not SQL NULL -- leaving a row where neither `labels->>'x'`
// nor `labels IS NULL` is useful to the harness. `{}` is the intended shape.
func TestEmptyLabelsStoreAsEmptyObject(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	ev := sampleEvent("fp-emptymaps", event.AlertFiring, time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC))
	ev.Labels = nil
	ev.Annotations = map[string]string{}

	res, err := s.RecordEvent(ctx, ev)
	if err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}

	var labelsType, annotationsType string
	err = s.Pool().QueryRow(ctx,
		`SELECT jsonb_typeof(labels), jsonb_typeof(annotations) FROM event WHERE event_id = $1`,
		res.EventID).Scan(&labelsType, &annotationsType)
	if err != nil {
		t.Fatalf("read jsonb_typeof: %v", err)
	}
	if labelsType != "object" {
		t.Errorf("labels stored as jsonb %s, want object", labelsType)
	}
	if annotationsType != "object" {
		t.Errorf("annotations stored as jsonb %s, want object", annotationsType)
	}

	got, found, err := s.GetEvent(ctx, res.EventID)
	if err != nil || !found {
		t.Fatalf("GetEvent: found=%v err=%v", found, err)
	}
	if len(got.Labels) != 0 || got.Labels == nil {
		t.Errorf("labels = %#v, want an empty non-nil map", got.Labels)
	}
}
