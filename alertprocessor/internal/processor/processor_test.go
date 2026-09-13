package processor_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/processor"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/queue"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// fakeStore records calls and lets each test decide what RecordEvent returns.
type fakeStore struct {
	mu sync.Mutex

	recordFn func(event.Event) (store.RecordResult, error)

	recorded      []event.Event
	markedIDs     []string
	markErr       error
	pingErr       error
	markCallCount int
}

func (f *fakeStore) RecordEvent(_ context.Context, ev event.Event) (store.RecordResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded = append(f.recorded, ev)
	if f.recordFn != nil {
		return f.recordFn(ev)
	}
	return store.RecordResult{EventID: ev.EventID, Inserted: true}, nil
}

func (f *fakeStore) MarkPublished(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markCallCount++
	if f.markErr != nil {
		return f.markErr
	}
	f.markedIDs = append(f.markedIDs, id)
	return nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) marked() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.markedIDs...)
}

// fakePublisher records what was published and can be told to fail.
type fakePublisher struct {
	mu sync.Mutex

	err       error
	published []event.Event
}

func (f *fakePublisher) Publish(_ context.Context, ev event.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, ev)
	return nil
}

func (f *fakePublisher) Ping(context.Context) error { return nil }

func (f *fakePublisher) sent() []event.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.published...)
}

func sampleEvent() event.Event {
	return event.Event{
		EventID:     "11111111-1111-1111-1111-111111111111",
		AlertID:     "a1b2c3d4e5f60718",
		Status:      event.StatusWaiting,
		AlertState:  event.AlertFiring,
		AlertName:   "KubePodCrashLooping",
		Severity:    "warning",
		Namespace:   "ckh",
		Resource:    "pod/worker-0",
		StartsAt:    time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC),
		ReceivedAt:  time.Date(2026, 9, 13, 10, 1, 0, 0, time.UTC),
		Labels:      map[string]string{"alertname": "KubePodCrashLooping"},
		Annotations: map[string]string{},
	}
}

func TestProcessHappyPath(t *testing.T) {
	st := &fakeStore{}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if out.Failed() {
		t.Fatalf("unexpected failure at %s: %v", out.Stage, out.Err)
	}
	if !out.Recorded || !out.Published {
		t.Errorf("Recorded=%v Published=%v, want both true", out.Recorded, out.Published)
	}
	if len(st.recorded) != 1 {
		t.Fatalf("recorded %d events, want 1", len(st.recorded))
	}
	if st.recorded[0].Status != event.StatusWaiting {
		t.Errorf("recorded status %q, want waiting", st.recorded[0].Status)
	}
	if sent := pub.sent(); len(sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(sent))
	}
	if marked := st.marked(); len(marked) != 1 || marked[0] != out.EventID {
		t.Errorf("MarkPublished called with %v, want [%s]", marked, out.EventID)
	}
}

// The ordering that the whole design rests on: a message referencing a row that
// does not exist would leave the harness with a dangling event_id.
func TestProcessRecordsBeforePublishing(t *testing.T) {
	var order []string
	var mu sync.Mutex
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }

	st := &fakeStore{recordFn: func(ev event.Event) (store.RecordResult, error) {
		note("record")
		return store.RecordResult{EventID: ev.EventID, Inserted: true}, nil
	}}
	pub := &publisherSpy{onPublish: func() { note("publish") }}

	p := processor.New(st, pub, nil, discardLogger())
	p.Process(context.Background(), sampleEvent())

	if len(order) != 2 || order[0] != "record" || order[1] != "publish" {
		t.Errorf("call order = %v, want [record publish]", order)
	}
}

type publisherSpy struct {
	onPublish func()
	err       error
}

func (p *publisherSpy) Publish(context.Context, event.Event) error {
	if p.onPublish != nil {
		p.onPublish()
	}
	return p.err
}
func (p *publisherSpy) Ping(context.Context) error { return nil }

// A duplicate is Alertmanager retrying, which is normal operation. The pipeline
// has to converge on the existing row rather than accumulate work.
func TestProcessDuplicateNotYetPublished(t *testing.T) {
	const existingID = "99999999-9999-9999-9999-999999999999"

	st := &fakeStore{recordFn: func(event.Event) (store.RecordResult, error) {
		return store.RecordResult{EventID: existingID, Inserted: false, AlreadyPublished: false}, nil
	}}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if out.Failed() {
		t.Fatalf("a duplicate must not be an error: %v", out.Err)
	}
	if out.Recorded {
		t.Error("Recorded should be false for a notification already in the table")
	}
	// The row exists but was never handed off -- a previous attempt died between
	// the insert and the confirm. Publishing now is the recovery.
	if !out.Published {
		t.Error("an unpublished duplicate must still be published")
	}
	// The critical assertion: the message carries the EXISTING row's id, not the
	// freshly generated one. Otherwise message_id names a row nobody can find.
	if out.EventID != existingID {
		t.Errorf("EventID = %q, want the existing row's id %q", out.EventID, existingID)
	}
	sent := pub.sent()
	if len(sent) != 1 {
		t.Fatalf("published %d messages, want 1", len(sent))
	}
	if sent[0].EventID != existingID {
		t.Errorf("published event_id = %q, want %q", sent[0].EventID, existingID)
	}
}

// This is what stops Alertmanager's repeat_interval from putting the same event
// on the queue every four hours.
func TestProcessSkipsAlreadyPublished(t *testing.T) {
	st := &fakeStore{recordFn: func(event.Event) (store.RecordResult, error) {
		return store.RecordResult{
			EventID:          "22222222-2222-2222-2222-222222222222",
			Inserted:         false,
			AlreadyPublished: true,
		}, nil
	}}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if out.Failed() {
		t.Fatalf("unexpected failure: %v", out.Err)
	}
	if out.Published {
		t.Error("Published should be false when a previous attempt already delivered it")
	}
	if n := len(pub.sent()); n != 0 {
		t.Errorf("published %d messages, want 0 -- this is the duplicate-delivery guard", n)
	}
	if n := st.markCallCount; n != 0 {
		t.Errorf("MarkPublished called %d times, want 0", n)
	}
}

func TestProcessRecordFailure(t *testing.T) {
	wantErr := errors.New("connection refused")
	st := &fakeStore{recordFn: func(event.Event) (store.RecordResult, error) {
		return store.RecordResult{}, wantErr
	}}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if !out.Failed() {
		t.Fatal("expected a failure")
	}
	if out.Stage != processor.StageRecord {
		t.Errorf("Stage = %q, want record", out.Stage)
	}
	if !errors.Is(out.Err, wantErr) {
		t.Errorf("Err = %v, want it to wrap %v", out.Err, wantErr)
	}
	// Nothing may reach the queue when the row does not exist.
	if n := len(pub.sent()); n != 0 {
		t.Errorf("published %d messages after a failed insert, want 0", n)
	}
}

func TestProcessPublishFailure(t *testing.T) {
	st := &fakeStore{}
	pub := &fakePublisher{err: errors.New("broker unreachable")}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if !out.Failed() {
		t.Fatal("expected a failure")
	}
	if out.Stage != processor.StagePublish {
		t.Errorf("Stage = %q, want publish", out.Stage)
	}
	// The row stays, with published_at NULL. That is the recoverable failure the
	// insert-first ordering buys.
	if len(st.recorded) != 1 {
		t.Error("the event should still have been recorded")
	}
	if n := len(st.marked()); n != 0 {
		t.Errorf("published_at was set despite the publish failing (%d calls)", n)
	}
}

func TestProcessUnroutableIsReported(t *testing.T) {
	st := &fakeStore{}
	pub := &fakePublisher{err: fmt.Errorf("%w: no queue bound", queue.ErrUnroutable)}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if !out.Failed() || out.Stage != processor.StagePublish {
		t.Fatalf("expected a publish failure, got stage=%q err=%v", out.Stage, out.Err)
	}
	// Distinguishable by the caller, because it is a topology problem that
	// retrying will never fix.
	if !errors.Is(out.Err, queue.ErrUnroutable) {
		t.Errorf("Err = %v, want it to wrap ErrUnroutable", out.Err)
	}
}

// The message is already on the queue; only the bookkeeping failed. Reporting
// failure makes Alertmanager retry and send a second copy, which is why message
// consumers must deduplicate on message_id -- but succeeding silently would
// leave the row looking unpublished forever.
func TestProcessMarkPublishedFailure(t *testing.T) {
	st := &fakeStore{markErr: errors.New("write conflict")}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	out := p.Process(context.Background(), sampleEvent())

	if !out.Failed() {
		t.Fatal("a failed MarkPublished must not be swallowed")
	}
	if out.Stage != processor.StageMarkPublished {
		t.Errorf("Stage = %q, want mark_published", out.Stage)
	}
	if !out.Published {
		t.Error("Published should be true: the message did reach the broker")
	}
}

// One unlucky alert in a group of twenty must not cost the other nineteen their
// processing.
func TestProcessBatchContinuesPastFailures(t *testing.T) {
	failFor := "bad-alert"
	st := &fakeStore{recordFn: func(ev event.Event) (store.RecordResult, error) {
		if ev.AlertID == failFor {
			return store.RecordResult{}, errors.New("constraint violation")
		}
		return store.RecordResult{EventID: ev.EventID, Inserted: true}, nil
	}}
	pub := &fakePublisher{}
	p := processor.New(st, pub, nil, discardLogger())

	events := make([]event.Event, 4)
	for i := range events {
		events[i] = sampleEvent()
		events[i].AlertID = fmt.Sprintf("alert-%d", i)
		events[i].EventID = fmt.Sprintf("id-%d", i)
	}
	events[2].AlertID = failFor

	outcomes := p.ProcessBatch(context.Background(), events)

	if len(outcomes) != 4 {
		t.Fatalf("got %d outcomes for 4 alerts", len(outcomes))
	}
	for i, out := range outcomes {
		if i == 2 {
			if !out.Failed() {
				t.Errorf("outcome %d should have failed", i)
			}
			continue
		}
		if out.Failed() {
			t.Errorf("outcome %d failed because a sibling did: %v", i, out.Err)
		}
	}
	if n := len(pub.sent()); n != 3 {
		t.Errorf("published %d of 3 healthy alerts", n)
	}
}

func TestSummarize(t *testing.T) {
	outcomes := []processor.Outcome{
		{EventID: "1", Recorded: true, Published: true},
		{EventID: "2", Recorded: false, Published: true},  // duplicate, republished
		{EventID: "3", Recorded: false, Published: false}, // already published
		{EventID: "4", AlertID: "a4", Stage: processor.StageRecord, Err: errors.New("boom")},
	}

	s := processor.Summarize(outcomes)

	if s.Total != 4 {
		t.Errorf("Total = %d", s.Total)
	}
	if s.Recorded != 1 {
		t.Errorf("Recorded = %d, want 1", s.Recorded)
	}
	if s.Duplicate != 2 {
		t.Errorf("Duplicate = %d, want 2", s.Duplicate)
	}
	if s.Published != 2 {
		t.Errorf("Published = %d, want 2", s.Published)
	}
	if s.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1", s.Skipped)
	}
	if s.Failed != 1 {
		t.Errorf("Failed = %d, want 1", s.Failed)
	}
	if len(s.Errors) != 1 || !strings.Contains(s.Errors[0], "a4") {
		t.Errorf("Errors = %v, want one entry naming alert a4", s.Errors)
	}
}

// A node failure alerting on three hundred pods must not produce a
// three-hundred-line response body to a sender that is going to retry anyway.
func TestSummarizeCapsErrors(t *testing.T) {
	outcomes := make([]processor.Outcome, 50)
	for i := range outcomes {
		outcomes[i] = processor.Outcome{
			AlertID: fmt.Sprintf("a%d", i),
			Stage:   processor.StagePublish,
			Err:     errors.New("broker unreachable"),
		}
	}

	s := processor.Summarize(outcomes)

	if s.Failed != 50 {
		t.Errorf("Failed = %d, want 50", s.Failed)
	}
	if len(s.Errors) > 11 {
		t.Errorf("Errors has %d entries; it should be capped", len(s.Errors))
	}
	last := s.Errors[len(s.Errors)-1]
	if !strings.Contains(last, "40 more") {
		t.Errorf("last error entry should say how many were elided, got %q", last)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	s := processor.Summarize(nil)
	if s.Total != 0 || s.Failed != 0 || len(s.Errors) != 0 {
		t.Errorf("empty batch produced %+v", s)
	}
}
