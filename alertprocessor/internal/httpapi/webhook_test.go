package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/httpapi"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/processor"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakeStore struct {
	mu       sync.Mutex
	recordFn func(event.Event) (store.RecordResult, error)
	recorded []event.Event
	pingErr  error
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

func (f *fakeStore) MarkPublished(context.Context, string) error { return nil }
func (f *fakeStore) Ping(context.Context) error                  { return f.pingErr }

func (f *fakeStore) events() []event.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.recorded...)
}

type fakePublisher struct {
	mu        sync.Mutex
	err       error
	pingErr   error
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

func (f *fakePublisher) Ping(context.Context) error { return f.pingErr }

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type harness struct {
	server *httpapi.Server
	store  *fakeStore
	pub    *fakePublisher
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	st := &fakeStore{}
	pub := &fakePublisher{}
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))

	cfg := config.HTTPConfig{
		Addr:         ":0",
		WebhookPath:  "/api/v1/alerts",
		MaxBodyBytes: 1 << 20,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	srv := httpapi.New(cfg, processor.New(st, pub, log), map[string]httpapi.Pinger{
		"postgres": st,
		"rabbitmq": pub,
	}, log)

	return &harness{server: srv, store: st, pub: pub}
}

func (h *harness) post(t *testing.T, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/alerts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.server.Handler().ServeHTTP(rec, req)
	return rec
}

func (h *harness) get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.server.Handler().ServeHTTP(rec, req)
	return rec
}

func testdata(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "test", "testdata", name))
	if err != nil {
		t.Fatalf("read testdata %s: %v", name, err)
	}
	return string(b)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestWebhookHappyPath(t *testing.T) {
	h := newHarness(t)

	rec := h.post(t, testdata(t, "firing-batch.json"), nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}

	var summary processor.Summary
	if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
		t.Fatalf("response is not a Summary: %v (%s)", err, rec.Body)
	}
	// One POST, two alerts, two events. Treating a delivery as one alert is the
	// failure this asserts against.
	if summary.Total != 2 || summary.Published != 2 {
		t.Errorf("summary = %+v, want 2 total / 2 published", summary)
	}
	if got := len(h.store.events()); got != 2 {
		t.Errorf("recorded %d events, want 2", got)
	}
	if got := h.pub.count(); got != 2 {
		t.Errorf("published %d messages, want 2", got)
	}
	// Every event this service writes carries exactly one status.
	for _, ev := range h.store.events() {
		if ev.Status != event.StatusWaiting {
			t.Errorf("event %s has status %q, want waiting", ev.EventID, ev.Status)
		}
	}
}

// Alertmanager retries a 5xx and gives up on a 4xx. Each code below is chosen
// for which of those two behaviours is correct.
func TestWebhookStatusCodes(t *testing.T) {
	t.Run("malformed body is 400, because retrying will not make it parse", func(t *testing.T) {
		h := newHarness(t)
		rec := h.post(t, `{"version":"4","alerts":`, nil)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("empty batch is 200", func(t *testing.T) {
		// Not from Alertmanager -- it does not send empty batches -- so there is
		// nothing to retry and nothing to record.
		h := newHarness(t)
		rec := h.post(t, `{"version":"4","alerts":[]}`, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
		if n := len(h.store.events()); n != 0 {
			t.Errorf("recorded %d events for an empty batch", n)
		}
	})

	t.Run("dependency failure is 500, so Alertmanager retries", func(t *testing.T) {
		h := newHarness(t)
		h.store.recordFn = func(event.Event) (store.RecordResult, error) {
			return store.RecordResult{}, errors.New("connection refused")
		}
		rec := h.post(t, testdata(t, "firing-batch.json"), nil)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", rec.Code)
		}
		var body struct {
			Error   string             `json:"error"`
			Summary *processor.Summary `json:"summary"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode error body: %v", err)
		}
		if body.Summary == nil || body.Summary.Failed != 2 {
			t.Errorf("error body should report the failures: %s", rec.Body)
		}
	})

	t.Run("GET is 405", func(t *testing.T) {
		// Method-qualified routing means the handler never sees it.
		h := newHarness(t)
		rec := h.get(t, "/api/v1/alerts")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want 405", rec.Code)
		}
	})

	t.Run("unknown path is 404", func(t *testing.T) {
		h := newHarness(t)
		if rec := h.get(t, "/nope"); rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})
}

// A partial failure must not be rounded up to success: the alerts that failed
// would be dropped with nothing anywhere recording that it happened.
func TestWebhookPartialFailureIs500(t *testing.T) {
	h := newHarness(t)
	h.store.recordFn = func(ev event.Event) (store.RecordResult, error) {
		if strings.HasSuffix(ev.AlertID, "1829") {
			return store.RecordResult{}, errors.New("deadlock detected")
		}
		return store.RecordResult{EventID: ev.EventID, Inserted: true}, nil
	}

	rec := h.post(t, testdata(t, "firing-batch.json"), nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 so the batch is retried", rec.Code)
	}
	// The healthy alert still went through; the retry will short-circuit on it.
	if n := h.pub.count(); n != 1 {
		t.Errorf("published %d messages, want 1 (the alert that succeeded)", n)
	}
}

func TestWebhookRejectsOversizedBody(t *testing.T) {
	h := newHarness(t)
	// Well over the 1 MiB harness limit.
	huge := `{"version":"4","alerts":[{"status":"firing","labels":{"alertname":"A","pad":"` +
		strings.Repeat("x", 2<<20) + `"}}]}`

	rec := h.post(t, huge, nil)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if n := len(h.store.events()); n != 0 {
		t.Errorf("recorded %d events from an oversized body", n)
	}
}

// A publish failure after a successful insert leaves the row with published_at
// NULL and must be reported, so the sender retries and the row is reconciled.
func TestWebhookPublishFailureAfterInsert(t *testing.T) {
	h := newHarness(t)
	h.pub.err = errors.New("broker unreachable")

	rec := h.post(t, testdata(t, "resolved-single.json"), nil)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if n := len(h.store.events()); n != 1 {
		t.Errorf("recorded %d events; the insert should still have happened", n)
	}
}

// A repeat delivery of an event already on the queue must not put it there
// again, and must still answer 200 so Alertmanager stops retrying.
func TestWebhookAlreadyPublishedIsSuccessWithoutRepublishing(t *testing.T) {
	h := newHarness(t)
	h.store.recordFn = func(ev event.Event) (store.RecordResult, error) {
		return store.RecordResult{EventID: ev.EventID, Inserted: false, AlreadyPublished: true}, nil
	}

	rec := h.post(t, testdata(t, "firing-batch.json"), nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if n := h.pub.count(); n != 0 {
		t.Errorf("published %d messages, want 0", n)
	}

	var summary processor.Summary
	if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.Skipped != 2 || summary.Duplicate != 2 {
		t.Errorf("summary = %+v, want 2 skipped / 2 duplicate", summary)
	}
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

// Liveness must never touch a dependency: a database blip would otherwise
// restart every replica at once.
func TestHealthzIgnoresDependencies(t *testing.T) {
	h := newHarness(t)
	h.store.pingErr = errors.New("postgres is down")
	h.pub.pingErr = errors.New("rabbitmq is down")

	rec := h.get(t, "/healthz")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 even with both dependencies down", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	t.Run("all up", func(t *testing.T) {
		h := newHarness(t)
		rec := h.get(t, "/readyz")
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200 (body: %s)", rec.Code, rec.Body)
		}
	})

	t.Run("one down is not ready, and says which", func(t *testing.T) {
		h := newHarness(t)
		h.store.pingErr = errors.New("connection refused")

		rec := h.get(t, "/readyz")

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		var body struct {
			Status       string `json:"status"`
			Dependencies map[string]struct {
				Status string `json:"status"`
				Error  string `json:"error"`
			} `json:"dependencies"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Dependencies["postgres"].Status != "down" {
			t.Errorf("postgres should be reported down: %s", rec.Body)
		}
		if body.Dependencies["rabbitmq"].Status != "up" {
			t.Errorf("rabbitmq should still be up: %s", rec.Body)
		}
	})
}
