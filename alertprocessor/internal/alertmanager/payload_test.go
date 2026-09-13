package alertmanager_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/alertmanager"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

const noLimit = 1 << 20

func testdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "test", "testdata", name))
	if err != nil {
		t.Fatalf("read testdata %s: %v", name, err)
	}
	return b
}

func TestDecodeFiringBatch(t *testing.T) {
	p, err := alertmanager.Decode(strings.NewReader(string(testdata(t, "firing-batch.json"))), noLimit)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if p.Version != "4" {
		t.Errorf("Version = %q, want 4", p.Version)
	}
	// The property that matters: one POST, many alerts. Treating a delivery as
	// one alert is the mistake that loses eleven pods out of twelve.
	if len(p.Alerts) != 2 {
		t.Fatalf("got %d alerts, want 2", len(p.Alerts))
	}
	if p.Receiver != "alertprocessor" {
		t.Errorf("Receiver = %q", p.Receiver)
	}
}

func TestDecodeRejections(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		limit   int64
		wantErr string
		is      error
	}{
		{
			name: "empty alerts array",
			body: `{"version":"4","alerts":[]}`,
			is:   alertmanager.ErrNoAlerts,
		},
		{
			name: "no alerts key at all",
			body: `{"version":"4"}`,
			is:   alertmanager.ErrNoAlerts,
		},
		{
			name:    "not json",
			body:    `this is not json`,
			wantErr: "decode webhook payload",
		},
		{
			name:    "truncated json",
			body:    `{"version":"4","alerts":[{"status":"fir`,
			wantErr: "decode webhook payload",
		},
		{
			name:    "alerts is the wrong type",
			body:    `{"version":"4","alerts":"nope"}`,
			wantErr: "decode webhook payload",
		},
		{
			// The limit exists so a large batch bounds memory instead of OOMing
			// the pod at exactly the moment a node failure is alerting.
			name:    "body over the limit",
			body:    `{"version":"4","alerts":[{"status":"firing","labels":{"alertname":"A"}}]}`,
			limit:   16,
			wantErr: "exceeds 16 bytes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit := tc.limit
			if limit == 0 {
				limit = noLimit
			}
			_, err := alertmanager.Decode(strings.NewReader(tc.body), limit)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("error %v does not wrap %v", err, tc.is)
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// Alertmanager adds fields between versions. Rejecting a payload over one we do
// not read would drop real alerts the moment the upstream chart is upgraded.
func TestDecodeToleratesUnknownFields(t *testing.T) {
	body := `{"version":"5","somethingNew":{"a":1},"alerts":[
	  {"status":"firing","labels":{"alertname":"A"},"futureField":true}]}`
	p, err := alertmanager.Decode(strings.NewReader(body), noLimit)
	if err != nil {
		t.Fatalf("decode rejected a payload with unknown fields: %v", err)
	}
	if len(p.Alerts) != 1 {
		t.Fatalf("got %d alerts, want 1", len(p.Alerts))
	}
}

func TestToEventsProjection(t *testing.T) {
	p, err := alertmanager.Decode(strings.NewReader(string(testdata(t, "firing-batch.json"))), noLimit)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	received := time.Date(2026, 9, 13, 10, 1, 0, 0, time.UTC)
	events := p.ToEvents(received)
	if len(events) != 2 {
		t.Fatalf("got %d events from 2 alerts", len(events))
	}

	ev := events[0]
	if ev.AlertID != "a1b2c3d4e5f60718" {
		t.Errorf("AlertID = %q, want Alertmanager's own fingerprint", ev.AlertID)
	}
	// This service writes exactly one status, ever.
	if ev.Status != event.StatusWaiting {
		t.Errorf("Status = %q, want %q", ev.Status, event.StatusWaiting)
	}
	if ev.AlertState != event.AlertFiring {
		t.Errorf("AlertState = %q, want firing", ev.AlertState)
	}
	if ev.AlertName != "KubePodCrashLooping" {
		t.Errorf("AlertName = %q", ev.AlertName)
	}
	if ev.Severity != "warning" {
		t.Errorf("Severity = %q", ev.Severity)
	}
	if ev.Namespace != "ckh" {
		t.Errorf("Namespace = %q", ev.Namespace)
	}
	// Pod beats container and namespace: most specific wins, kind-qualified so
	// the value still means something outside the label set.
	if ev.Resource != "pod/worker-7d8f6c9b4d-abcde" {
		t.Errorf("Resource = %q, want pod/worker-7d8f6c9b4d-abcde", ev.Resource)
	}
	if ev.Summary != "Pod is crash looping." {
		t.Errorf("Summary = %q", ev.Summary)
	}
	if !strings.Contains(ev.Description, "CrashLoopBackOff") {
		t.Errorf("Description = %q", ev.Description)
	}
	// Hoisted from commonAnnotations: Alertmanager factors shared annotations
	// out of the group, and a summary lost to grouping is a summary lost.
	if !strings.Contains(ev.RunbookURL, "kubepodcrashlooping") {
		t.Errorf("RunbookURL = %q, want the value from commonAnnotations", ev.RunbookURL)
	}
	if !ev.StartsAt.Equal(time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("StartsAt = %v", ev.StartsAt)
	}
	// A firing alert has a zero endsAt, which would otherwise land in Postgres
	// as year 1 and pollute every range query.
	if ev.EndsAt != nil {
		t.Errorf("EndsAt = %v, want nil for a firing alert", *ev.EndsAt)
	}
	if !ev.ReceivedAt.Equal(received) {
		t.Errorf("ReceivedAt = %v, want the passed-in time", ev.ReceivedAt)
	}
	if ev.EventID == "" || ev.EventID == events[1].EventID {
		t.Errorf("each alert needs its own event_id, got %q and %q", ev.EventID, events[1].EventID)
	}
	if len(ev.RawAlert) == 0 || string(ev.RawAlert) == "{}" {
		t.Errorf("RawAlert is empty: %s", ev.RawAlert)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(ev.RawAlert, &roundTrip); err != nil {
		t.Errorf("RawAlert is not valid JSON: %v", err)
	}
}

func TestToEventResolved(t *testing.T) {
	p, err := alertmanager.Decode(strings.NewReader(string(testdata(t, "resolved-single.json"))), noLimit)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := p.ToEvents(time.Now())[0]

	if ev.AlertState != event.AlertResolved {
		t.Errorf("AlertState = %q, want resolved", ev.AlertState)
	}
	// A resolved event is new work for the harness, not a terminal state of the
	// row -- so its own status is still waiting.
	if ev.Status != event.StatusWaiting {
		t.Errorf("Status = %q; a resolution is still a waiting event", ev.Status)
	}
	if ev.EndsAt == nil {
		t.Fatal("EndsAt is nil on a resolved alert")
	}
	if !ev.EndsAt.Equal(time.Date(2026, 9, 13, 10, 42, 0, 0, time.UTC)) {
		t.Errorf("EndsAt = %v", *ev.EndsAt)
	}
	// Same alert, same start: only alert_status differs. That is exactly what
	// makes the resolution a distinct row under the deduplication index.
	if ev.AlertID != "a1b2c3d4e5f60718" {
		t.Errorf("AlertID = %q; a resolution must keep the firing alert's id", ev.AlertID)
	}
}

func TestToEventDerivesFingerprintWhenAbsent(t *testing.T) {
	p, err := alertmanager.Decode(strings.NewReader(string(testdata(t, "no-fingerprint.json"))), noLimit)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ev := p.ToEvents(time.Now())[0]

	if ev.AlertID == "" {
		t.Fatal("AlertID is empty; a sender without a fingerprint must still get one")
	}
	want := event.Fingerprint(map[string]string{
		"alertname": "HandRolledAlert",
		"severity":  "critical",
		"node":      "minikube",
	})
	if ev.AlertID != want {
		t.Errorf("AlertID = %q, want the derived fingerprint %q", ev.AlertID, want)
	}
	if ev.Resource != "node/minikube" {
		t.Errorf("Resource = %q, want node/minikube", ev.Resource)
	}
	// Summary came only from commonAnnotations here; the alert's own annotations
	// map is empty.
	if !strings.Contains(ev.Summary, "not Alertmanager") {
		t.Errorf("Summary = %q, want the value hoisted from commonAnnotations", ev.Summary)
	}
}

func TestToEventDefaults(t *testing.T) {
	received := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	t.Run("missing status is treated as firing", func(t *testing.T) {
		// Guessing "resolved" would file a live problem as already over.
		a := alertmanager.Alert{Labels: map[string]string{"alertname": "A"}}
		ev := a.ToEvent(nil, received)
		if ev.AlertState != event.AlertFiring {
			t.Errorf("AlertState = %q, want firing", ev.AlertState)
		}
	})

	t.Run("unknown status is treated as firing", func(t *testing.T) {
		a := alertmanager.Alert{Status: "pending", Labels: map[string]string{"alertname": "A"}}
		if ev := a.ToEvent(nil, received); ev.AlertState != event.AlertFiring {
			t.Errorf("AlertState = %q, want firing", ev.AlertState)
		}
	})

	t.Run("missing alertname is named", func(t *testing.T) {
		// alert_name is NOT NULL; an empty string is legal and ungreppable.
		a := alertmanager.Alert{Status: "firing", Labels: map[string]string{"severity": "warning"}}
		if ev := a.ToEvent(nil, received); ev.AlertName != "UnknownAlert" {
			t.Errorf("AlertName = %q, want UnknownAlert", ev.AlertName)
		}
	})

	t.Run("missing startsAt falls back to receipt time", func(t *testing.T) {
		// starts_at is NOT NULL and part of the deduplication key: a zero value
		// would make every delivery of the alert look distinct.
		a := alertmanager.Alert{Status: "firing", Labels: map[string]string{"alertname": "A"}}
		ev := a.ToEvent(nil, received)
		if !ev.StartsAt.Equal(received) {
			t.Errorf("StartsAt = %v, want the receipt time %v", ev.StartsAt, received)
		}
	})

	t.Run("nil label and annotation maps become empty maps", func(t *testing.T) {
		// The columns are NOT NULL JSONB. A nil map marshals to `null`, which is
		// not a valid JSON object for them.
		a := alertmanager.Alert{Status: "firing"}
		ev := a.ToEvent(nil, received)
		if ev.Labels == nil {
			t.Error("Labels is nil; would be written as JSON null into a NOT NULL column")
		}
		if ev.Annotations == nil {
			t.Error("Annotations is nil")
		}
	})
}

// The projection must not alias the decoded payload's maps: mutating an event's
// labels later would otherwise reach back into a sibling event in the batch.
func TestToEventCopiesLabelMaps(t *testing.T) {
	labels := map[string]string{"alertname": "A", "pod": "web-0"}
	a := alertmanager.Alert{Status: "firing", Labels: labels}
	ev := a.ToEvent(nil, time.Now())

	ev.Labels["pod"] = "mutated"
	if labels["pod"] != "web-0" {
		t.Error("ToEvent aliased the source label map instead of copying it")
	}
}

func TestResource(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{"pod beats namespace", map[string]string{"namespace": "ckh", "pod": "web-0"}, "pod/web-0"},
		{"pod beats node", map[string]string{"node": "minikube", "pod": "web-0"}, "pod/web-0"},
		{"pod beats deployment", map[string]string{"deployment": "web", "pod": "web-0"}, "pod/web-0"},
		{"deployment beats node", map[string]string{"node": "n1", "deployment": "web"}, "deployment/web"},
		{"pvc", map[string]string{"persistentvolumeclaim": "data-0"}, "persistentvolumeclaim/data-0"},
		{"node alone", map[string]string{"node": "minikube"}, "node/minikube"},
		{"instance is the last resort", map[string]string{"instance": "10.0.0.1:9100"}, "instance/10.0.0.1:9100"},
		{"nothing recognisable", map[string]string{"alertname": "A", "severity": "info"}, ""},
		{"empty value is skipped", map[string]string{"pod": "", "node": "minikube"}, "node/minikube"},
		{"nil map", nil, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := alertmanager.Resource(tc.labels); got != tc.want {
				t.Errorf("Resource() = %q, want %q", got, tc.want)
			}
		})
	}
}
