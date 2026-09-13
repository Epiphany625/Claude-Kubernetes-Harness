package event_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

func TestStatusValid(t *testing.T) {
	for _, s := range event.AllStatuses() {
		if !s.Valid() {
			t.Errorf("AllStatuses contains %q but Valid() rejects it", s)
		}
	}
	for _, s := range []event.Status{"", "WAITING", "waiting human approval", "done", "pending"} {
		if s.Valid() {
			t.Errorf("Valid() accepted %q, which the CHECK constraint would reject", s)
		}
	}
}

// The status vocabulary is a cross-service contract enforced in two places: the
// Go constants here and the CHECK constraint in the migration. They cannot be
// generated from one another, so this asserts they agree -- otherwise adding a
// status in Go produces a clean build and a SQLSTATE 23514 at runtime.
func TestStatusesMatchMigration(t *testing.T) {
	sql, err := store.MigrationSQL("0001_event.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}

	constraint := regexp.MustCompile(`(?s)CONSTRAINT event_status_valid CHECK \(status IN \((.*?)\)\)`)
	m := constraint.FindStringSubmatch(sql)
	if m == nil {
		t.Fatal("could not find the event_status_valid CHECK in 0001_event.sql; " +
			"if the constraint was renamed, update this test rather than deleting it")
	}

	inSQL := map[string]bool{}
	for _, raw := range strings.Split(m[1], ",") {
		v := strings.Trim(strings.TrimSpace(raw), "'")
		// Strip trailing SQL comments on the same line.
		if i := strings.Index(v, "--"); i >= 0 {
			v = strings.TrimSpace(v[:i])
		}
		if v != "" {
			inSQL[v] = true
		}
	}

	inGo := map[string]bool{}
	for _, s := range event.AllStatuses() {
		inGo[string(s)] = true
	}

	for s := range inGo {
		if !inSQL[s] {
			t.Errorf("status %q exists in Go but not in the CHECK constraint; "+
				"writing it would fail with SQLSTATE 23514", s)
		}
	}
	for s := range inSQL {
		if !inGo[s] {
			t.Errorf("status %q exists in the CHECK constraint but not in AllStatuses()", s)
		}
	}
}

func TestAlertStateValid(t *testing.T) {
	if !event.AlertFiring.Valid() || !event.AlertResolved.Valid() {
		t.Error("firing and resolved must both be valid alert states")
	}
	for _, s := range []event.AlertState{"", "pending", "FIRING"} {
		if s.Valid() {
			t.Errorf("Valid() accepted alert state %q", s)
		}
	}
}

// Stability is the whole point of Fingerprint: it is half the deduplication key,
// so a value that varies between calls would make every retry look like a new
// event and the unique index would never fire.
func TestFingerprintIsStable(t *testing.T) {
	labels := map[string]string{
		"alertname": "KubePodCrashLooping",
		"namespace": "ckh",
		"pod":       "worker-0",
		"severity":  "warning",
		"container": "worker",
		"job":       "kube-state-metrics",
	}

	first := event.Fingerprint(labels)
	// Go randomizes map iteration order per range. Enough iterations that an
	// order-dependent implementation cannot pass by luck.
	for i := 0; i < 200; i++ {
		if got := event.Fingerprint(labels); got != first {
			t.Fatalf("fingerprint changed between calls: %q then %q (iteration %d)", first, got, i)
		}
	}

	// A copy built in a different insertion order must agree.
	reordered := map[string]string{}
	for _, k := range []string{"severity", "pod", "job", "container", "namespace", "alertname"} {
		reordered[k] = labels[k]
	}
	if got := event.Fingerprint(reordered); got != first {
		t.Errorf("fingerprint depends on map insertion order: %q vs %q", got, first)
	}
}

func TestFingerprintDistinguishesLabelSets(t *testing.T) {
	base := map[string]string{"alertname": "A", "pod": "web-0"}
	cases := []struct {
		name   string
		labels map[string]string
	}{
		{"different value", map[string]string{"alertname": "A", "pod": "web-1"}},
		{"extra label", map[string]string{"alertname": "A", "pod": "web-0", "severity": "warning"}},
		{"missing label", map[string]string{"alertname": "A"}},
		// The separator bytes exist so these two cannot collide: without them
		// both would hash the concatenation "alertnameApodweb-0".
		{"shifted boundary", map[string]string{"alertnameA": "", "pod": "web-0"}},
	}

	want := event.Fingerprint(base)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := event.Fingerprint(tc.labels); got == want {
				t.Errorf("%s hashes the same as the base label set (%q)", tc.name, got)
			}
		})
	}
}

func TestFingerprintShape(t *testing.T) {
	// 16 lowercase hex characters, so a derived id is indistinguishable from one
	// Alertmanager generated and nothing downstream has to care which it got.
	got := event.Fingerprint(map[string]string{"alertname": "A"})
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(got) {
		t.Errorf("fingerprint %q is not 16 lowercase hex characters", got)
	}
	if empty := event.Fingerprint(nil); !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(empty) {
		t.Errorf("fingerprint of an empty label set is malformed: %q", empty)
	}
}

func TestRoutingKey(t *testing.T) {
	cases := []struct {
		name   string
		ev     event.Event
		prefix string
		want   string
	}{
		{
			name:   "firing critical",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "critical"},
			prefix: "alert",
			want:   "alert.firing.critical",
		},
		{
			name:   "resolved warning",
			ev:     event.Event{AlertState: event.AlertResolved, Severity: "warning"},
			prefix: "alert",
			want:   "alert.resolved.warning",
		},
		{
			// Three segments always, so `alert.firing.*` still matches. Collapsing
			// to two would drop this message out of that binding silently.
			name:   "missing severity becomes unknown",
			ev:     event.Event{AlertState: event.AlertFiring},
			prefix: "alert",
			want:   "alert.firing.unknown",
		},
		{
			name:   "empty prefix defaults",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "info"},
			prefix: "",
			want:   "alert.firing.info",
		},
		{
			name:   "uppercase severity is normalised",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "CRITICAL"},
			prefix: "alert",
			want:   "alert.firing.critical",
		},
		{
			// A severity label is arbitrary text from a PrometheusRule. A dot in
			// it would add a fourth segment and escape every three-segment
			// binding; '*' and '#' are AMQP wildcards.
			name:   "dots in severity cannot add a segment",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "very.bad"},
			prefix: "alert",
			want:   "alert.firing.very_bad",
		},
		{
			name:   "wildcards in severity are neutralised",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "#*"},
			prefix: "alert",
			want:   "alert.firing.__",
		},
		{
			name:   "whitespace in severity",
			ev:     event.Event{AlertState: event.AlertFiring, Severity: "page now"},
			prefix: "alert",
			want:   "alert.firing.page_now",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ev.RoutingKey(tc.prefix); got != tc.want {
				t.Errorf("RoutingKey(%q) = %q, want %q", tc.prefix, got, tc.want)
			}
		})
	}
}

func TestRoutingKeyAlwaysThreeSegments(t *testing.T) {
	for _, sev := range []string{"", "a.b.c.d", "###", "  ", "x"} {
		ev := event.Event{AlertState: event.AlertFiring, Severity: sev}
		key := ev.RoutingKey("alert")
		if n := strings.Count(key, "."); n != 2 {
			t.Errorf("severity %q produced %q with %d dots; the alert.*.* binding "+
				"pattern requires exactly 3 segments", sev, key, n+1)
		}
	}
}
