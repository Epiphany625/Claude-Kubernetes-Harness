// Package event defines the unit of agent work: one Event is one alert
// notification the pipeline has accepted responsibility for.
//
// The distinction that shapes everything here is between an *alert* and an
// *event*. An alert is a condition Prometheus observes; it has one identity for
// as long as it keeps firing (Alertmanager's fingerprint). An event is one
// notification about that alert which the harness may act on. A single alert
// produces several events over its life: it fires, it resolves, it fires again.
// Hence AlertID and EventID are different columns, and neither is derivable from
// the other.
package event

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// Status is the lifecycle state of an event, shared across every service that
// touches the event table. The values are a cross-service contract: they are
// written here, constrained by the event_status_valid CHECK in
// internal/store/migrations/0001_event.sql, and read by harness/.
//
// Spellings are snake_case rather than the prose form ("waiting human approval")
// because they travel through SQL predicates, AMQP routing keys and JSON field
// values, all of which treat a space as a reason to quote.
type Status string

const (
	// StatusWaiting is the only status this service ever writes. Everything
	// downstream of "an alert arrived and was recorded" belongs to harness/.
	StatusWaiting Status = "waiting"

	// The remaining statuses are declared here, unused by this service, so that
	// the vocabulary has exactly one definition. harness/ transitions rows
	// through them; a consumer that invents a sixth value will fail the CHECK
	// constraint rather than quietly widen the contract.
	StatusSolving              Status = "solving"
	StatusWaitingHumanApproval Status = "waiting_human_approval"
	StatusResolved             Status = "resolved"
	StatusFailedToResolve      Status = "failed_to_resolve"
)

// AllStatuses lists every valid status, in lifecycle order. Kept in sync with
// the SQL CHECK constraint by TestStatusesMatchMigration.
func AllStatuses() []Status {
	return []Status{
		StatusWaiting,
		StatusSolving,
		StatusWaitingHumanApproval,
		StatusResolved,
		StatusFailedToResolve,
	}
}

// Valid reports whether s is one of the statuses the event table accepts.
func (s Status) Valid() bool {
	for _, known := range AllStatuses() {
		if s == known {
			return true
		}
	}
	return false
}

func (s Status) String() string { return string(s) }

// AlertState is the firing/resolved state Alertmanager reports for an alert. It
// is deliberately separate from Status: AlertState describes the world, Status
// describes what the harness has done about it. An alert can be resolved in
// Prometheus while its event is still `solving`.
type AlertState string

const (
	AlertFiring   AlertState = "firing"
	AlertResolved AlertState = "resolved"
)

// Valid reports whether a is a state Alertmanager actually sends.
func (a AlertState) Valid() bool {
	return a == AlertFiring || a == AlertResolved
}

func (a AlertState) String() string { return string(a) }

// Event is one row of the event table and, serialized, one RabbitMQ message.
//
// The JSON tags are the queue contract. Renaming a field here renames it for
// every consumer; see .claude/skills/add-event-field.
type Event struct {
	EventID string `json:"event_id"`
	AlertID string `json:"alert_id"`
	Status  Status `json:"status"`

	// AlertState is what Alertmanager said, unmodified.
	AlertState AlertState `json:"alert_status"`

	AlertName string `json:"alert_name"`
	Severity  string `json:"severity,omitempty"`
	Namespace string `json:"namespace,omitempty"`

	// Resource is the most specific Kubernetes object the alert's labels name --
	// the pod, else the deployment, else the node, and so on. It is a convenience
	// for the harness, which would otherwise re-derive it from Labels on every
	// message; Labels remains the authority.
	Resource string `json:"resource,omitempty"`

	Summary      string `json:"summary,omitempty"`
	Description  string `json:"description,omitempty"`
	RunbookURL   string `json:"runbook_url,omitempty"`
	GeneratorURL string `json:"generator_url,omitempty"`

	StartsAt time.Time  `json:"starts_at"`
	EndsAt   *time.Time `json:"ends_at,omitempty"`

	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`

	// RawAlert is the alert object exactly as received, so an event can be
	// replayed or re-interpreted after the projection above changes. Stored as
	// JSONB; carried in the message body.
	RawAlert json.RawMessage `json:"raw_alert,omitempty"`

	ReceivedAt time.Time `json:"received_at"`
}

// RoutingKey builds the AMQP topic key for this event:
//
//	alert.firing.critical
//	alert.resolved.warning
//	alert.firing.unknown     (severity label absent)
//
// Three segments, always, so a consumer binding pattern like `alert.firing.*`
// means what it looks like. A missing severity becomes "unknown" rather than
// collapsing the key to two segments, which would silently fall out of such a
// binding.
func (e Event) RoutingKey(prefix string) string {
	if prefix == "" {
		prefix = "alert"
	}
	severity := sanitizeRoutingSegment(e.Severity)
	if severity == "" {
		severity = "unknown"
	}
	state := sanitizeRoutingSegment(string(e.AlertState))
	if state == "" {
		state = "unknown"
	}
	return prefix + "." + state + "." + severity
}

// sanitizeRoutingSegment strips the characters AMQP treats as structure. A label
// value is arbitrary user input from a PrometheusRule; a severity of "very.bad"
// would otherwise add a fourth segment and escape every three-segment binding.
func sanitizeRoutingSegment(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			// '.', '*', '#', whitespace and anything else become '_'.
			b.WriteRune('_')
		}
	}
	return b.String()
}

// Fingerprint derives a stable identifier from a label set, for senders that
// omit Alertmanager's own `fingerprint` field -- hand-written test payloads, and
// Alertmanager before 0.18.
//
// Stability is the whole point: it must not depend on Go's randomized map
// iteration order, or the same alert would produce a different AlertID on every
// delivery and the deduplication index would never match. Hence the sort.
func Fingerprint(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		// Length-prefixing would be more rigorous, but Prometheus label names
		// cannot contain the separators, so `k\x00v\x01` cannot be ambiguous.
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(labels[k]))
		h.Write([]byte{1})
	}
	// 16 hex chars, matching the width Alertmanager's own fingerprints use, so
	// the two are indistinguishable to anything reading the column.
	return hex.EncodeToString(h.Sum(nil))[:16]
}
