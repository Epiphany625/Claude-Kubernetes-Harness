// Package alertmanager decodes Alertmanager's webhook payload and projects each
// alert in it into an event.Event.
//
// The payload format is documented at
// https://prometheus.io/docs/alerting/latest/configuration/#webhook_config and is
// versioned; version "4" has been current since Alertmanager 0.16. This package
// accepts other versions with a warning rather than rejecting them, because
// refusing a payload we could have understood loses an alert permanently -- the
// sender is Alertmanager, and it will retry a few times and then give up.
package alertmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

// PayloadVersion is the webhook schema this package is written against.
const PayloadVersion = "4"

// Payload is one webhook POST: a group of alerts that Alertmanager has decided
// belong together, per the groupBy in the AlertmanagerConfig.
//
// One POST carries many alerts. Treating it as one alert is the mistake this
// type exists to make impossible -- a group of twelve crash-looping pods is one
// request and must become twelve events.
type Payload struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	TruncatedAlerts   int               `json:"truncatedAlerts"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []Alert           `json:"alerts"`
}

// Alert is a single alert inside a Payload.
type Alert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	// Fingerprint is Alertmanager's own stable identity for the alert. Present
	// since 0.18; event.Fingerprint covers the gap when it is not.
	Fingerprint string `json:"fingerprint"`
}

// ErrNoAlerts means the payload decoded cleanly but carried nothing to record.
// It is not a malformed request -- Alertmanager does not send empty batches, so
// this generally means something else is POSTing to the webhook.
var ErrNoAlerts = errors.New("payload contains no alerts")

// Decode reads a webhook payload from r, refusing anything larger than limit.
//
// The size limit matters: a payload is attacker-controllable in size if the
// webhook is reachable, and Alertmanager itself can send a genuinely large batch
// when a node fails and three hundred pods alert at once. Reading it into memory
// unbounded is how this service OOMs at exactly the moment it is most needed.
func Decode(r io.Reader, limit int64) (*Payload, error) {
	if limit > 0 {
		r = io.LimitReader(r, limit+1)
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read webhook body: %w", err)
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, fmt.Errorf("webhook body exceeds %d bytes", limit)
	}

	var p Payload
	// Unknown fields are allowed (no DisallowUnknownFields). Alertmanager adds
	// them between versions, and rejecting a payload over a field we do not read
	// would drop real alerts on an upstream upgrade.
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("decode webhook payload: %w", err)
	}
	if len(p.Alerts) == 0 {
		return nil, ErrNoAlerts
	}
	return &p, nil
}

// ToEvents projects every alert in the payload into an Event ready to be stored
// and published. receivedAt is passed in rather than read from the clock so that
// every event in one batch shares a timestamp and tests are deterministic.
func (p *Payload) ToEvents(receivedAt time.Time) []event.Event {
	events := make([]event.Event, 0, len(p.Alerts))
	for i := range p.Alerts {
		events = append(events, p.Alerts[i].ToEvent(p, receivedAt))
	}
	return events
}

// ToEvent projects a single alert. The payload is passed for its common
// annotations, which Alertmanager factors out of the individual alerts.
func (a Alert) ToEvent(p *Payload, receivedAt time.Time) event.Event {
	labels := copyMap(a.Labels)
	annotations := copyMap(a.Annotations)

	// Alertmanager hoists annotations shared by every alert in the group into
	// commonAnnotations and leaves them on the individual alerts too -- but only
	// for alerts that have them. Filling gaps from the common set means a
	// summary is never lost to grouping.
	if p != nil {
		for k, v := range p.CommonAnnotations {
			if _, ok := annotations[k]; !ok {
				annotations[k] = v
			}
		}
	}

	alertID := a.Fingerprint
	if alertID == "" {
		alertID = event.Fingerprint(labels)
	}

	state := event.AlertState(a.Status)
	if !state.Valid() {
		// An alert with no status is firing; that is what it means for
		// Alertmanager to be telling us about it at all. Guessing "resolved"
		// here would file it as already over.
		state = event.AlertFiring
	}

	ev := event.Event{
		EventID:      uuid.NewString(),
		AlertID:      alertID,
		Status:       event.StatusWaiting, // the only status this service writes
		AlertState:   state,
		AlertName:    labels["alertname"],
		Severity:     labels["severity"],
		Namespace:    labels["namespace"],
		Resource:     Resource(labels),
		Summary:      firstNonEmpty(annotations, "summary", "message", "description"),
		Description:  firstNonEmpty(annotations, "description", "message"),
		RunbookURL:   firstNonEmpty(annotations, "runbook_url", "runbookURL", "runbook"),
		GeneratorURL: a.GeneratorURL,
		StartsAt:     a.StartsAt.UTC(),
		Labels:       labels,
		Annotations:  annotations,
		RawAlert:     a.raw(),
		ReceivedAt:   receivedAt.UTC(),
	}

	// Alertmanager sends a zero EndsAt for a firing alert, which marshals as
	// "0001-01-01T00:00:00Z" and would land in Postgres as a year-1 timestamp
	// that every range query then has to exclude by hand.
	if !a.EndsAt.IsZero() {
		end := a.EndsAt.UTC()
		ev.EndsAt = &end
	}

	// An alert with no alertname is not something Prometheus produces, but a
	// hand-rolled sender can manage it. An empty string in a NOT NULL column is
	// legal and useless; name it so the row is greppable.
	if ev.AlertName == "" {
		ev.AlertName = "UnknownAlert"
	}

	// StartsAt is part of the deduplication key and is NOT NULL. A sender that
	// omits it would otherwise make every delivery of the alert look distinct.
	if ev.StartsAt.IsZero() {
		ev.StartsAt = receivedAt.UTC()
	}

	return ev
}

// resourceLabels is the search order for the most specific Kubernetes object an
// alert names, narrowest first. A pod-level alert usually also carries the
// namespace and node; reporting the node would be true and useless.
var resourceLabels = []string{
	"pod",
	"persistentvolumeclaim",
	"container",
	"deployment",
	"statefulset",
	"daemonset",
	"job_name",
	"job",
	"service",
	"endpoint",
	"node",
	"instance",
}

// Resource picks the most specific resource label present, qualified by kind so
// the value is unambiguous once it is out of the label set: "pod/web-0", not
// "web-0".
func Resource(labels map[string]string) string {
	for _, key := range resourceLabels {
		if v, ok := labels[key]; ok && v != "" {
			return key + "/" + v
		}
	}
	return ""
}

// firstNonEmpty returns the first of keys present and non-empty in m. Rule
// authors are inconsistent about where they put prose -- `summary` and `message`
// are both common, and kube-prometheus-stack's own rules use both.
func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func copyMap(m map[string]string) map[string]string {
	// Never nil: these are NOT NULL JSONB columns, and a nil map marshals to
	// `null`, which is not a valid JSON object for them.
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// raw re-marshals the alert for the raw_alert column. Re-marshalling rather than
// slicing the original body keeps the stored form canonical (sorted keys, one
// timestamp format) at the cost of dropping fields this struct does not know
// about -- an acceptable trade, since those fields are by definition ones no
// consumer reads yet.
func (a Alert) raw() json.RawMessage {
	b, err := json.Marshal(struct {
		Status       string            `json:"status"`
		Labels       map[string]string `json:"labels"`
		Annotations  map[string]string `json:"annotations"`
		StartsAt     time.Time         `json:"startsAt"`
		EndsAt       time.Time         `json:"endsAt"`
		GeneratorURL string            `json:"generatorURL"`
		Fingerprint  string            `json:"fingerprint"`
	}{
		Status:       a.Status,
		Labels:       copyMap(a.Labels),
		Annotations:  copyMap(a.Annotations),
		StartsAt:     a.StartsAt,
		EndsAt:       a.EndsAt,
		GeneratorURL: a.GeneratorURL,
		Fingerprint:  a.Fingerprint,
	})
	if err != nil {
		// Unreachable: every field is a plain type. Store a valid JSON object
		// rather than propagating an error, because the alert itself is fine and
		// losing it over its archival copy would be the wrong trade.
		return json.RawMessage(`{}`)
	}
	return b
}

// SortedLabelKeys is a small helper for logging and tests, where stable output
// matters more than allocation.
func SortedLabelKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
