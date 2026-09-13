// Package processor orchestrates what happens to one alert: record it, hand it
// off, mark it handed off.
//
// # Why this order
//
// Record before publish, always. The alternative -- publish first -- puts a
// message on the queue carrying an event_id that has no row behind it, and the
// harness picks it up, looks it up, and finds nothing. A row that was recorded
// but never published is the strictly better failure: it is visible
// (published_at IS NULL), it is queryable, and it can be replayed. One direction
// loses work silently, the other leaves evidence.
//
// # Why duplicates are not errors
//
// Alertmanager retries a webhook it could not deliver and re-notifies every
// repeat_interval. The same notification therefore arrives many times, and the
// pipeline has to converge rather than accumulate. Convergence lives in two
// places: the unique index in the schema, which makes a duplicate INSERT return
// the existing row, and AlreadyPublished here, which stops a second message for
// a row the broker already confirmed.
package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/obs"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/queue"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/store"
)

// Stage names the step an event failed at. It is carried on the failure metric
// so "the pipeline is broken" can be narrowed to which half is broken without
// reading logs.
type Stage string

const (
	StageRecord        Stage = "record"
	StagePublish       Stage = "publish"
	StageMarkPublished Stage = "mark_published"
)

// Outcome is what happened to one alert.
type Outcome struct {
	EventID string
	AlertID string

	// Recorded is true when this call created the row; false when the
	// notification had already been recorded.
	Recorded bool
	// Published is true when this call put a message on the queue; false when a
	// previous attempt had already done so.
	Published bool

	// Stage and Err are set only on failure.
	Stage Stage
	Err   error
}

// Failed reports whether this alert needs the sender to try again.
func (o Outcome) Failed() bool { return o.Err != nil }

// Processor handles alerts. Safe for concurrent use.
type Processor struct {
	store     store.Store
	publisher queue.Publisher
	metrics   *obs.Metrics
	log       *slog.Logger
}

// New builds a Processor. metrics may be nil in tests.
func New(s store.Store, p queue.Publisher, m *obs.Metrics, log *slog.Logger) *Processor {
	if m == nil {
		m = obs.NewMetrics(nil)
	}
	return &Processor{store: s, publisher: p, metrics: m, log: log}
}

// ProcessBatch handles every alert in one webhook delivery and returns an
// outcome per alert, in the order given.
//
// It does not stop at the first failure. One malformed or unlucky alert in a
// group of twenty should not cost the other nineteen their processing -- and
// since the sender retries the whole batch, the nineteen that succeeded will
// short-circuit on the retry rather than being done twice.
func (p *Processor) ProcessBatch(ctx context.Context, events []event.Event) []Outcome {
	outcomes := make([]Outcome, 0, len(events))
	for i := range events {
		outcomes = append(outcomes, p.Process(ctx, events[i]))
	}
	return outcomes
}

// Process handles a single alert.
func (p *Processor) Process(ctx context.Context, ev event.Event) Outcome {
	out := Outcome{EventID: ev.EventID, AlertID: ev.AlertID}

	log := p.log.With(
		"alert_id", ev.AlertID,
		"alert_name", ev.AlertName,
		"alert_status", string(ev.AlertState),
		"severity", ev.Severity,
		"namespace", ev.Namespace,
	)

	// 1. Record. This is what makes the event exist as far as the rest of the
	//    system is concerned.
	start := time.Now()
	res, err := p.store.RecordEvent(ctx, ev)
	p.metrics.ObserveStage(string(StageRecord), time.Since(start))
	if err != nil {
		p.metrics.EventFailed(string(StageRecord))
		out.Stage, out.Err = StageRecord, err
		log.Error("failed to record event", "error", err)
		return out
	}

	// The row's id wins over the one generated during projection. On a duplicate
	// they differ, and using the generated one would publish a message_id that
	// matches no row -- reintroducing exactly the dangling reference this
	// ordering exists to prevent.
	out.EventID = res.EventID
	out.Recorded = res.Inserted
	log = log.With("event_id", res.EventID)

	if res.Inserted {
		p.metrics.EventRecorded("inserted")
		log.Info("event recorded", "status", string(ev.Status))
	} else {
		p.metrics.EventRecorded("duplicate")
		log.Debug("notification already recorded", "already_published", res.AlreadyPublished)
	}

	// 2. Skip the handoff if a previous attempt already completed it. This is
	//    the check that keeps Alertmanager's retries from putting the same event
	//    on the queue repeatedly.
	if res.AlreadyPublished {
		p.metrics.EventPublished("skipped")
		log.Debug("event already published; nothing to do")
		return out
	}

	// 3. Publish, and wait for the broker to confirm.
	ev.EventID = res.EventID
	start = time.Now()
	err = p.publisher.Publish(ctx, ev)
	p.metrics.ObserveStage(string(StagePublish), time.Since(start))
	if err != nil {
		p.metrics.EventFailed(string(StagePublish))
		out.Stage, out.Err = StagePublish, err
		if errors.Is(err, queue.ErrUnroutable) {
			// Worth its own line: the row exists, the message went nowhere, and
			// the cause is a missing binding rather than anything transient.
			// Retrying will fail identically until the topology is fixed.
			log.Error("event recorded but message was unroutable; "+
				"check the exchange has a queue bound to it", "error", err)
		} else {
			log.Error("failed to publish event; row remains with published_at NULL", "error", err)
		}
		return out
	}
	out.Published = true
	p.metrics.EventPublished("published")

	// 4. Mark the handoff complete.
	start = time.Now()
	if err := p.store.MarkPublished(ctx, res.EventID); err != nil {
		p.metrics.EventFailed(string(StageMarkPublished))
		out.Stage, out.Err = StageMarkPublished, err
		// The message IS on the queue; only the bookkeeping failed. Reporting
		// this as a failure makes Alertmanager retry, and the retry will publish
		// a second copy of an event the consumer already has -- which is why the
		// message carries message_id and consumers are required to deduplicate
		// on it. Silently succeeding instead would leave the row looking
		// unpublished forever, and a replay job would eventually send a third.
		log.Error("published but failed to record published_at; "+
			"a retry will republish this event", "error", err)
		return out
	}

	log.Info("event published", "routing_key", ev.RoutingKey(""))
	return out
}

// Summary condenses a batch's outcomes for the HTTP response and the access log.
type Summary struct {
	Total     int `json:"total"`
	Recorded  int `json:"recorded"`
	Duplicate int `json:"duplicate"`
	Published int `json:"published"`
	Skipped   int `json:"skipped"`
	Failed    int `json:"failed"`

	// Errors is capped: a batch of three hundred failing alerts should not
	// return a three-hundred-line body to a sender that is going to retry it
	// anyway. The full detail is in the logs.
	Errors []string `json:"errors,omitempty"`
}

const maxReportedErrors = 10

// Summarize folds outcomes into a Summary.
func Summarize(outcomes []Outcome) Summary {
	s := Summary{Total: len(outcomes)}
	for _, o := range outcomes {
		switch {
		case o.Failed():
			s.Failed++
			if len(s.Errors) < maxReportedErrors {
				s.Errors = append(s.Errors, fmt.Sprintf("alert %s: %s: %v", o.AlertID, o.Stage, o.Err))
			}
		case o.Published:
			s.Published++
		default:
			s.Skipped++
		}
		if o.Recorded {
			s.Recorded++
		} else if !o.Failed() {
			s.Duplicate++
		}
	}
	if s.Failed > maxReportedErrors {
		s.Errors = append(s.Errors,
			fmt.Sprintf("... and %d more; see the alertprocessor logs", s.Failed-maxReportedErrors))
	}
	return s
}
