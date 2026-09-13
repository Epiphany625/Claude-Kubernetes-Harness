// Package obs holds the service's logging and metrics setup.
//
// The metric names here are a contract with ops/alerting/prometheusrule-ckh.yaml
// -- renaming one silently disarms the alert that watches it, because a
// PromQL expression over a metric that does not exist evaluates to no data
// rather than to an error.
package obs

import (
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every collector the service exports.
type Metrics struct {
	WebhookRequests *prometheus.CounterVec
	WebhookDuration *prometheus.HistogramVec
	AlertsReceived  prometheus.Counter
	EventsRecorded  *prometheus.CounterVec
	EventsPublished *prometheus.CounterVec
	EventFailures   *prometheus.CounterVec
	StageDuration   *prometheus.HistogramVec
	DependencyUp    *prometheus.GaugeVec
	registry        *prometheus.Registry
}

// NewMetrics builds and registers the collectors. Pass nil for reg to get an
// unregistered set -- what tests want, so that two tests in one process do not
// collide on the default registry.
func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		WebhookRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alertprocessor_webhook_requests_total",
			Help: "Webhook deliveries received, by HTTP status code.",
		}, []string{"code"}),

		WebhookDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "alertprocessor_webhook_duration_seconds",
			Help: "Time to handle one webhook delivery end to end.",
			// Bucketed around Alertmanager's 10s webhook timeout: the question
			// these buckets have to answer is "how close are we to the sender
			// giving up", so the resolution is at the top of the range.
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		}, []string{"code"}),

		AlertsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "alertprocessor_alerts_received_total",
			Help: "Individual alerts extracted from webhook payloads. One delivery carries many.",
		}),

		EventsRecorded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alertprocessor_events_recorded_total",
			Help: "Events written to Postgres, by result (inserted, duplicate).",
		}, []string{"result"}),

		EventsPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alertprocessor_events_published_total",
			Help: "Events handed to RabbitMQ, by result (published, skipped).",
		}, []string{"result"}),

		EventFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "alertprocessor_event_failures_total",
			Help: "Events that failed, by pipeline stage (record, publish, mark_published).",
		}, []string{"stage"}),

		StageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "alertprocessor_stage_duration_seconds",
			Help:    "Time spent in one pipeline stage for one event.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage"}),

		DependencyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "alertprocessor_dependency_up",
			Help: "1 when the dependency answered its last readiness check, 0 otherwise.",
		}, []string{"dependency"}),
	}

	if reg != nil {
		m.registry = reg
		reg.MustRegister(
			m.WebhookRequests, m.WebhookDuration, m.AlertsReceived,
			m.EventsRecorded, m.EventsPublished, m.EventFailures,
			m.StageDuration, m.DependencyUp,
		)
		reg.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)

		// Initialise the label sets that alerting rules divide by, so the rules
		// evaluate from the first scrape. A rate() over a counter that has never
		// been incremented is no data, and `no data` in the denominator makes
		// AlertProcessorWebhookErrors silently never fire.
		for _, code := range []string{"200", "400", "401", "500"} {
			m.WebhookRequests.WithLabelValues(code)
		}
		for _, stage := range []string{"record", "publish", "mark_published"} {
			m.EventFailures.WithLabelValues(stage)
		}
		m.EventsRecorded.WithLabelValues("inserted")
		m.EventsRecorded.WithLabelValues("duplicate")
		m.EventsPublished.WithLabelValues("published")
		m.EventsPublished.WithLabelValues("skipped")
	}

	return m
}

// Registry returns the registry these metrics are registered with, or nil.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) WebhookHandled(code string, d time.Duration) {
	m.WebhookRequests.WithLabelValues(code).Inc()
	m.WebhookDuration.WithLabelValues(code).Observe(d.Seconds())
}

func (m *Metrics) AlertsIngested(n int)         { m.AlertsReceived.Add(float64(n)) }
func (m *Metrics) EventRecorded(result string)  { m.EventsRecorded.WithLabelValues(result).Inc() }
func (m *Metrics) EventPublished(result string) { m.EventsPublished.WithLabelValues(result).Inc() }
func (m *Metrics) EventFailed(stage string)     { m.EventFailures.WithLabelValues(stage).Inc() }

func (m *Metrics) ObserveStage(stage string, d time.Duration) {
	m.StageDuration.WithLabelValues(stage).Observe(d.Seconds())
}

// SetDependencyUp records the outcome of a readiness check.
func (m *Metrics) SetDependencyUp(dependency string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	m.DependencyUp.WithLabelValues(dependency).Set(v)
}

// NewLogger builds the structured logger. JSON in the cluster so log
// aggregation can index the fields; text for a terminal.
func NewLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.ToLower(format) == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", "alertprocessor")
}
