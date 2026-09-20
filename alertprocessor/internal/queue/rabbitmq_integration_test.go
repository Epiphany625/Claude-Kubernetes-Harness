//go:build integration

// Integration tests for the RabbitMQ publisher, against a real broker in a
// throwaway container.
//
//	go test -tags=integration ./internal/queue/...
//
// What these cover that the unit tests cannot: publisher confirms, the
// mandatory/return path that turns a silently-discarded message into an error,
// topology declaration, and reconnecting after the broker drops the connection.
package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/queue"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

// startBroker runs one RabbitMQ container for the test and returns its AMQP URL.
func startBroker(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := tcrabbit.Run(ctx, "rabbitmq:4-management-alpine")
	if err != nil {
		t.Fatalf("start rabbitmq container (is Docker running?): %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate rabbitmq container: %v", err)
		}
	})

	url, err := container.AmqpURL(ctx)
	if err != nil {
		t.Fatalf("amqp url: %v", err)
	}
	return url
}

func baseConfig(url string) config.AMQPConfig {
	return config.AMQPConfig{
		URL:             url,
		Exchange:        "alerts",
		Queue:           "agent.events",
		RoutingPrefix:  "alert",
		ConfirmTimeout: 10 * time.Second,
		ConnectTimeout:  15 * time.Second,
		PublishTimeout:  20 * time.Second,
	}
}

func sampleEvent() event.Event {
	return event.Event{
		EventID:    "33333333-3333-4333-8333-333333333333",
		AlertID:    "a1b2c3d4e5f60718",
		Status:     event.StatusWaiting,
		AlertState: event.AlertFiring,
		AlertName:  "KubePodCrashLooping",
		Severity:   "critical",
		Namespace:  "ckh",
		Resource:   "pod/worker-0",
		Summary:    "Pod is crash looping.",
		StartsAt:   time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 9, 13, 10, 1, 0, 0, time.UTC).UTC(),
		Labels: map[string]string{
			"alertname": "KubePodCrashLooping",
			"namespace": "ckh",
			"pod":       "worker-0",
		},
		Annotations: map[string]string{"summary": "Pod is crash looping."},
		RawAlert:    json.RawMessage(`{"status":"firing"}`),
	}
}

// consume pulls exactly one message off the queue and leaves nothing behind.
//
// basic.get rather than basic.consume, deliberately. A registered consumer keeps
// receiving after the first delivery, so two calls in one test would see the
// first consumer auto-ack every message and the second time out on an empty
// queue -- a test failure that looks exactly like a publish that never happened.
func consume(t *testing.T, url, queueName string, timeout time.Duration) amqp.Delivery {
	t.Helper()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	defer ch.Close()

	deadline := time.Now().Add(timeout)
	for {
		d, ok, err := ch.Get(queueName, true /*autoAck*/)
		if err != nil {
			t.Fatalf("get from %s: %v", queueName, err)
		}
		if ok {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatalf("no message on %s within %s", queueName, timeout)
		}
		// The queue is empty right now; a publish confirm and the message
		// becoming visible to a consumer are not the same instant.
		time.Sleep(100 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------

func TestPublishAndConsume(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	ev := sampleEvent()
	if err := pub.Publish(ctx, ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	d := consume(t, url, "agent.events", 10*time.Second)

	// The properties the consumer contract rests on.
	if d.MessageId != ev.EventID {
		t.Errorf("message_id = %q, want the event_id %q -- consumers deduplicate on this",
			d.MessageId, ev.EventID)
	}
	if d.CorrelationId != ev.AlertID {
		t.Errorf("correlation_id = %q, want the alert_id %q", d.CorrelationId, ev.AlertID)
	}
	if d.ContentType != "application/json" {
		t.Errorf("content_type = %q", d.ContentType)
	}
	// A transient message vanishes on a broker restart, and the row would say
	// published_at -- so nothing would ever retry it.
	if d.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery_mode = %d, want %d (persistent)", d.DeliveryMode, amqp.Persistent)
	}
	if d.RoutingKey != "alert.firing.critical" {
		t.Errorf("routing key = %q, want alert.firing.critical", d.RoutingKey)
	}
	// Headers let a consumer route without parsing the body.
	if got, _ := d.Headers["severity"].(string); got != "critical" {
		t.Errorf("severity header = %v", d.Headers["severity"])
	}
	if got, _ := d.Headers["alert_status"].(string); got != "firing" {
		t.Errorf("alert_status header = %v", d.Headers["alert_status"])
	}

	var decoded event.Event
	if err := json.Unmarshal(d.Body, &decoded); err != nil {
		t.Fatalf("body is not a valid Event: %v (%s)", err, d.Body)
	}
	if decoded.EventID != ev.EventID || decoded.AlertID != ev.AlertID {
		t.Errorf("body ids = %q/%q", decoded.EventID, decoded.AlertID)
	}
	if decoded.Status != event.StatusWaiting {
		t.Errorf("body status = %q, want waiting", decoded.Status)
	}
	if decoded.Labels["pod"] != "worker-0" {
		t.Errorf("labels did not survive the round trip: %v", decoded.Labels)
	}
	if !decoded.StartsAt.Equal(ev.StartsAt) {
		t.Errorf("starts_at = %v, want %v", decoded.StartsAt, ev.StartsAt)
	}
}

func TestRoutingKeyReachesTheQueueForEverySeverity(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// The binding is `alert.#`, so every key this service can produce must land
	// -- including the ones with no severity label at all.
	for _, tc := range []struct {
		severity string
		state    event.AlertState
	}{
		{"critical", event.AlertFiring},
		{"warning", event.AlertFiring},
		{"info", event.AlertResolved},
		{"", event.AlertFiring},
		{"weird.severity", event.AlertFiring},
	} {
		ev := sampleEvent()
		ev.Severity = tc.severity
		ev.AlertState = tc.state
		ev.EventID = "33333333-3333-4333-8333-" + time.Now().Format("150405.000000")[:12]

		if err := pub.Publish(ctx, ev); err != nil {
			t.Errorf("severity %q: %v", tc.severity, err)
		}
	}
}

// Without `mandatory`, the broker silently discards a message no queue is bound
// to, the publish confirms, and the service reports success for an event nobody
// will ever receive.
func TestUnroutablePublishIsAnError(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	cfg := baseConfig(url)
	// Declare the exchange but no queue, reproducing the state before harness/
	// exists to bind one.
	cfg.Queue = ""
	cfg.Exchange = "alerts-no-consumer"

	pub, err := queue.Open(ctx, cfg, quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	err = pub.Publish(ctx, sampleEvent())
	if err == nil {
		t.Fatal("publishing to an exchange with no bound queue reported success")
	}
	if !errors.Is(err, queue.ErrUnroutable) {
		t.Errorf("error %v does not wrap ErrUnroutable; "+
			"the processor distinguishes this from a transient failure", err)
	}
}

// A stale return must not be attributed to a later, successful publish.
func TestPublishSucceedsAfterAnUnroutableOne(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	cfg := baseConfig(url)
	pub, err := queue.Open(ctx, cfg, quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// Publish something the `alert.#` binding cannot match, by using a routing
	// prefix outside it.
	stray := sampleEvent()
	strayPub, err := queue.Open(ctx, func() config.AMQPConfig {
		c := baseConfig(url)
		c.RoutingPrefix = "unbound"
		c.Queue = "" // skip queue binding so "unbound.#" stays unroutable
		return c
	}(), quietLogger())
	if err != nil {
		t.Fatalf("open stray publisher: %v", err)
	}
	t.Cleanup(func() { _ = strayPub.Close() })

	if err := strayPub.Publish(ctx, stray); !errors.Is(err, queue.ErrUnroutable) {
		t.Fatalf("expected the stray publish to be unroutable, got %v", err)
	}

	// The routable publisher must be unaffected.
	if err := pub.Publish(ctx, sampleEvent()); err != nil {
		t.Errorf("a routable publish failed after an unroutable one: %v", err)
	}
}

// Declaring is the startup path and the reconnect path, so it runs repeatedly
// against a broker that already has the objects.
func TestDeclareTopologyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	for i := 0; i < 3; i++ {
		pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
		if err != nil {
			t.Fatalf("open attempt %d: %v", i+1, err)
		}
		if err := pub.Publish(ctx, sampleEvent()); err != nil {
			t.Errorf("publish on attempt %d: %v", i+1, err)
		}
		if err := pub.Close(); err != nil {
			t.Errorf("close attempt %d: %v", i+1, err)
		}
	}
}

// The first publish after a broker restart or a rolling update always fails.
// The one retry on a fresh connection is what makes the second one succeed.
func TestPublishRecoversAfterConnectionLoss(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.Publish(ctx, sampleEvent()); err != nil {
		t.Fatalf("initial publish: %v", err)
	}

	// Drop the connection out from under the publisher, which is what a broker
	// restart or a rolling update looks like from in here. More deterministic
	// than asking the management API to kill the connection and waiting.
	if err := queue.CloseUnderlyingConnection(pub); err != nil {
		t.Fatalf("close underlying connection: %v", err)
	}

	ev := sampleEvent()
	ev.EventID = "44444444-4444-4444-8444-444444444444"
	if err := pub.Publish(ctx, ev); err != nil {
		t.Fatalf("publish after connection loss did not recover: %v", err)
	}

	// Both messages must be on the queue: the one from before the outage and the
	// one the reconnect delivered. Counting alone would pass if the retry had
	// somehow republished the first message instead.
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		got[consume(t, url, "agent.events", 10*time.Second).MessageId] = true
	}
	for _, want := range []string{sampleEvent().EventID, ev.EventID} {
		if !got[want] {
			t.Errorf("message %s is missing from the queue; got %v", want, got)
		}
	}
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.Ping(ctx); err != nil {
		t.Errorf("Ping on a healthy broker: %v", err)
	}

	// Readiness is polled every 10s, which makes it the natural place to heal a
	// dropped connection before a webhook arrives and pays for the reconnect.
	if err := queue.CloseUnderlyingConnection(pub); err != nil {
		t.Fatalf("close underlying connection: %v", err)
	}
	if err := pub.Ping(ctx); err != nil {
		t.Errorf("Ping did not reconnect after the connection was lost: %v", err)
	}
}

func TestPublishAfterClose(t *testing.T) {
	ctx := context.Background()
	url := startBroker(t)

	pub, err := queue.Open(ctx, baseConfig(url), quietLogger())
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	if err := pub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close must be final, not a prelude to a silent reconnect during shutdown.
	if err := pub.Publish(ctx, sampleEvent()); err == nil {
		t.Error("publishing after Close succeeded")
	}
	// Idempotent.
	if err := pub.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
