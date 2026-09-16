package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

// ErrUnroutable means the broker accepted the message and then handed it back
// because no queue was bound to match its routing key.
var ErrUnroutable = errors.New("message was returned by the broker as unroutable")

// RabbitPublisher publishes events to a topic exchange, waiting for a broker
// confirmation on every message.
type RabbitPublisher struct {
	cfg config.AMQPConfig
	log *slog.Logger

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	returns chan amqp.Return
	closed  bool
}

var _ Publisher = (*RabbitPublisher)(nil)

// Open dials the broker and, when configured to, declares the topology.
func Open(ctx context.Context, cfg config.AMQPConfig, log *slog.Logger) (*RabbitPublisher, error) {
	p := &RabbitPublisher{cfg: cfg, log: log}
	if err := p.reconnect(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// Close tears down the channel and connection. Safe to call more than once.
func (p *RabbitPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return p.teardownLocked()
}

func (p *RabbitPublisher) teardownLocked() error {
	var errs []error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, err)
		}
		p.channel = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	p.returns = nil
	return errors.Join(errs...)
}

// reconnect establishes a fresh connection, channel and topology.
//
// Called on startup and whenever a publish finds the connection dead. There is
// no background reconnect loop on purpose: reconnecting lazily means the service
// does not hold a connection open through an outage it is not being asked to
// publish through, and a failed publish is retried by Alertmanager anyway.
func (p *RabbitPublisher) reconnect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reconnectLocked(ctx)
}

func (p *RabbitPublisher) reconnectLocked(ctx context.Context) error {
	if p.closed {
		return errors.New("publisher is closed")
	}
	_ = p.teardownLocked()

	dialCfg := amqp.Config{
		Dial:      amqp.DefaultDial(p.cfg.ConnectTimeout),
		Heartbeat: 10 * time.Second,
		// Named so the connection is identifiable in the RabbitMQ management UI
		// without guessing from the source IP.
		Properties: amqp.Table{"connection_name": "alertprocessor"},
	}

	conn, err := amqp.DialConfig(p.cfg.URL, dialCfg)
	if err != nil {
		return fmt.Errorf("dial rabbitmq at %s: %w", config.Redacted(p.cfg.URL), err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open rabbitmq channel: %w", err)
	}

	// Confirm mode. Without it Publish returns as soon as the bytes are written
	// to the socket, which says nothing about whether the broker stored them --
	// and this service writes published_at on the strength of that return value.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("put channel in confirm mode: %w", err)
	}

	// Buffered so the library never blocks delivering a return while we are
	// waiting for the corresponding confirm.
	returns := ch.NotifyReturn(make(chan amqp.Return, 8))

	p.conn = conn
	p.channel = ch
	p.returns = returns

	if p.cfg.DeclareTopology {
		if err := p.declareLocked(); err != nil {
			_ = p.teardownLocked()
			return err
		}
	}

	p.log.Info("connected to rabbitmq",
		"url", config.Redacted(p.cfg.URL),
		"exchange", p.cfg.Exchange,
		"declared_topology", p.cfg.DeclareTopology)
	return nil
}

// declareLocked creates the exchange, queue and binding.
//
// Idempotent as long as nothing else declares the same objects with different
// arguments; if something does, RabbitMQ answers PRECONDITION_FAILED and closes
// the channel. Other services must use the same topology arguments.
func (p *RabbitPublisher) declareLocked() error {
	ch := p.channel

	// Topic rather than direct, so a consumer can bind to `alert.firing.*` or
	// `alert.#` instead of enumerating every severity.
	if err := ch.ExchangeDeclare(p.cfg.Exchange, amqp.ExchangeTopic,
		true /*durable*/, false /*autoDelete*/, false /*internal*/, false /*noWait*/, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", p.cfg.Exchange, err)
	}

	if p.cfg.Queue == "" {
		return nil
	}

	// Quorum queues are the durable default in RabbitMQ 4 and survive a broker
	// restart with the data intact. They work at a replication factor of one,
	// which is what a single-node cluster gives.
	queueArgs := amqp.Table{
		"x-queue-type": "quorum",
	}
	if _, err := ch.QueueDeclare(p.cfg.Queue,
		true /*durable*/, false /*autoDelete*/, false /*exclusive*/, false /*noWait*/, queueArgs); err != nil {
		return fmt.Errorf("declare queue %s (a PRECONDITION_FAILED here usually means the queue "+
			"already exists with different arguments; expected a durable quorum queue): %w",
			p.cfg.Queue, err)
	}

	// `alert.#` catches every routing key this service produces, including
	// severities it has never seen.
	bindingKey := p.cfg.RoutingPrefix + ".#"
	if err := ch.QueueBind(p.cfg.Queue, bindingKey, p.cfg.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s to %s on %s: %w",
			p.cfg.Queue, p.cfg.Exchange, bindingKey, err)
	}

	return nil
}

// Publish implements Publisher.
func (p *RabbitPublisher) Publish(ctx context.Context, ev event.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event %s: %w", ev.EventID, err)
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		// Persistent. A transient message is dropped when the broker restarts,
		// and an event that reached the queue and then vanished is worse than
		// one that never got there
		DeliveryMode: amqp.Persistent,
		// message_id is the event_id, which is what makes redelivery safe for
		// the consumer: it can deduplicate on this without parsing the body.
		MessageId: ev.EventID,
		// correlation_id is the alert_id, so every event for one alert is
		// visibly related in the management UI and in consumer logs.
		CorrelationId: ev.AlertID,
		Timestamp:     ev.ReceivedAt,
		Type:          "alert.event",
		AppId:         "alertprocessor",
		Headers: amqp.Table{
			"alert_name":   ev.AlertName,
			"alert_status": string(ev.AlertState),
			"severity":     ev.Severity,
			"namespace":    ev.Namespace,
			"resource":     ev.Resource,
			"status":       string(ev.Status),
		},
		Body: body,
	}

	routingKey := ev.RoutingKey(p.cfg.RoutingPrefix)

	err = p.publishOnce(ctx, routingKey, msg)
	if err == nil {
		return nil
	}
	// An unroutable message is a topology problem, not a connection problem;
	// reconnecting would not change the answer and would hide the real cause.
	if errors.Is(err, ErrUnroutable) {
		return err
	}

	// One retry, on a fresh connection. The common case is a channel closed
	// under us by a broker restart or a rolling update, where the first publish
	// after the outage always fails and the second always works.
	p.log.Warn("publish failed, reconnecting and retrying once",
		"event_id", ev.EventID, "error", err)
	if rerr := p.reconnect(ctx); rerr != nil {
		return errors.Join(err, rerr)
	}
	if err := p.publishOnce(ctx, routingKey, msg); err != nil {
		return fmt.Errorf("publish event %s after reconnect: %w", ev.EventID, err)
	}
	return nil
}

func (p *RabbitPublisher) publishOnce(ctx context.Context, routingKey string, msg amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("publisher is closed")
	}
	if p.channel == nil || p.channel.IsClosed() {
		if err := p.reconnectLocked(ctx); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	// Drain any return left over from an earlier message, so the check after
	// the confirm below cannot attribute a stale return to this publish.
	p.drainReturnsLocked()

	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		p.cfg.Exchange,
		routingKey,
		true,  // mandatory -- see ErrUnroutable
		false, // immediate: removed from AMQP 0-9-1; RabbitMQ rejects true
		msg,
	)
	if err != nil {
		return fmt.Errorf("publish to %s with key %s: %w", p.cfg.Exchange, routingKey, err)
	}

	confirmCtx, confirmCancel := context.WithTimeout(ctx, p.cfg.ConfirmTimeout)
	defer confirmCancel()

	acked, err := confirm.WaitContext(confirmCtx)
	if err != nil {
		return fmt.Errorf("wait for broker confirmation of %s: %w", msg.MessageId, err)
	}
	if !acked {
		// A nack means the broker took the message and could not store it --
		// disk full, or a queue that rejected it. Never retry blindly.
		return fmt.Errorf("broker nacked message %s", msg.MessageId)
	}

	// basic.return precedes basic.ack for the same message, so by here any
	// return for this publish has already been delivered to the channel.
	select {
	case ret := <-p.returns:
		return fmt.Errorf("%w: exchange %s, key %s, broker said %d %s",
			ErrUnroutable, ret.Exchange, ret.RoutingKey, ret.ReplyCode, ret.ReplyText)
	default:
	}

	return nil
}

func (p *RabbitPublisher) drainReturnsLocked() {
	for {
		select {
		case ret := <-p.returns:
			p.log.Warn("discarding stale unroutable return",
				"exchange", ret.Exchange, "routing_key", ret.RoutingKey, "message_id", ret.MessageId)
		default:
			return
		}
	}
}

// Ping implements Publisher.
func (p *RabbitPublisher) Ping(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("publisher is closed")
	}
	if p.conn == nil || p.conn.IsClosed() || p.channel == nil || p.channel.IsClosed() {
		// Reconnect rather than just reporting unhealthy: readiness is polled
		// every 10s, which makes it the natural place to heal the connection
		// after an outage, before a webhook arrives and pays for the reconnect.
		if err := p.reconnectLocked(ctx); err != nil {
			return fmt.Errorf("rabbitmq unreachable: %w", err)
		}
	}
	return nil
}
