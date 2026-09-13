// Package queue publishes events to RabbitMQ for harness/ to consume.
package queue

import (
	"context"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/event"
)

// Publisher is the messaging contract the processor depends on. An interface so
// the handler can be unit-tested without a broker; RabbitPublisher is the only
// production implementation.
type Publisher interface {
	// Publish sends ev and returns only once the broker has confirmed it. A nil
	// error means the message is durably on the queue -- the processor records
	// published_at on the strength of it, so anything weaker than a confirm
	// would make that column a lie.
	Publish(ctx context.Context, ev event.Event) error

	// Ping reports whether the broker is reachable. /readyz only.
	Ping(ctx context.Context) error
}
