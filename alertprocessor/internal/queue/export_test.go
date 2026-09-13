//go:build integration

package queue

// CloseUnderlyingConnection drops the publisher's AMQP connection without
// marking the publisher closed, simulating what a broker restart or a rolling
// update looks like from inside the process.
//
// It lives in an export_test.go so the capability exists only during the
// integration build. Production code must never be able to put the publisher
// into this state deliberately: the whole point is that it is something that
// happens TO the service, and the reconnect path is what the test exercises.
func CloseUnderlyingConnection(p *RabbitPublisher) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Closing the connection closes its channels too, so the next publish finds
	// p.channel.IsClosed() true and reconnects.
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
