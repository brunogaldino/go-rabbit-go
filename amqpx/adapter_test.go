package amqpx

import (
	"testing"
)

func TestConnAdapter_ImplementsAMQPConnection(t *testing.T) {
	// Compile-time check: *ConnAdapter satisfies AMQPConnection.
	var _ AMQPConnection = (*ConnAdapter)(nil)
}

func TestDefaultDialer_ImplementsDialer(t *testing.T) {
	// Compile-time check: *DefaultDialer satisfies Dialer.
	var _ Dialer = (*DefaultDialer)(nil)
}
