// Package conn provides connection state management for AMQP connections.
package conn

import (
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// Managed encapsulates the state for a single AMQP connection
// (either publisher or consumer side).
type Managed struct {
	Conn             amqpx.AMQPConnection
	NotifyError      chan *amqp.Error
	ReconnectAttempt int
	IsConnected      atomic.Bool
	IsReconnecting   atomic.Bool
}

// MarkConnected sets the connection as active and clears the reconnecting flag.
func (mc *Managed) MarkConnected() {
	mc.IsConnected.Store(true)
	mc.IsReconnecting.Store(false)
}

// MarkDisconnected sets the connection as inactive.
func (mc *Managed) MarkDisconnected() {
	mc.IsConnected.Store(false)
}

// Close gracefully closes the underlying AMQP connection if it is open.
func (mc *Managed) Close() {
	if mc.Conn == nil || mc.Conn.IsClosed() {
		return
	}

	mc.Conn.Close()
}
