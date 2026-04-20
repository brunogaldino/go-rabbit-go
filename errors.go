package rabbitmq

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by the library.
var (
	// ErrConnectionClosed is returned when a publish is attempted on a
	// connection that is shutting down.
	ErrConnectionClosed = errors.New("rabbitmq: connection is closed")
	// ErrConnectionBlocked is returned when the broker remains blocked
	// after the maximum wait period.
	ErrConnectionBlocked = errors.New("rabbitmq: connection is blocked after max wait")
	// ErrMaxReconnectAttempts is returned when all reconnection attempts
	// have been exhausted.
	ErrMaxReconnectAttempts = errors.New("rabbitmq: max reconnect attempts reached")
)

// ChannelError is returned when a channel operation fails.
// It is shared across the [client], [consumer], and [publisher] packages.
type ChannelError struct {
	Operation string // "open", "close", "qos", "confirm"
	Err       error
}

func (e *ChannelError) Error() string {
	return fmt.Sprintf("rabbitmq: channel %s failed: %v", e.Operation, e.Err)
}

func (e *ChannelError) Unwrap() error { return e.Err }
