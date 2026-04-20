package client

import "fmt"

// DialError is returned when a connection to the broker fails.
type DialError struct {
	Role string // "publisher" or "consumer"
	Err  error
}

func (e *DialError) Error() string {
	return fmt.Sprintf("rabbitmq: could not connect to broker (%s): %v", e.Role, e.Err)
}

func (e *DialError) Unwrap() error { return e.Err }
