package consumer

import "fmt"

// QueueError is returned when a queue operation fails.
type QueueError struct {
	Operation string // "declare", "bind", "declare-dlq", "declare-retry"
	Queue     string
	Err       error
}

func (e *QueueError) Error() string {
	return fmt.Sprintf("rabbitmq: queue %s %s failed: %v", e.Operation, e.Queue, e.Err)
}

func (e *QueueError) Unwrap() error { return e.Err }
