package manager

import "fmt"

// ConnectionError is returned when a named connection fails.
type ConnectionError struct {
	Name string
	Err  error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("rabbitmq: connection %q failed: %v", e.Name, e.Err)
}

func (e *ConnectionError) Unwrap() error { return e.Err }
