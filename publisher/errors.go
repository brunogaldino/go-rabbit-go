package publisher

import "fmt"

// ExchangeError is returned when an exchange declaration fails.
type ExchangeError struct {
	Name string
	Err  error
}

func (e *ExchangeError) Error() string {
	return fmt.Sprintf("rabbitmq: could not declare exchange %q: %v", e.Name, e.Err)
}

func (e *ExchangeError) Unwrap() error { return e.Err }

// PublishError is returned when a publish operation fails.
type PublishError struct {
	Tag    uint64
	Reason string
	Err    error
}

func (e *PublishError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("rabbitmq: publish failed: %v", e.Err)
	}

	return fmt.Sprintf("rabbitmq: publish nack for delivery tag %d", e.Tag)
}

func (e *PublishError) Unwrap() error { return e.Err }
