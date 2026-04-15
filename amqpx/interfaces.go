// Package amqpx provides AMQP interface abstractions for testability
// and internal decoupling from the amqp091-go concrete types.
package amqpx

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPConnection abstracts *amqp.Connection so that production code and
// tests can be decoupled from real broker connections.
type AMQPConnection interface {
	Channel() (AMQPChannel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	NotifyBlocked(receiver chan amqp.Blocking) chan amqp.Blocking
	IsClosed() bool
	Close() error
}

// AMQPChannel abstracts *amqp.Channel for testability.
type AMQPChannel interface {
	Qos(prefetchCount, prefetchSize int, global bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Cancel(consumer string, noWait bool) error
	Close() error
	IsClosed() bool
}

// Dialer abstracts the AMQP dial operation for testability.
type Dialer interface {
	Dial(uri string, config amqp.Config) (AMQPConnection, error)
}
