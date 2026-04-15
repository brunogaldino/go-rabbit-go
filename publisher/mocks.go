package publisher

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// mockConnProvider implements ConnProvider for testing.
type mockConnProvider struct {
	ChannelFn      func() (amqpx.AMQPChannel, error)
	BlockedFn      func() bool
	ReconnectingFn func() bool
	ClosingFn      func() bool
	LoggerFn       func() rabbitmq.Logger
	SetPublisherFn func(p *Publisher)
}

func (m *mockConnProvider) Channel() (amqpx.AMQPChannel, error) {
	if m.ChannelFn != nil {
		return m.ChannelFn()
	}

	return nil, nil
}

func (m *mockConnProvider) Blocked() bool {
	if m.BlockedFn != nil {
		return m.BlockedFn()
	}

	return false
}

func (m *mockConnProvider) Reconnecting() bool {
	if m.ReconnectingFn != nil {
		return m.ReconnectingFn()
	}

	return false
}

func (m *mockConnProvider) Closing() bool {
	if m.ClosingFn != nil {
		return m.ClosingFn()
	}

	return false
}

func (m *mockConnProvider) Logger() rabbitmq.Logger {
	if m.LoggerFn != nil {
		return m.LoggerFn()
	}

	return &mockLogger{}
}

func (m *mockConnProvider) SetPublisher(p *Publisher) {
	if m.SetPublisherFn != nil {
		m.SetPublisherFn(p)
	}
}

// mockAMQPChannel implements amqpx.AMQPChannel for testing.
type mockAMQPChannel struct {
	QosFn                        func(prefetchCount, prefetchSize int, global bool) error
	QueueDeclareFn               func(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBindFn                  func(name, key, exchange string, noWait bool, args amqp.Table) error
	ConsumeFn                    func(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	PublishFn                    func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	PublishWithDeferredConfirmFn func(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	PublishWithContextFn         func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclareFn            func(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ConfirmFn                    func(noWait bool) error
	NotifyPublishFn              func(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyCloseFn                func(c chan *amqp.Error) chan *amqp.Error
	CancelFn                     func(consumer string, noWait bool) error
	CloseFn                      func() error
	IsClosedFn                   func() bool
}

func (m *mockAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if m.QosFn != nil {
		return m.QosFn(prefetchCount, prefetchSize, global)
	}

	return nil
}

func (m *mockAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if m.QueueDeclareFn != nil {
		return m.QueueDeclareFn(name, durable, autoDelete, exclusive, noWait, args)
	}

	return amqp.Queue{}, nil
}

func (m *mockAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if m.QueueBindFn != nil {
		return m.QueueBindFn(name, key, exchange, noWait, args)
	}

	return nil
}

func (m *mockAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if m.ConsumeFn != nil {
		return m.ConsumeFn(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	}

	return nil, nil
}

func (m *mockAMQPChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.PublishFn != nil {
		return m.PublishFn(exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (m *mockAMQPChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if m.PublishWithDeferredConfirmFn != nil {
		return m.PublishWithDeferredConfirmFn(exchange, key, mandatory, immediate, msg)
	}

	return nil, nil
}

func (m *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.PublishWithContextFn != nil {
		return m.PublishWithContextFn(ctx, exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (m *mockAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if m.ExchangeDeclareFn != nil {
		return m.ExchangeDeclareFn(name, kind, durable, autoDelete, internal, noWait, args)
	}

	return nil
}

func (m *mockAMQPChannel) Confirm(noWait bool) error {
	if m.ConfirmFn != nil {
		return m.ConfirmFn(noWait)
	}

	return nil
}

func (m *mockAMQPChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	if m.NotifyPublishFn != nil {
		return m.NotifyPublishFn(confirm)
	}

	return confirm
}

func (m *mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	if m.NotifyCloseFn != nil {
		return m.NotifyCloseFn(c)
	}

	return c
}

func (m *mockAMQPChannel) Cancel(consumer string, noWait bool) error {
	if m.CancelFn != nil {
		return m.CancelFn(consumer, noWait)
	}

	return nil
}

func (m *mockAMQPChannel) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}

	return nil
}

func (m *mockAMQPChannel) IsClosed() bool {
	if m.IsClosedFn != nil {
		return m.IsClosedFn()
	}

	return false
}

// mockLogger implements rabbitmq.Logger for testing.
type mockLogger struct {
	InfoFn  func(msg string, args ...any)
	ErrorFn func(msg string, args ...any)
}

func (m *mockLogger) Info(msg string, args ...any) {
	if m.InfoFn != nil {
		m.InfoFn(msg, args...)
	}
}

func (m *mockLogger) Error(msg string, args ...any) {
	if m.ErrorFn != nil {
		m.ErrorFn(msg, args...)
	}
}
