package consumer

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// --- mockConnProvider ---

type mockConnProvider struct {
	channelFn            func() (amqpx.AMQPChannel, error)
	connectedFn          func() bool
	closingFn            func() bool
	hostFn               func() string
	loggerFn             func() rabbitmq.Logger
	logTypeFn            func() rabbitmq.LogType
	registerConsumerFn   func(string, *Consumer)
	unregisterConsumerFn func(string)
}

func (m *mockConnProvider) Channel() (amqpx.AMQPChannel, error) {
	if m.channelFn != nil {
		return m.channelFn()
	}

	return &mockAMQPChannel{}, nil
}

func (m *mockConnProvider) Connected() bool {
	if m.connectedFn != nil {
		return m.connectedFn()
	}

	return true
}

func (m *mockConnProvider) Closing() bool {
	if m.closingFn != nil {
		return m.closingFn()
	}

	return false
}

func (m *mockConnProvider) Host() string {
	if m.hostFn != nil {
		return m.hostFn()
	}

	return "test-host"
}

func (m *mockConnProvider) Logger() rabbitmq.Logger {
	if m.loggerFn != nil {
		return m.loggerFn()
	}

	return &mockLogger{}
}

func (m *mockConnProvider) LogType() rabbitmq.LogType {
	if m.logTypeFn != nil {
		return m.logTypeFn()
	}

	return rabbitmq.LogTypeNone
}

func (m *mockConnProvider) RegisterConsumer(name string, c *Consumer) {
	if m.registerConsumerFn != nil {
		m.registerConsumerFn(name, c)
	}
}

func (m *mockConnProvider) UnregisterConsumer(name string) {
	if m.unregisterConsumerFn != nil {
		m.unregisterConsumerFn(name)
	}
}

// --- mockAMQPChannel ---

type mockAMQPChannel struct {
	qosFn                        func(int, int, bool) error
	queueDeclareFn               func(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	queueBindFn                  func(string, string, string, bool, amqp.Table) error
	consumeFn                    func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	publishFn                    func(string, string, bool, bool, amqp.Publishing) error
	publishWithDeferredConfirmFn func(string, string, bool, bool, amqp.Publishing) (*amqp.DeferredConfirmation, error)
	publishWithContextFn         func(context.Context, string, string, bool, bool, amqp.Publishing) error
	exchangeDeclareFn            func(string, string, bool, bool, bool, bool, amqp.Table) error
	confirmFn                    func(bool) error
	notifyPublishFn              func(chan amqp.Confirmation) chan amqp.Confirmation
	notifyCloseFn                func(chan *amqp.Error) chan *amqp.Error
	cancelFn                     func(string, bool) error
	closeFn                      func() error
	isClosedFn                   func() bool
}

func (m *mockAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if m.qosFn != nil {
		return m.qosFn(prefetchCount, prefetchSize, global)
	}

	return nil
}

func (m *mockAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if m.queueDeclareFn != nil {
		return m.queueDeclareFn(name, durable, autoDelete, exclusive, noWait, args)
	}

	return amqp.Queue{}, nil
}

func (m *mockAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if m.queueBindFn != nil {
		return m.queueBindFn(name, key, exchange, noWait, args)
	}

	return nil
}

func (m *mockAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if m.consumeFn != nil {
		return m.consumeFn(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	}

	ch := make(chan amqp.Delivery)
	close(ch)

	return ch, nil
}

func (m *mockAMQPChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.publishFn != nil {
		return m.publishFn(exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (m *mockAMQPChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if m.publishWithDeferredConfirmFn != nil {
		return m.publishWithDeferredConfirmFn(exchange, key, mandatory, immediate, msg)
	}

	return nil, nil
}

func (m *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.publishWithContextFn != nil {
		return m.publishWithContextFn(ctx, exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (m *mockAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if m.exchangeDeclareFn != nil {
		return m.exchangeDeclareFn(name, kind, durable, autoDelete, internal, noWait, args)
	}

	return nil
}

func (m *mockAMQPChannel) Confirm(noWait bool) error {
	if m.confirmFn != nil {
		return m.confirmFn(noWait)
	}

	return nil
}

func (m *mockAMQPChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	if m.notifyPublishFn != nil {
		return m.notifyPublishFn(confirm)
	}

	return confirm
}

func (m *mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	if m.notifyCloseFn != nil {
		return m.notifyCloseFn(c)
	}

	return c
}

func (m *mockAMQPChannel) Cancel(consumer string, noWait bool) error {
	if m.cancelFn != nil {
		return m.cancelFn(consumer, noWait)
	}

	return nil
}

func (m *mockAMQPChannel) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}

	return nil
}

func (m *mockAMQPChannel) IsClosed() bool {
	if m.isClosedFn != nil {
		return m.isClosedFn()
	}

	return false
}

// --- mockAcknowledger ---

type mockAcknowledger struct {
	ackFn    func(uint64, bool) error
	nackFn   func(uint64, bool, bool) error
	rejectFn func(uint64, bool) error
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	if m.ackFn != nil {
		return m.ackFn(tag, multiple)
	}

	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if m.nackFn != nil {
		return m.nackFn(tag, multiple, requeue)
	}

	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	if m.rejectFn != nil {
		return m.rejectFn(tag, requeue)
	}

	return nil
}

// --- mockLogger ---

type mockLogger struct {
	infoFn  func(string, ...map[string]any)
	errorFn func(string, ...map[string]any)
}

func (m *mockLogger) Info(msg string, data ...map[string]any) {
	if m.infoFn != nil {
		m.infoFn(msg, data...)
	}
}

func (m *mockLogger) Error(msg string, data ...map[string]any) {
	if m.errorFn != nil {
		m.errorFn(msg, data...)
	}
}
