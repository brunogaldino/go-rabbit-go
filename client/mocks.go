package client

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// --- mockDialer ---

type mockDialer struct {
	dialFn func(uri string, config amqp.Config) (amqpx.AMQPConnection, error)
}

func (d *mockDialer) Dial(uri string, config amqp.Config) (amqpx.AMQPConnection, error) {
	return d.dialFn(uri, config)
}

// --- mockAMQPConnection ---

type mockAMQPConnection struct {
	channelFn       func() (amqpx.AMQPChannel, error)
	notifyCloseFn   func(chan *amqp.Error) chan *amqp.Error
	notifyBlockedFn func(chan amqp.Blocking) chan amqp.Blocking
	isClosedFn      func() bool
	closeFn         func() error
}

func (c *mockAMQPConnection) Channel() (amqpx.AMQPChannel, error) {
	if c.channelFn != nil {
		return c.channelFn()
	}

	return &mockAMQPChannel{}, nil
}

func (c *mockAMQPConnection) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	if c.notifyCloseFn != nil {
		return c.notifyCloseFn(ch)
	}
	return ch
}

func (c *mockAMQPConnection) NotifyBlocked(ch chan amqp.Blocking) chan amqp.Blocking {
	if c.notifyBlockedFn != nil {
		return c.notifyBlockedFn(ch)
	}

	return ch
}

func (c *mockAMQPConnection) IsClosed() bool {
	if c.isClosedFn != nil {
		return c.isClosedFn()
	}

	return false
}

func (c *mockAMQPConnection) Close() error {
	if c.closeFn != nil {
		return c.closeFn()
	}

	return nil
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

func (ch *mockAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if ch.qosFn != nil {
		return ch.qosFn(prefetchCount, prefetchSize, global)
	}

	return nil
}

func (ch *mockAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	if ch.queueDeclareFn != nil {
		return ch.queueDeclareFn(name, durable, autoDelete, exclusive, noWait, args)
	}

	return amqp.Queue{}, nil
}

func (ch *mockAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	if ch.queueBindFn != nil {
		return ch.queueBindFn(name, key, exchange, noWait, args)
	}

	return nil
}

func (ch *mockAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if ch.consumeFn != nil {
		return ch.consumeFn(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	}

	return nil, nil
}

func (ch *mockAMQPChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if ch.publishFn != nil {
		return ch.publishFn(exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (ch *mockAMQPChannel) PublishWithDeferredConfirm(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if ch.publishWithDeferredConfirmFn != nil {
		return ch.publishWithDeferredConfirmFn(exchange, key, mandatory, immediate, msg)
	}

	return nil, nil
}

func (ch *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if ch.publishWithContextFn != nil {
		return ch.publishWithContextFn(ctx, exchange, key, mandatory, immediate, msg)
	}

	return nil
}

func (ch *mockAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	if ch.exchangeDeclareFn != nil {
		return ch.exchangeDeclareFn(name, kind, durable, autoDelete, internal, noWait, args)
	}

	return nil
}

func (ch *mockAMQPChannel) Confirm(noWait bool) error {
	if ch.confirmFn != nil {
		return ch.confirmFn(noWait)
	}

	return nil
}

func (ch *mockAMQPChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	if ch.notifyPublishFn != nil {
		return ch.notifyPublishFn(confirm)
	}

	return confirm
}

func (ch *mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	if ch.notifyCloseFn != nil {
		return ch.notifyCloseFn(c)
	}

	return c
}

func (ch *mockAMQPChannel) Cancel(consumer string, noWait bool) error {
	if ch.cancelFn != nil {
		return ch.cancelFn(consumer, noWait)
	}

	return nil
}

func (ch *mockAMQPChannel) Close() error {
	if ch.closeFn != nil {
		return ch.closeFn()
	}

	return nil
}

func (ch *mockAMQPChannel) IsClosed() bool {
	if ch.isClosedFn != nil {
		return ch.isClosedFn()
	}

	return false
}

// --- mockLogger ---

type mockLogger struct {
	mu       sync.Mutex
	infos    []string
	errors   []string
	disabled bool
}

func (l *mockLogger) Info(msg string, data ...map[string]any) {
	if l.disabled {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, msg)
}

func (l *mockLogger) Error(msg string, data ...map[string]any) {
	if l.disabled {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}

func (l *mockLogger) Fatal(msg string, data ...map[string]any) {
	if l.disabled {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, msg)
}

var (
	_ amqpx.Dialer         = (*mockDialer)(nil)
	_ amqpx.AMQPConnection = (*mockAMQPConnection)(nil)
	_ amqpx.AMQPChannel    = (*mockAMQPChannel)(nil)
	_ rabbitmq.Logger      = (*mockLogger)(nil)
)
