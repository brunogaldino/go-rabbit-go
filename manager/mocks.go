package manager

import (
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

	return nil, nil
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

// --- mockLogger ---

type mockLogger struct {
	mu       sync.Mutex
	infos    []string
	errors   []string
	disabled bool
}

func (l *mockLogger) Info(msg string, args ...any) {
	if l.disabled {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, msg)
}

func (l *mockLogger) Error(msg string, args ...any) {
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
	_ rabbitmq.Logger      = (*mockLogger)(nil)
)
