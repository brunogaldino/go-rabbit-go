package conn

import (
	"github.com/brunogaldino/go-rabbit-go/amqpx"
	amqp "github.com/rabbitmq/amqp091-go"
)

type mockAMQPConnection struct {
	channelFn     func() (amqpx.AMQPChannel, error)
	notifyCloseFn func(chan *amqp.Error) chan *amqp.Error
	notifyBlockFn func(chan amqp.Blocking) chan amqp.Blocking
	isClosedFn    func() bool
	closeFn       func() error
}

func (m *mockAMQPConnection) Channel() (amqpx.AMQPChannel, error) {
	if m.channelFn != nil {
		return m.channelFn()
	}
	return nil, nil
}

func (m *mockAMQPConnection) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	if m.notifyCloseFn != nil {
		return m.notifyCloseFn(c)
	}
	return c
}

func (m *mockAMQPConnection) NotifyBlocked(c chan amqp.Blocking) chan amqp.Blocking {
	if m.notifyBlockFn != nil {
		return m.notifyBlockFn(c)
	}
	return c
}

func (m *mockAMQPConnection) IsClosed() bool {
	if m.isClosedFn != nil {
		return m.isClosedFn()
	}
	return false
}

func (m *mockAMQPConnection) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}
