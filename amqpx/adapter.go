package amqpx

import amqp "github.com/rabbitmq/amqp091-go"

// ConnAdapter wraps *amqp.Connection to satisfy AMQPConnection.
// Needed because Connection.Channel() returns (*amqp.Channel, error)
// while AMQPConnection.Channel() returns (AMQPChannel, error).
type ConnAdapter struct {
	Conn *amqp.Connection
}

func (a *ConnAdapter) Channel() (AMQPChannel, error) {
	ch, err := a.Conn.Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (a *ConnAdapter) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return a.Conn.NotifyClose(c)
}

func (a *ConnAdapter) NotifyBlocked(c chan amqp.Blocking) chan amqp.Blocking {
	return a.Conn.NotifyBlocked(c)
}

func (a *ConnAdapter) IsClosed() bool { return a.Conn.IsClosed() }
func (a *ConnAdapter) Close() error   { return a.Conn.Close() }

// DefaultDialer dials an AMQP broker using amqp.DialConfig.
type DefaultDialer struct{}

func (d *DefaultDialer) Dial(uri string, config amqp.Config) (AMQPConnection, error) {
	conn, err := amqp.DialConfig(uri, config)
	if err != nil {
		return nil, err
	}

	return &ConnAdapter{Conn: conn}, nil
}
