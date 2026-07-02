// Package publisher provides a message publisher for AMQP with
// publisher confirms, exchange declaration, and lazy channel re-open.
//
// A [Publisher] owns a single AMQP channel. Channel-only failures are
// recovered lazily on the next publish; connection-level drops are
// recovered by the client's connection monitor, which re-dials and calls
// [Publisher.Connect].
//
// A [Publisher] is created via [New] with a [ConnProvider] (typically a
// [github.com/brunogaldino/go-rabbit-go/client.Client]).
package publisher

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// Exchange type constants for use in [ExchangeOption].
const (
	ExchangeDirect = "direct"
	ExchangeTopic  = "topic"
	ExchangeFanout = "fanout"
	ExchangeHeader = "header"
)

// DefaultContentType is the content type used when none is specified.
const DefaultContentType = "application/json"

// Publisher timing and limit defaults.
const (
	publishTimeout  = 5 * time.Second
	blockMaxRetries = 5
	blockDelay      = 5 * time.Second
)

// ConnProvider defines what a [Publisher] needs from its connection
// owner (typically a [github.com/brunogaldino/go-rabbit-go/client.Client]).
type ConnProvider interface {
	Channel() (amqpx.AMQPChannel, error)
	Blocked() bool
	Reconnecting() bool
	Closing() bool
	Logger() rabbitmq.Logger
	LogType() rabbitmq.LogType
	SetPublisher(p *Publisher)
}

// ExchangeDeclarationOptions configures how an exchange is declared.
type ExchangeDeclarationOptions struct {
	Durable    *bool
	AutoDelete *bool
}

// ExchangeOption describes an exchange to declare when creating a [Publisher].
type ExchangeOption struct {
	Name    string
	Type    string
	Options ExchangeDeclarationOptions
}

// Message is the payload passed to [Publisher.Publish].
type Message struct {
	Exchange      string
	RoutingKey    string
	Message       []byte
	ContentType   string
	Headers       map[string]any
	CorrelationId string
}

// Publisher publishes messages to the broker through a single AMQP
// channel. It supports publisher confirms and lazily re-opens its
// channel when it has been closed. Connection-level recovery is owned by
// the client's connection monitor, which calls [Publisher.Connect] after
// re-dialing.
type Publisher struct {
	conn            ConnProvider
	ch              amqpx.AMQPChannel
	publishConfirms bool
	config          []ExchangeOption
	disconnectOnce  sync.Once
	closing         atomic.Bool

	// mu guards ch and isConnected, which are mutated by Connect (called
	// from New, the client's reconnection, and lazy re-open) and read by
	// the publish path concurrently.
	mu          sync.Mutex
	isConnected bool
}

// New creates a [Publisher] that declares the given exchanges. It
// registers itself with the [ConnProvider] for lifecycle management.
func New(conn ConnProvider, config []ExchangeOption) (*Publisher, error) {
	pub := &Publisher{
		config:          config,
		conn:            conn,
		publishConfirms: true,
	}

	if err := pub.Connect(); err != nil {
		return nil, err
	}

	conn.SetPublisher(pub)

	return pub, nil
}

// Connect opens a new AMQP channel, enables publisher confirms (when
// configured), and declares all configured exchanges. It is called by
// [New], by the client after a connection-level reconnection, and
// lazily before publishing when the channel has been closed. It is safe
// to call repeatedly and concurrently.
func (p *Publisher) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.openLocked()
}

// openLocked opens the channel and prepares it. Callers must hold p.mu.
func (p *Publisher) openLocked() error {
	ch, err := p.conn.Channel()
	if err != nil {
		return &rabbitmq.ChannelError{Operation: "open", Err: err}
	}

	if p.publishConfirms {
		if err := ch.Confirm(false); err != nil {
			return &rabbitmq.ChannelError{Operation: "confirm", Err: err}
		}

		if err := p.declareExchanges(ch); err != nil {
			return err
		}
	}

	p.ch = ch
	p.isConnected = true

	return nil
}

// channel returns the current channel under lock.
func (p *Publisher) channel() amqpx.AMQPChannel {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch
}

// ensureChannel re-opens the publisher channel if it has been closed
// (for example after a channel-only failure). Connection-level drops are
// recovered by the client's connection monitor, which calls Connect.
func (p *Publisher) ensureChannel() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch != nil && !p.ch.IsClosed() {
		return nil
	}

	if p.closing.Load() || p.conn.Closing() {
		return rabbitmq.ErrConnectionClosed
	}

	return p.openLocked()
}

func (p *Publisher) declareExchanges(ch amqpx.AMQPChannel) error {
	for _, e := range p.config {
		durable := true
		if e.Options.Durable != nil {
			durable = *e.Options.Durable
		}

		autoDelete := false
		if e.Options.AutoDelete != nil {
			autoDelete = *e.Options.AutoDelete
		}

		if err := ch.ExchangeDeclare(e.Name, e.Type, durable, autoDelete, false, false, nil); err != nil {
			return &ExchangeError{Name: e.Name, Err: err}
		}
	}
	return nil
}

// Disconnect closes the publisher channel. It is idempotent and safe to
// call multiple times and from concurrent goroutines: the teardown runs
// exactly once.
func (p *Publisher) Disconnect() error {
	var err error
	p.disconnectOnce.Do(func() {
		p.closing.Store(true)

		p.mu.Lock()
		defer p.mu.Unlock()

		if p.ch == nil || p.ch.IsClosed() {
			return
		}

		p.conn.Logger().Info("closing publisher channel")
		if closeErr := p.ch.Close(); closeErr != nil {
			err = &rabbitmq.ChannelError{Operation: "close", Err: closeErr}
		}
	})

	return err
}

// Publish sends a message to the broker. If the connection is blocked or
// reconnecting, it waits up to 25 seconds before returning an error.
// When publisher confirms are enabled (the default), it blocks until
// the broker acknowledges the message.
func (p *Publisher) Publish(msg Message) error {
	if err := p.waitForConnection(); err != nil {
		return err
	}

	if err := p.ensureChannel(); err != nil {
		return err
	}

	start := time.Now()
	var publishErr error

	if p.publishConfirms {
		publishErr = p.publishWithConfirmation(msg)
	} else {
		publishErr = p.publishWithoutConfirmation(msg)
	}

	elapsed := time.Since(start)
	if p.conn.LogType().Includes(rabbitmq.LogTypePublisher) || publishErr != nil {
		p.inspect(msg, elapsed, publishErr)
	}

	return publishErr
}

func (p *Publisher) waitForConnection() error {
	if p.conn.Blocked() || p.conn.Reconnecting() {
		return p.waitForUnblock()
	}

	if p.conn.Closing() {
		return rabbitmq.ErrConnectionClosed
	}

	return nil
}

func (p *Publisher) waitForUnblock() error {
	for range blockMaxRetries {
		time.Sleep(blockDelay)
		if !p.conn.Blocked() && !p.conn.Reconnecting() {
			return nil
		}
	}

	return rabbitmq.ErrConnectionBlocked
}

func (p *Publisher) publishWithConfirmation(msg Message) error {
	if msg.ContentType == "" {
		msg.ContentType = DefaultContentType
	}

	confirmation, err := p.channel().PublishWithDeferredConfirm(msg.Exchange,
		msg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:   msg.ContentType,
			CorrelationId: msg.CorrelationId,
			Body:          msg.Message,
			DeliveryMode:  amqp.Persistent,
			Headers: amqpx.MergeTable(amqp.Table{
				amqpx.KeyOriginalExchange: msg.Exchange,
				amqpx.KeyOriginalRouteKey: msg.RoutingKey,
				amqpx.KeyPublishedAt:      time.Now().String(),
			}, msg.Headers),
		})
	if err != nil {
		return err
	}

	if confirmation.Wait() {
		return nil
	}

	return &PublishError{Tag: confirmation.DeliveryTag, Reason: "nack"}
}

func (p *Publisher) publishWithoutConfirmation(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	return p.channel().PublishWithContext(ctx, msg.Exchange, msg.RoutingKey, false, false, amqp.Publishing{
		ContentType:   DefaultContentType,
		CorrelationId: msg.CorrelationId,
		Body:          msg.Message,
		DeliveryMode:  amqp.Persistent,
		Headers: amqpx.MergeTable(amqp.Table{
			amqpx.KeyOriginalExchange: msg.Exchange,
			amqpx.KeyOriginalRouteKey: msg.RoutingKey,
			amqpx.KeyPublishedAt:      time.Now().String(),
		}, msg.Headers),
	})
}

// --- Inspection logging ---

func (p *Publisher) inspect(msg Message, elapsed time.Duration, err error) {
	title := fmt.Sprintf("[AMQP] [PUBLISH] [%s] [%s]",
		msg.Exchange, msg.RoutingKey)

	data := map[string]any{
		"type":          "publisher",
		"duration":      elapsed.Milliseconds(),
		"correlationId": msg.CorrelationId,
		"binding": map[string]any{
			"exchange":   msg.Exchange,
			"routingKey": msg.RoutingKey,
		},
		"publishedMessage": map[string]any{
			"content": string(msg.Message),
			"headers": msg.Headers,
		},
	}

	if err != nil {
		data["error"] = err.Error()
		p.conn.Logger().Error(title, data)
		return
	}

	p.conn.Logger().Info(title, data)
}
