// Package publisher provides a message publisher for AMQP with
// publisher confirms, exchange declaration, and automatic channel
// reconnection.
//
// A [Publisher] is created via [New] with a [ConnProvider] (typically a
// [github.com/brunogaldino/go-rabbit-go/client.Client]).
package publisher

import (
	"context"
	"fmt"
	"sync"
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
	maxReconnect    = 5
	reconnectBase   = 1 * time.Second
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
// channel. It supports publisher confirms and automatic channel
// reconnection.
type Publisher struct {
	conn             ConnProvider
	ch               amqpx.AMQPChannel
	wg               *sync.WaitGroup
	publishConfirms  bool
	notifyChanClose  chan *amqp.Error
	config           []ExchangeOption
	reconnectAttempt int

	// isConnected/isReconnecting are lifecycle flags managed via manual
	// Add/Done because Connect() and Disconnect() are separate call sites.
	isConnected    bool
	isReconnecting bool
}

// New creates a [Publisher] that declares the given exchanges. It
// registers itself with the [ConnProvider] for lifecycle management.
func New(conn ConnProvider, config []ExchangeOption) (*Publisher, error) {
	pub := &Publisher{
		config:          config,
		conn:            conn,
		publishConfirms: true,
		wg:              &sync.WaitGroup{},
	}

	if err := pub.Connect(); err != nil {
		return nil, err
	}

	conn.SetPublisher(pub)

	return pub, nil
}

// Connect opens a new AMQP channel, enables publisher confirms, and
// declares all configured exchanges. It is called internally by [New]
// and by the client after a connection-level reconnection.
func (p *Publisher) Connect() error {
	ch, err := p.conn.Channel()
	if err != nil {
		return &rabbitmq.ChannelError{Operation: "open", Err: err}
	}

	p.notifyChanClose = make(chan *amqp.Error)
	ch.NotifyClose(p.notifyChanClose)
	go p.monitorChannel()

	if !p.publishConfirms {
		p.ch = ch
		p.isConnected = true
		// Lifecycle tracking: Add here, Done in Disconnect.
		p.wg.Add(1)
		return nil
	}

	if err := ch.Confirm(false); err != nil {
		return &rabbitmq.ChannelError{Operation: "confirm", Err: err}
	}

	if err := p.declareExchanges(ch); err != nil {
		return err
	}

	// Lifecycle tracking: Add here, Done in Disconnect.
	p.wg.Add(1)
	p.ch = ch
	p.isConnected = true

	return nil
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

// Disconnect closes the publisher channel. It is safe to call even if
// the channel is already closed.
func (p *Publisher) Disconnect() error {
	if p.ch.IsClosed() {
		p.wg.Done()
		return nil
	}

	p.conn.Logger().Info("closing publisher channel")
	// TODO: Verificar se tem alguma mensagem presa em buffer antes de fechar
	if err := p.ch.Close(); err != nil {
		return &rabbitmq.ChannelError{Operation: "close", Err: err}
	}

	p.wg.Done()

	return nil
}

func (p *Publisher) reconnect() {
	if p.reconnectAttempt >= maxReconnect {
		p.conn.Logger().Error("maximum publisher channel reconnection attempts reached")
		p.isConnected = false
		p.isReconnecting = false
		return
	}

	p.isReconnecting = true
	p.reconnectAttempt++
	reconnSleep := reconnectBase * time.Duration(p.reconnectAttempt)
	p.conn.Logger().Info(fmt.Sprintf("reconnecting publisher channel in %.0fs: %d of %d attempts",
		reconnSleep.Seconds(), p.reconnectAttempt, maxReconnect))
	time.Sleep(reconnSleep)

	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			p.conn.Logger().Error(fmt.Sprintf("close old publisher channel: %v", err))
		}
	}

	p.Connect()
}

// Publish sends a message to the broker. If the connection is blocked or
// reconnecting, it waits up to 25 seconds before returning an error.
// When publisher confirms are enabled (the default), it blocks until
// the broker acknowledges the message.
func (p *Publisher) Publish(msg Message) error {
	if err := p.waitForConnection(); err != nil {
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

	confirmation, err := p.ch.PublishWithDeferredConfirm(msg.Exchange,
		msg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:   msg.ContentType,
			CorrelationId: msg.CorrelationId,
			Body:          msg.Message,
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

	return p.ch.PublishWithContext(ctx, msg.Exchange, msg.RoutingKey, false, false, amqp.Publishing{
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

func (p *Publisher) monitorChannel() {
	for err := range p.notifyChanClose {
		if err != nil && !err.Recover {
			p.conn.Logger().Error(fmt.Sprintf("shutting down publisher channel permanently: %s", err.Reason))
			p.isConnected = false
			return
		}

		if err != nil {
			p.conn.Logger().Error(fmt.Sprintf("publisher channel closed: %s (recoverable: %t)", err.Reason, err.Recover))
		}
		p.reconnect()
	}
}
