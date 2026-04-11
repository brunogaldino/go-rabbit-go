package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeDirect = "direct"
	ExchangeTopic  = "topic"
	ExchangeFanout = "fanout"
	ExchangeHeader = "header"
)

var errConnClosed = errors.New("RabbitMQ connection is closed")

type ExchangeDeclarationOptions struct {
	Durable    *bool
	AutoDelete *bool
}

type ExchangeOption struct {
	Name    string
	Type    string
	Options ExchangeDeclarationOptions
}

type PublishMessage struct {
	Exchange    string
	RoutingKey  string
	Message     []byte
	ContentType string
	Headers     map[string]any
}

type Publisher struct {
	client           *Client
	ch               *amqp.Channel
	wg               *sync.WaitGroup
	confirmCh        chan amqp.Confirmation
	publishConfirms  bool
	notifyChanClose  chan *amqp.Error
	config           []ExchangeOption
	reconnectAttempt int

	isConnected    bool
	isReconnecting bool
}

func (c *Client) NewPublisher(config []ExchangeOption) *Publisher {
	if c.publisherCh != nil {
		fmt.Println("Publisher already initialized, please use that one")
		return c.publisherCh
	}

	newPub := &Publisher{
		config:          config,
		client:          c,
		publishConfirms: true,
		wg:              &sync.WaitGroup{},
	}
	newPub.connect()

	c.publisherCh = newPub
	return newPub
}

func (p *Publisher) connect() {
	ch, err := p.client.conn.Channel()
	failOnError(err, "could not create Publish channel")

	p.notifyChanClose = make(chan *amqp.Error)
	ch.NotifyClose(p.notifyChanClose)
	go p.monitorChannel()

	if p.publishConfirms {
		if err := ch.Confirm(false); err != nil {
			failOnError(err, "failed to enable publish confirm mode")
		}

		p.confirmCh = ch.NotifyPublish(make(chan amqp.Confirmation, 100))
		for _, e := range p.config {
			if e.Options.Durable == nil {
				x := true
				e.Options.Durable = &x
			}

			if e.Options.AutoDelete == nil {
				x := false
				e.Options.AutoDelete = &x
			}

			err := ch.ExchangeDeclare(e.Name,
				e.Type,
				*e.Options.Durable,
				*e.Options.AutoDelete,
				false,
				false,
				nil)
			failOnError(err, "could not declare exchange")
		}
	}

	p.wg.Add(1)
	p.ch = ch
	p.isConnected = true
}

func (p *Publisher) Disconnect() error {
	if p.ch.IsClosed() {
		p.wg.Done()
		return nil
	}

	fmt.Println("Closing publisher channel")
	// TODO: Verificar se tem alguma mensagem presa em buffer antes de fechar
	if err := p.ch.Close(); err != nil {
		return fmt.Errorf("error closing publishing channel: %w", err)
	}

	p.wg.Done()
	return nil
}

func (p *Publisher) reconnect() {
	if p.reconnectAttempt >= 5 {
		log.Fatalf("Maximum reconnection attempts reached")
	}

	p.isReconnecting = true
	p.reconnectAttempt++
	reconnSleep := (1 * time.Second) * time.Duration(p.reconnectAttempt)
	fmt.Printf("attempt reconnecting publish in %.0fs: %d of 5 attempts.\n", time.Duration(reconnSleep).Seconds(), p.reconnectAttempt)
	time.Sleep(reconnSleep)

	if p.ch != nil {
		err := p.ch.Close()
		fmt.Println(err)
	}

	p.connect()
}

func (p *Publisher) Publish(msg PublishMessage) error {
	if p.client.isBlocked || p.client.isReconnecting {
		i := 0
		for p.client.isBlocked || p.client.isReconnecting {
			if i >= 5 {
				return fmt.Errorf("connection is still blocked, could not publish")
			}
			time.Sleep(5 * time.Second)
			i++
		}
	} else if p.client.isClosing {
		return errConnClosed
	}

	if p.publishConfirms {
		return p.publishWithConfirmation(msg)
	} else {
		return p.publishWithoutConfirmation(msg)
	}
}

func (p Publisher) publishWithConfirmation(msg PublishMessage) error {
	if msg.ContentType == "" {
		msg.ContentType = "application/json"
	}

	if _, err := p.ch.PublishWithDeferredConfirm(msg.Exchange,
		msg.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: msg.ContentType,
			Body:        msg.Message,
			Headers: mergeTable(amqp.Table{
				"x-original-exchange":    msg.Exchange,
				"x-original-routing-key": msg.RoutingKey,
				"x-published-at":         time.Now().String(),
			}, msg.Headers),
		}); err != nil {
		return err
	}

	for {
		confirm := <-p.confirmCh
		if confirm.Ack {
			return nil
		} else {
			return fmt.Errorf("nack for delivery tag %d", confirm.DeliveryTag)
		}
	}
}

func (p Publisher) publishWithoutConfirmation(msg PublishMessage) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := p.ch.PublishWithContext(ctx, msg.Exchange, msg.RoutingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         msg.Message,
		DeliveryMode: amqp.Persistent,
		Headers: mergeTable(amqp.Table{
			"x-original-exchange":    msg.Exchange,
			"x-original-routing-key": msg.RoutingKey,
			"x-published-at":         time.Now().String(),
		}, msg.Headers),
	})

	return err
}

func (p *Publisher) monitorChannel() {
	for err := range p.notifyChanClose {
		if err != nil {
			fmt.Printf("publish channel closed: %s || recoverable: %t\n", err.Reason, err.Recover)

			if !err.Recover {
				log.Fatalf("Shutting down publisher channel permanently")
			}
		}

		p.reconnect()
	}
}
