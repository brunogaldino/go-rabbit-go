package client

import (
	"context"
	"encoding/json"
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
	ExcangeFanout  = "fanout"
	ExchangeHeader = "header"
)

var errConnClosed = errors.New("RabbitMQ connection is closed")

type ExchangeDeclarionOptions struct {
	Durable    *bool
	AutoDelete *bool
}

type ExchangeOption struct {
	Name    string
	Type    string
	Options ExchangeDeclarionOptions
}

type PublishMessage struct {
	Exchange   string
	RoutingKey string
	Message    any
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

type RabbitMQConfigurations struct{}

func (c *Client) NewPublisher(config []ExchangeOption) *Publisher {
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
			failOnError(err, "123 could not declare exchange")
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

func (p *Publisher) Publish(message []PublishMessage) error {
	if p.client.isBlocked || p.client.isReconnecting {
		i := 0
		for !p.client.isBlocked || !p.client.isReconnecting {
			if i >= 5 {
				return fmt.Errorf("connection is still blocked, could not publish")
			}
			fmt.Printf("Connection is blocked or is reconnecting, waiting befor publishing")
			time.Sleep(5 * time.Second)
			i++
		}
	} else if p.client.isClosing {
		return errConnClosed
	}

	if p.publishConfirms {
		p.publishWithConfirmation(message)
	} else {
		p.publishWithoutConfimation(message)
	}

	return nil
}

func (p Publisher) publishWithConfirmation(messages []PublishMessage) {
	for _, v := range messages {
		// TODO: Deal with error later
		bd, _ := json.Marshal(v.Message)
		p.ch.PublishWithDeferredConfirm(v.Exchange,
			v.RoutingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(bd),
			})
	}

	for range len(messages) {
		confirm := <-p.confirmCh
		if confirm.Ack {
			fmt.Printf("Confirmed delivery tag %d\n", confirm.DeliveryTag)
		} else {
			fmt.Printf("Nack for delivery tag %d\n", confirm.DeliveryTag)
		}
	}
}

func (p Publisher) publishWithoutConfimation(messages []PublishMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, v := range messages {
		bd, _ := json.Marshal(v.Message)
		err := p.ch.PublishWithContext(ctx, v.Exchange, v.RoutingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			Body:         []byte(bd),
			DeliveryMode: amqp.Persistent,
		})
		failOnError(err, "could not publish message")
		log.Println("Sent message")
	}
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
