package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	URI                  string
	Heartbeat            time.Duration
	MaxReconnectAttempts int
}

type Client struct {
	conf Config
	ctx  context.Context
	wg   *sync.WaitGroup

	publisherConn *amqp.Connection
	consumerConn  *amqp.Connection

	pubNotifyBlock chan amqp.Blocking
	pubNotifyError chan *amqp.Error
	conNotifyError chan *amqp.Error

	pubReconnectAttempt int
	conReconnectAttempt int

	isPubConnected    atomic.Bool
	isConConnected    atomic.Bool
	isBlocked         atomic.Bool
	isPubReconnecting atomic.Bool
	isConReconnecting atomic.Bool
	isClosing         atomic.Bool

	mu          sync.Mutex
	publisherCh *Publisher
	consumerMap map[string]*Consumer
	hostname    string
}

type HealthStatus struct {
	Connected          bool
	PublisherConnected bool
	ConsumerConnected  bool
	Blocked            bool
	Reconnecting       bool
}

func New(ctx context.Context, config Config) (*Client, *sync.WaitGroup) {
	host, _ := os.Hostname()

	var wg sync.WaitGroup
	return &Client{
		conf:        config,
		ctx:         ctx,
		wg:          &wg,
		consumerMap: map[string]*Consumer{},
		hostname:    host,
	}, &wg
}

func (c *Client) dial(suffix string) (*amqp.Connection, error) {
	cfg := amqp.Config{
		Heartbeat: c.conf.Heartbeat,
		Properties: amqp.Table{
			"connection_name": fmt.Sprintf("%s-%s", c.hostname, suffix),
		},
	}

	conn, err := amqp.DialConfig(c.conf.URI, cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to RabbitMQ broker (%s): %w", suffix, err)
	}
	return conn, nil
}

func (c *Client) connectPublisher() error {
	conn, err := c.dial("publisher")
	if err != nil {
		return err
	}

	c.publisherConn = conn
	c.pubNotifyBlock = make(chan amqp.Blocking, 1)
	c.pubNotifyError = make(chan *amqp.Error, 1)
	c.publisherConn.NotifyClose(c.pubNotifyError)
	c.publisherConn.NotifyBlocked(c.pubNotifyBlock)
	c.isPubConnected.Store(true)
	c.isPubReconnecting.Store(false)
	return nil
}

func (c *Client) connectConsumer() error {
	conn, err := c.dial("consumer")
	if err != nil {
		return err
	}

	c.consumerConn = conn
	c.conNotifyError = make(chan *amqp.Error, 1)
	c.consumerConn.NotifyClose(c.conNotifyError)
	c.isConConnected.Store(true)
	c.isConReconnecting.Store(false)
	return nil
}

func (c *Client) Connect() error {
	if err := c.connectPublisher(); err != nil {
		return err
	}
	if err := c.connectConsumer(); err != nil {
		c.publisherConn.Close()
		return err
	}

	c.wg.Add(2)
	go c.monitorPublisherConn()
	go c.monitorConsumerConn()

	return nil
}

func (c *Client) reconnectPublisher() {
	if c.isClosing.Load() {
		return
	}
	if c.pubReconnectAttempt >= c.conf.MaxReconnectAttempts {
		fmt.Println("Maximum publisher reconnection attempts reached")
		return
	}

	c.isPubReconnecting.Store(true)
	c.pubReconnectAttempt++
	// TODO: exponential backoff with jitter
	time.Sleep(5 * time.Second)

	if c.publisherConn != nil && !c.publisherConn.IsClosed() {
		c.publisherConn.Close()
	}

	if err := c.connectPublisher(); err == nil {
		c.pubReconnectAttempt = 0
		fmt.Println("successfully reconnected publisher connection")

		if c.publisherCh != nil {
			if err := c.publisherCh.connect(); err != nil {
				fmt.Printf("failed to reconnect publisher channel: %v\n", err)
			}
		}
	}
}

func (c *Client) reconnectConsumer() {
	if c.isClosing.Load() {
		return
	}
	if c.conReconnectAttempt >= c.conf.MaxReconnectAttempts {
		fmt.Println("Maximum consumer reconnection attempts reached")
		return
	}

	c.isConReconnecting.Store(true)
	c.conReconnectAttempt++
	// TODO: exponential backoff with jitter
	time.Sleep(5 * time.Second)

	if c.consumerConn != nil && !c.consumerConn.IsClosed() {
		c.consumerConn.Close()
	}

	if err := c.connectConsumer(); err == nil {
		c.conReconnectAttempt = 0
		fmt.Println("successfully reconnected consumer connection")
		// Consumer Begin() loops detect isConConnected and re-setup automatically
	}
}

func (c *Client) monitorPublisherConn() {
	defer c.wg.Done()

	for {
		select {
		case blocking := <-c.pubNotifyBlock:
			c.isBlocked.Store(blocking.Active)

			if blocking.Active {
				fmt.Printf("RabbitMQ publisher connection is BLOCKED, reason: %s\n", blocking.Reason)
			} else {
				fmt.Println("RabbitMQ publisher connection is UNBLOCKED")
			}
		case err := <-c.pubNotifyError:
			if err != nil {
				fmt.Printf("Publisher connection closed: %v || isRecoverable: %t\n", err, err.Recover)

				if !err.Recover {
					c.isPubConnected.Store(false)
					fmt.Println("Shutting down publisher connection permanently")
					return
				}
			}

			fmt.Println("Attempting to reconnect publisher connection")
			c.isPubConnected.Store(false)
			c.reconnectPublisher()
			if c.isClosing.Load() {
				return
			}
		case <-c.ctx.Done():
			fmt.Println("gracefully shutting down connections via context")
			c.Disconnect()
			return
		}
	}
}

func (c *Client) monitorConsumerConn() {
	defer c.wg.Done()

	for {
		select {
		case err := <-c.conNotifyError:
			if err != nil {
				fmt.Printf("Consumer connection closed: %v || isRecoverable: %t\n", err, err.Recover)

				if !err.Recover {
					c.isConConnected.Store(false)
					fmt.Println("Shutting down consumer connection permanently")
					return
				}
			}

			fmt.Println("Attempting to reconnect consumer connection")
			c.isConConnected.Store(false)
			c.reconnectConsumer()
			if c.isClosing.Load() {
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Client) Disconnect() {
	if c.isClosing.Load() {
		return
	}
	c.isClosing.Store(true)

	wg := sync.WaitGroup{}
	if c.publisherCh != nil {
		c.publisherCh.Disconnect()
	}

	c.mu.Lock()
	consumers := make([]*Consumer, 0, len(c.consumerMap))
	for _, consumer := range c.consumerMap {
		consumers = append(consumers, consumer)
	}
	c.mu.Unlock()

	if len(consumers) > 0 {
		fmt.Printf("terminating all consumers: %d\n", len(consumers))
		for _, consumer := range consumers {
			wg.Go(func() {
				consumer.Disconnect()
			})
		}
	}

	wg.Wait()

	if c.publisherConn != nil && !c.publisherConn.IsClosed() {
		c.publisherConn.Close()
	}
	if c.consumerConn != nil && !c.consumerConn.IsClosed() {
		c.consumerConn.Close()
	}

	c.isPubConnected.Store(false)
	c.isConConnected.Store(false)
}

func (c *Client) CheckHealth() HealthStatus {
	pubConn := c.isPubConnected.Load()
	conConn := c.isConConnected.Load()
	return HealthStatus{
		Connected:          pubConn && conConn,
		PublisherConnected: pubConn,
		ConsumerConnected:  conConn,
		Blocked:            c.isBlocked.Load(),
		Reconnecting:       c.isPubReconnecting.Load() || c.isConReconnecting.Load(),
	}
}
