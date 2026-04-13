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
	conn *amqp.Connection
	ctx  context.Context
	wg   *sync.WaitGroup

	notifyBlock      chan amqp.Blocking
	notifyError      chan *amqp.Error
	reconnectAttempt int

	isConnected    atomic.Bool
	isBlocked      atomic.Bool
	isReconnecting atomic.Bool
	isClosing      atomic.Bool

	mu          sync.Mutex
	publisherCh *Publisher
	consumerMap map[string]*Consumer
	hostname    string
}

type HealthStatus struct {
	Connected    bool
	Blocked      bool
	Reconnecting bool
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

func (c *Client) Connect() error {
	cfg := amqp.Config{
		Heartbeat: c.conf.Heartbeat,
		Properties: amqp.Table{
			"connection_name": c.hostname,
		},
	}

	conn, err := amqp.DialConfig(c.conf.URI, cfg)
	if err != nil {
		fmt.Printf("could not connect to RabbitMQ broker: %v\n", err)
		return err
	}
	c.wg.Add(1)

	c.conn = conn
	c.notifyBlock = make(chan amqp.Blocking, 1)
	c.notifyError = make(chan *amqp.Error, 1)
	c.isConnected.Store(true)
	c.isReconnecting.Store(false)

	c.conn.NotifyClose(c.notifyError)
	c.conn.NotifyBlocked(c.notifyBlock)

	go c.monitorConnection()

	return nil
}

func (c *Client) reconnect() {
	if c.isClosing.Load() {
		return
	}

	if c.reconnectAttempt >= c.conf.MaxReconnectAttempts {
		fmt.Println("Maximum reconnection attempts reached")
		return
	}

	c.isReconnecting.Store(true)
	c.reconnectAttempt++
	// TODO: Make a pushback to increase reconnection time
	time.Sleep(5 * time.Second)

	if c.conn != nil {
		c.conn.Close()
	}

	if err := c.Connect(); err == nil {
		c.reconnectAttempt = 0
		fmt.Println("sucessfully reconnected")
		return
	}
}

func (c *Client) monitorConnection() {
	for {
		select {
		case blocking := <-c.notifyBlock:
			c.isBlocked.Store(blocking.Active)

			if blocking.Active {
				fmt.Printf("RabbitMQ connection is BLOCKED, reason: %s\n", blocking.Reason)
			} else {
				fmt.Println("RabbitMQ connection is UNBLOCKED")
			}
		case err := <-c.notifyError:
			if err != nil {
				fmt.Printf("Connection closed: %v || isRecoverable: %t \n", err, err.Recover)

				if !err.Recover {
					c.isClosing.Store(true)
					c.isConnected.Store(false)
					fmt.Println("Shutting down main connection permanently")
					return
				}
			}

			fmt.Println("Attempting reconnecting main connection")
			c.isConnected.Store(false)
			c.reconnect()
		case <-c.ctx.Done():
			fmt.Println("gracefully shutting down all channels and connection via context")
			c.Disconnect()
			c.wg.Done()
			return
		}
	}
}

func (c *Client) Disconnect() {
	if c.conn.IsClosed() || c.isClosing.Load() {
		fmt.Println("alreay disconnected", c.conn.IsClosed(), c.isClosing.Load())
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
	c.conn.Close()
	c.isConnected.Store(false)
}

func (c *Client) CheckHealth() HealthStatus {
	return HealthStatus{
		Connected:    c.isConnected.Load(),
		Blocked:      c.isBlocked.Load(),
		Reconnecting: c.isReconnecting.Load(),
	}
}
