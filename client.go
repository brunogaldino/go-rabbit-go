package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
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

	isConnected    bool
	isBlocked      bool
	isReconnecting bool
	isClosing      bool

	publisherCh *Publisher
	consumerMap []*Consumer
}

func New(ctx context.Context, config Config) (*Client, *sync.WaitGroup) {
	var wg sync.WaitGroup
	return &Client{
		conf: config,
		ctx:  ctx,
		wg:   &wg,
	}, &wg
}

func (c *Client) Connect() error {
	host, _ := os.Hostname()
	cfg := amqp.Config{
		Heartbeat: c.conf.Heartbeat,
		Properties: amqp.Table{
			"connection_name": host,
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

	c.conn.NotifyClose(c.notifyError)
	c.conn.NotifyBlocked(c.notifyBlock)

	go c.monitorConnection()

	return nil
}

func (c *Client) reconnect() {
	if c.isClosing {
		return
	}

	if c.reconnectAttempt >= c.conf.MaxReconnectAttempts {
		fmt.Println("Maximum reconnection attempts reached")
		return
	}

	c.isReconnecting = true
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
			c.isBlocked = blocking.Active

			if blocking.Active {
				fmt.Printf("RabbitMQ connection is BLOCKED, reason: %s\n", blocking.Reason)
			} else {
				fmt.Println("RabbitMQ connection is UNBLOCKED")
			}
		case err := <-c.notifyError:
			if err != nil {
				fmt.Printf("Connection closed: %v || isRecoverable: %t \n", err, err.Recover)

				if !err.Recover {
					c.isClosing = true
					c.isConnected = false
					log.Fatalf("Shutting down main connection permanently\n")
				}
			}

			fmt.Println("Attempting reconnecting main connection")
			c.isConnected = false
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
	if c.conn.IsClosed() || c.isClosing {
		fmt.Println("alreay disconnected", c.conn.IsClosed(), c.isClosing)
		return
	}
	c.isClosing = true

	wg := sync.WaitGroup{}
	if c.publisherCh != nil {
		c.publisherCh.Disconnect()
	}

	if len(c.consumerMap) > 0 {
		fmt.Printf("terminating all consumers: %d\n", len(c.consumerMap))
		for _, consumer := range c.consumerMap {
			wg.Go(func() {
				consumer.Disconnect()
			})
		}
	}

	wg.Wait()
	c.conn.Close()
	c.isConnected = false
}

func failOnError(err error, title string) {
	if err != nil {
		log.Fatalf("%s: %v", title, err)
	}
}
