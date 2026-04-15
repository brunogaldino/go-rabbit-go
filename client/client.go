// Package client provides a dual-connection AMQP client with automatic
// reconnection and lifecycle management. Each [Client] maintains two
// independent connections (one for publishing, one for consuming) so
// that broker flow-control only affects the publisher side.
//
// A [Client] implements both [consumer.ConnProvider] and
// [publisher.ConnProvider], allowing consumers and publishers from
// their respective packages to be attached.
package client

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
	"github.com/brunogaldino/go-rabbit-go/consumer"
	"github.com/brunogaldino/go-rabbit-go/internal/conn"
	"github.com/brunogaldino/go-rabbit-go/publisher"
)

const reconnectDelay = 5 * time.Second

// Config holds the connection parameters for a [Client].
type Config struct {
	// URI is the AMQP connection string (e.g. "amqp://guest:guest@localhost:5672/").
	URI string
	// Heartbeat is the interval between AMQP heartbeat frames.
	Heartbeat time.Duration
	// MaxReconnectAttempts limits how many times the client will try to
	// re-establish a dropped connection before giving up.
	MaxReconnectAttempts int
	// Logger overrides the default logger. When nil, a default logger
	// that writes to stdout is used.
	Logger rabbitmq.Logger
	// Dialer overrides the AMQP dial function. Used for testing.
	// When nil, the default amqp091-go dialer is used.
	Dialer amqpx.Dialer
}

// Client manages two independent AMQP connections (one for publishing,
// one for consuming) and coordinates their lifecycle, reconnection, and
// graceful shutdown.
type Client struct {
	conf   Config
	ctx    context.Context
	wg     *sync.WaitGroup
	dialer amqpx.Dialer
	logger rabbitmq.Logger

	pub            conn.Managed
	con            conn.Managed
	pubNotifyBlock chan amqp.Blocking

	isBlocked atomic.Bool
	isClosing atomic.Bool

	mu          sync.Mutex
	publisherCh *publisher.Publisher
	consumerMap map[string]*consumer.Consumer
	hostname    string
}

// HealthStatus reports the state of a [Client]'s connections.
type HealthStatus struct {
	Connected          bool
	PublisherConnected bool
	ConsumerConnected  bool
	Blocked            bool
	Reconnecting       bool
}

// New creates a new [Client] and returns a [sync.WaitGroup] that callers
// can use to block until the client finishes shutting down.
func New(ctx context.Context, config Config) (*Client, *sync.WaitGroup) {
	host, _ := os.Hostname()

	logger := config.Logger
	if logger == nil {
		logger = rabbitmq.NewDefaultLogger()
	}

	dialer := config.Dialer
	if dialer == nil {
		dialer = &amqpx.DefaultDialer{}
	}

	var wg sync.WaitGroup
	return &Client{
		conf:        config,
		ctx:         ctx,
		wg:          &wg,
		dialer:      dialer,
		logger:      logger,
		consumerMap: map[string]*consumer.Consumer{},
		hostname:    host,
	}, &wg
}

// --- consumer.ConnProvider implementation ---

func (c *Client) Channel() (amqpx.AMQPChannel, error) {
	return c.con.Conn.Channel()
}

func (c *Client) Connected() bool         { return c.con.IsConnected.Load() }
func (c *Client) Closing() bool           { return c.isClosing.Load() }
func (c *Client) Host() string            { return c.hostname }
func (c *Client) Logger() rabbitmq.Logger { return c.logger }

func (c *Client) RegisterConsumer(name string, cons *consumer.Consumer) {
	c.mu.Lock()
	c.consumerMap[name] = cons
	c.mu.Unlock()
}

func (c *Client) UnregisterConsumer(name string) {
	c.mu.Lock()
	delete(c.consumerMap, name)
	c.mu.Unlock()
}

// --- publisher.ConnProvider adapter ---
// publisherConn adapts Client to satisfy publisher.ConnProvider,
// routing Channel() to the publisher connection instead of the consumer one.
type publisherConn struct{ c *Client }

func (a *publisherConn) Channel() (amqpx.AMQPChannel, error) {
	return a.c.pub.Conn.Channel()
}
func (a *publisherConn) Blocked() bool           { return a.c.isBlocked.Load() }
func (a *publisherConn) Reconnecting() bool      { return a.c.pub.IsReconnecting.Load() }
func (a *publisherConn) Closing() bool           { return a.c.isClosing.Load() }
func (a *publisherConn) Logger() rabbitmq.Logger { return a.c.logger }
func (a *publisherConn) SetPublisher(p *publisher.Publisher) {
	a.c.mu.Lock()
	a.c.publisherCh = p
	a.c.mu.Unlock()
}

// PublisherConn returns a [publisher.ConnProvider] backed by the client's
// publisher connection. Pass it to [publisher.New].
func (c *Client) PublisherConn() publisher.ConnProvider {
	return &publisherConn{c: c}
}

// --- Connection lifecycle ---

func (c *Client) dial(suffix string) (amqpx.AMQPConnection, error) {
	cfg := amqp.Config{
		Heartbeat: c.conf.Heartbeat,
		Properties: amqp.Table{
			amqpx.KeyConnectionName: fmt.Sprintf("%s-%s", c.hostname, suffix),
		},
	}

	conn, err := c.dialer.Dial(c.conf.URI, cfg)
	if err != nil {
		return nil, &DialError{Role: suffix, Err: err}
	}

	return conn, nil
}

func (c *Client) connectPublisher() error {
	cn, err := c.dial(amqpx.SuffixPublisher)
	if err != nil {
		return err
	}

	c.pub.Conn = cn
	c.pubNotifyBlock = make(chan amqp.Blocking, 1)
	c.pub.NotifyError = make(chan *amqp.Error, 1)
	c.pub.Conn.NotifyClose(c.pub.NotifyError)
	c.pub.Conn.NotifyBlocked(c.pubNotifyBlock)
	c.pub.MarkConnected()

	return nil
}

func (c *Client) connectConsumer() error {
	cn, err := c.dial(amqpx.SuffixConsumer)
	if err != nil {
		return err
	}

	c.con.Conn = cn
	c.con.NotifyError = make(chan *amqp.Error, 1)
	c.con.Conn.NotifyClose(c.con.NotifyError)
	c.con.MarkConnected()

	return nil
}

// Connect establishes both the publisher and consumer connections to the
// broker and starts their monitor goroutines.
func (c *Client) Connect() error {
	if err := c.connectPublisher(); err != nil {
		return err
	}

	if err := c.connectConsumer(); err != nil {
		c.pub.Close()
		return err
	}

	c.wg.Go(func() { c.monitorPublisherConn() })
	c.wg.Go(func() { c.monitorConsumerConn() })

	return nil
}

func (c *Client) reconnectPublisher() {
	if c.isClosing.Load() {
		return
	}

	if c.pub.ReconnectAttempt >= c.conf.MaxReconnectAttempts {
		c.logger.Error("maximum publisher reconnection attempts reached")
		return
	}

	c.pub.IsReconnecting.Store(true)
	c.pub.ReconnectAttempt++
	// TODO: exponential backoff with jitter
	time.Sleep(reconnectDelay)
	c.pub.Close()

	if err := c.connectPublisher(); err != nil {
		return
	}

	c.pub.ReconnectAttempt = 0
	c.logger.Info("successfully reconnected publisher connection")

	if c.publisherCh == nil {
		return
	}

	if err := c.publisherCh.Connect(); err != nil {
		c.logger.Error("failed to reconnect publisher channel: %v", err)
	}
}

func (c *Client) reconnectConsumer() {
	if c.isClosing.Load() {
		return
	}

	if c.con.ReconnectAttempt >= c.conf.MaxReconnectAttempts {
		c.logger.Error("maximum consumer reconnection attempts reached")
		return
	}

	c.con.IsReconnecting.Store(true)
	c.con.ReconnectAttempt++

	// TODO: exponential backoff with jitter
	time.Sleep(reconnectDelay)
	c.con.Close()

	if err := c.connectConsumer(); err != nil {
		return
	}

	c.con.ReconnectAttempt = 0
	c.logger.Info("successfully reconnected consumer connection")
	// Consumer Begin() loops detect Connected() and re-setup automatically
}

func (c *Client) monitorPublisherConn() {
	for {
		select {
		case blocking := <-c.pubNotifyBlock:
			c.isBlocked.Store(blocking.Active)
			if blocking.Active {
				c.logger.Info("publisher connection BLOCKED, reason: %s", blocking.Reason)
				continue
			}
			c.logger.Info("publisher connection UNBLOCKED")

		case err := <-c.pub.NotifyError:
			if err != nil && !err.Recover {
				c.pub.MarkDisconnected()
				c.logger.Error("shutting down publisher connection permanently: %v", err)
				return
			}

			c.logger.Info("attempting to reconnect publisher connection")
			c.pub.MarkDisconnected()
			c.reconnectPublisher()
			if c.isClosing.Load() {
				return
			}

		case <-c.ctx.Done():
			c.logger.Info("gracefully shutting down connections via context")
			c.Disconnect()
			return
		}
	}
}

func (c *Client) monitorConsumerConn() {
	for {
		select {
		case err := <-c.con.NotifyError:
			if err != nil && !err.Recover {
				c.con.MarkDisconnected()
				c.logger.Error("shutting down consumer connection permanently: %v", err)
				return
			}

			c.logger.Info("attempting to reconnect consumer connection")
			c.con.MarkDisconnected()
			c.reconnectConsumer()
			if c.isClosing.Load() {
				return
			}

		case <-c.ctx.Done():
			return
		}
	}
}

// Disconnect gracefully shuts down the publisher, all consumers, and
// both AMQP connections. It is safe to call multiple times.
func (c *Client) Disconnect() {
	if c.isClosing.Load() {
		return
	}
	c.isClosing.Store(true)

	if c.publisherCh != nil {
		c.publisherCh.Disconnect()
	}

	c.mu.Lock()
	consumers := make([]*consumer.Consumer, 0, len(c.consumerMap))
	for _, cons := range c.consumerMap {
		consumers = append(consumers, cons)
	}
	c.mu.Unlock()

	if len(consumers) > 0 {
		c.logger.Info("terminating all consumers: %d", len(consumers))
		var wg sync.WaitGroup
		for _, cons := range consumers {
			wg.Go(func() {
				cons.Disconnect()
			})
		}
		wg.Wait()
	}

	c.pub.Close()
	c.con.Close()
	c.pub.MarkDisconnected()
	c.con.MarkDisconnected()
}

// CheckHealth returns a snapshot of the client's connection status.
func (c *Client) CheckHealth() HealthStatus {
	pubConn := c.pub.IsConnected.Load()
	conConn := c.con.IsConnected.Load()

	return HealthStatus{
		Connected:          pubConn && conConn,
		PublisherConnected: pubConn,
		ConsumerConnected:  conConn,
		Blocked:            c.isBlocked.Load(),
		Reconnecting:       c.pub.IsReconnecting.Load() || c.con.IsReconnecting.Load(),
	}
}
