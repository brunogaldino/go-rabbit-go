// Package client provides a dual-connection AMQP client with automatic
// reconnection and lifecycle management. Each [Client] manages up to two
// independent connections (one for publishing, one for consuming) so
// that broker flow-control only affects the publisher side.
//
// Connections are established lazily: each role's connection is dialed
// only when the first channel for that role is requested — that is, when
// a publisher or consumer is created. Applications that only publish (or
// only consume) therefore open a single connection.
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
	// LogType controls which inspection logs (consumer/publisher/all/none)
	// are emitted. The env var GORABBIT_LOG_TYPE overrides this value.
	LogType rabbitmq.LogType
}

// Client manages up to two independent AMQP connections (one for
// publishing, one for consuming) and coordinates their lifecycle,
// reconnection, and graceful shutdown. Each connection is dialed lazily
// on the first channel request for its role.
type Client struct {
	conf    Config
	ctx     context.Context
	wg      *sync.WaitGroup
	dialer  amqpx.Dialer
	logger  rabbitmq.Logger
	logType rabbitmq.LogType

	pub            conn.Managed
	con            conn.Managed
	pubNotifyBlock chan amqp.Blocking

	isBlocked atomic.Bool
	isClosing atomic.Bool

	// Lazy dial state, one mutex per role. After the initial successful
	// dial the role's monitor goroutine owns reconnection; the *MonitorStarted
	// flags (guarded by their mutex) ensure each monitor starts once.
	pubMu             sync.Mutex
	conMu             sync.Mutex
	pubMonitorStarted bool
	conMonitorStarted bool
	pubRequested      atomic.Bool
	conRequested      atomic.Bool

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

	logType := config.LogType
	if envVal := os.Getenv("GORABBIT_LOG_TYPE"); envVal != "" {
		logType = rabbitmq.LogType(envVal)
	}
	if logType == "" {
		logType = rabbitmq.LogTypeNone
	}

	var wg sync.WaitGroup
	return &Client{
		conf:        config,
		ctx:         ctx,
		wg:          &wg,
		dialer:      dialer,
		logger:      logger,
		logType:     logType,
		consumerMap: map[string]*consumer.Consumer{},
		hostname:    host,
	}, &wg
}

// --- consumer.ConnProvider implementation ---

// Channel lazily establishes the consumer connection on first use and
// opens a new AMQP channel on it.
func (c *Client) Channel() (amqpx.AMQPChannel, error) {
	if err := c.ensureConsumerConn(); err != nil {
		return nil, err
	}

	return c.con.Conn.Channel()
}

func (c *Client) Connected() bool             { return c.con.IsConnected.Load() }
func (c *Client) Closing() bool               { return c.isClosing.Load() }
func (c *Client) Host() string                { return c.hostname }
func (c *Client) Logger() rabbitmq.Logger     { return c.logger }
func (c *Client) LogType() rabbitmq.LogType   { return c.logType }

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
	if err := a.c.ensurePublisherConn(); err != nil {
		return nil, err
	}

	return a.c.pub.Conn.Channel()
}
func (a *publisherConn) Blocked() bool             { return a.c.isBlocked.Load() }
func (a *publisherConn) Reconnecting() bool        { return a.c.pub.IsReconnecting.Load() }
func (a *publisherConn) Closing() bool             { return a.c.isClosing.Load() }
func (a *publisherConn) Logger() rabbitmq.Logger   { return a.c.logger }
func (a *publisherConn) LogType() rabbitmq.LogType { return a.c.logType }
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

// Connect validates the client configuration without dialing the broker.
// Connections are established lazily: each role's connection (publisher
// or consumer) is dialed on the first channel request for that role,
// i.e. when a publisher or consumer is created.
//
// Calling Connect is optional. It is kept for early URI validation and
// backwards compatibility with the previous eager-dial behaviour.
func (c *Client) Connect() error {
	if _, err := amqp.ParseURI(c.conf.URI); err != nil {
		return fmt.Errorf("rabbitmq: invalid AMQP URI: %w", err)
	}

	return nil
}

// ensurePublisherConn dials the publisher connection on first use and
// starts its monitor goroutine. After the initial successful dial, the
// monitor owns reconnection and this becomes a no-op. A failed initial
// dial is retried on the next call.
func (c *Client) ensurePublisherConn() error {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	// Checked under the mutex so a dial can never start after
	// Disconnect has begun closing connections or the context was
	// cancelled.
	if c.isClosing.Load() || c.ctx.Err() != nil {
		return rabbitmq.ErrConnectionClosed
	}

	c.pubRequested.Store(true)

	if c.pubMonitorStarted {
		return nil
	}

	if err := c.connectPublisher(); err != nil {
		return err
	}

	c.pubMonitorStarted = true
	c.wg.Go(func() { c.monitorPublisherConn() })

	return nil
}

// ensureConsumerConn dials the consumer connection on first use and
// starts its monitor goroutine. After the initial successful dial, the
// monitor owns reconnection and this becomes a no-op. A failed initial
// dial is retried on the next call.
func (c *Client) ensureConsumerConn() error {
	c.conMu.Lock()
	defer c.conMu.Unlock()

	// Checked under the mutex so a dial can never start after
	// Disconnect has begun closing connections or the context was
	// cancelled.
	if c.isClosing.Load() || c.ctx.Err() != nil {
		return rabbitmq.ErrConnectionClosed
	}

	c.conRequested.Store(true)

	if c.conMonitorStarted {
		return nil
	}

	if err := c.connectConsumer(); err != nil {
		return err
	}

	c.conMonitorStarted = true
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

	if !c.redialPublisher() {
		return
	}

	if c.publisherCh == nil {
		return
	}

	// Called without holding pubMu: Connect() requests a channel, which
	// re-enters ensurePublisherConn.
	if err := c.publisherCh.Connect(); err != nil {
		c.logger.Error(fmt.Sprintf("failed to reconnect publisher channel: %v", err))
	}
}

// redialPublisher closes the dropped publisher connection and dials a
// new one, serialized with lazy dials and Disconnect. It reports
// whether the connection was re-established.
func (c *Client) redialPublisher() bool {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	// Re-checked under the mutex: Disconnect may have run during the
	// backoff sleep, and redialing after it would leak a connection.
	if c.isClosing.Load() {
		c.pub.IsReconnecting.Store(false)
		return false
	}

	c.pub.Close()

	if err := c.connectPublisher(); err != nil {
		return false
	}

	c.pub.ReconnectAttempt = 0
	c.logger.Info("successfully reconnected publisher connection")

	return true
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

	c.conMu.Lock()
	defer c.conMu.Unlock()

	// Re-checked under the mutex: Disconnect may have run during the
	// backoff sleep, and redialing after it would leak a connection.
	if c.isClosing.Load() {
		c.con.IsReconnecting.Store(false)
		return
	}

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
				c.logger.Info(fmt.Sprintf("publisher connection BLOCKED, reason: %s", blocking.Reason))
				continue
			}
			c.logger.Info("publisher connection UNBLOCKED")

		case err := <-c.pub.NotifyError:
			if err != nil && !err.Recover {
				c.pub.MarkDisconnected()
				c.logger.Error(fmt.Sprintf("shutting down publisher connection permanently: %v", err))
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
				c.logger.Error(fmt.Sprintf("shutting down consumer connection permanently: %v", err))
				return
			}

			c.logger.Info("attempting to reconnect consumer connection")
			c.con.MarkDisconnected()
			c.reconnectConsumer()
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

// Disconnect gracefully shuts down the publisher, all consumers, and
// any established AMQP connections. It is safe to call multiple times
// and from concurrent goroutines.
func (c *Client) Disconnect() {
	if !c.isClosing.CompareAndSwap(false, true) {
		return
	}

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
		c.logger.Info(fmt.Sprintf("terminating all consumers: %d", len(consumers)))
		var wg sync.WaitGroup
		for _, cons := range consumers {
			wg.Go(func() {
				cons.Disconnect()
			})
		}
		wg.Wait()
	}

	// Serialize with in-flight lazy dials and monitor redials: once the
	// role mutexes are held, no new connection can appear, so whatever
	// exists now is everything there is to close.
	c.pubMu.Lock()
	c.conMu.Lock()
	defer c.conMu.Unlock()
	defer c.pubMu.Unlock()

	c.pub.Close()
	c.con.Close()
	c.pub.MarkDisconnected()
	c.con.MarkDisconnected()
}

// CheckHealth returns a snapshot of the client's connection status.
// Connected reflects only the roles that have been requested: a role
// whose connection was never needed (no publisher or no consumer
// created) does not degrade the overall status. A client that has been
// disconnected always reports Connected as false.
func (c *Client) CheckHealth() HealthStatus {
	pubConn := c.pub.IsConnected.Load()
	conConn := c.con.IsConnected.Load()

	pubOK := !c.pubRequested.Load() || pubConn
	conOK := !c.conRequested.Load() || conConn

	return HealthStatus{
		Connected:          !c.isClosing.Load() && pubOK && conOK,
		PublisherConnected: pubConn,
		ConsumerConnected:  conConn,
		Blocked:            c.isBlocked.Load(),
		Reconnecting:       c.pub.IsReconnecting.Load() || c.con.IsReconnecting.Load(),
	}
}
