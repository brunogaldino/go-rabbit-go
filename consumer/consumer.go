// Package consumer provides a message consumer for AMQP queues with
// automatic retry and dead letter support.
//
// A [Consumer] is created via [New] with a [ConnProvider] (typically a
// [github.com/brunogaldino/go-rabbit-go/client.Client]) and functional
// options. Call [Consumer.Begin] in a goroutine — it blocks and
// continuously processes messages, automatically reconnecting on
// channel or connection drops.
package consumer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
	irand "github.com/brunogaldino/go-rabbit-go/internal/rand"
)

// Consumer timing and limit defaults.
const (
	pollInterval        = 500 * time.Millisecond
	setupRetryDelay     = 1 * time.Second
	defaultPrefetch     = 10
	defaultMaxRetry     = 5
	retryMultiplier     = 1000
	defaultRandomIDLen  = 4
	defaultDrainTimeout = 30 * time.Second
)

// Operation names used in error types.
const (
	opChannelOpen  = "open"
	opChannelQos   = "qos"
	opQueueDeclare = "declare"
	opQueueBind    = "bind"
	opDeclareDLQ   = "declare-dlq"
	opDeclareRetry = "declare-retry"
)

// defaultExchange is the AMQP default (nameless) exchange used for
// retry and dead letter routing.
const defaultExchange = ""

// consumerNameFmt is the format for generating unique consumer names.
const consumerNameFmt = "%s:%s:%s"

// ConnProvider defines what a [Consumer] needs from its connection owner
// (typically a [github.com/brunogaldino/go-rabbit-go/client.Client]).
type ConnProvider interface {
	Channel() (amqpx.AMQPChannel, error)
	Connected() bool
	Closing() bool
	Host() string
	Logger() rabbitmq.Logger
	LogType() rabbitmq.LogType
	RegisterConsumer(name string, c *Consumer)
	UnregisterConsumer(name string)
}

// Handler is the function signature for message processing callbacks.
type Handler func(Delivery, string) error

// Delivery wraps an [amqp.Delivery] exposing only safe, read-only fields.
// Ack/Nack are handled automatically by the consumer — handlers should
// return nil to acknowledge or an error to trigger the retry strategy.
type Delivery struct {
	delivery      amqp.Delivery
	Body          []byte
	Headers       amqp.Table
	RoutingKey    string
	ContentType   string
	CorrelationId string
}

// Raw returns the underlying [amqp.Delivery] for access to fields not
// exposed directly (e.g. AppId, Timestamp, MessageId).
func (d Delivery) Raw() amqp.Delivery {
	return d.delivery
}

// GetHeader returns the value of a header by key, or nil if not present.
func (d Delivery) GetHeader(key string) any {
	if d.Headers == nil {
		return nil
	}

	return d.Headers[key]
}

// GetRoutingKey returns the original routing key from the
// x-original-routing-key header, falling back to the message's
// current routing key if the header is absent.
func (d Delivery) GetRoutingKey() string {
	rk, ok := d.Headers[amqpx.KeyOriginalRouteKey]
	if !ok {
		return d.RoutingKey
	}

	return rk.(string)
}

// Retry configures the retry behaviour for a [Consumer].
type Retry struct {
	Enabled    bool
	MaxAttempt int
	// RetryFunc computes the delay in milliseconds before the next retry.
	RetryFunc func(content Delivery, attempt int32, err error) int32
}

// Deadletter configures the dead letter behaviour for a [Consumer].
type Deadletter struct {
	Enabled     bool
	DLQueueName string
	// DLQFunc is called before sending the message to the DLQ.
	// Return true to send to DLQ (nack), false to acknowledge and drop.
	DLQFunc func(Delivery) bool
}

// Options holds all configuration for a [Consumer].
type Options struct {
	Queue              string
	RoutingKey         []string
	ExchangeName       string
	AutoDelete         bool
	Prefetch           int
	Callback           Handler
	RetryStrategy      *Retry
	DeadletterStrategy *Deadletter
	HeadersBinding     map[string]any
}

var defaults = &Options{
	AutoDelete: false,
	Prefetch:   defaultPrefetch,
	RetryStrategy: &Retry{
		Enabled:    true,
		MaxAttempt: defaultMaxRetry,
		RetryFunc: func(d Delivery, attempt int32, err error) int32 {
			return attempt * retryMultiplier
		},
	},
	DeadletterStrategy: &Deadletter{
		Enabled: true,
		DLQFunc: nil,
	},
}

// Consumer processes messages from a single AMQP queue with automatic
// retry and dead letter support.
type Consumer struct {
	conn           ConnProvider
	channel        amqpx.AMQPChannel
	params         Options
	wg             sync.WaitGroup
	consumerName   string
	closing        atomic.Bool
	disconnectOnce sync.Once
	drainTimeout   time.Duration
}

// --- Functional options ---

// Option is a functional option for configuring a [Consumer].
type Option func(*Consumer)

// WithRoutingKey sets the routing keys used to bind the queue to the exchange.
func WithRoutingKey(rks []string) Option {
	return func(c *Consumer) { c.params.RoutingKey = rks }
}

// WithExchangeName sets the exchange the consumer queue is bound to.
func WithExchangeName(exc string) Option {
	return func(c *Consumer) { c.params.ExchangeName = exc }
}

// WithPrefetch sets the channel-level prefetch count.
func WithPrefetch(pre int) Option {
	return func(c *Consumer) { c.params.Prefetch = pre }
}

// WithAutoDelete marks the queue as auto-delete.
func WithAutoDelete() Option {
	return func(c *Consumer) { c.params.AutoDelete = true }
}

// WithRetryDisabled disables the retry strategy; failed messages go
// directly to the dead letter strategy.
func WithRetryDisabled() Option {
	return func(c *Consumer) { c.params.RetryStrategy.Enabled = false }
}

// WithRetryMaxAttempt sets the maximum number of retry attempts.
func WithRetryMaxAttempt(max int) Option {
	return func(c *Consumer) { c.params.RetryStrategy.MaxAttempt = max }
}

// WithRetryFn provides a custom delay function for the retry strategy.
func WithRetryFn(fn func(d Delivery, attempt int32, err error) int32) Option {
	return func(c *Consumer) { c.params.RetryStrategy.RetryFunc = fn }
}

// WithDLQFn provides a callback that is invoked before sending a
// message to the dead letter queue.
func WithDLQFn(fn func(Delivery) bool) Option {
	return func(c *Consumer) { c.params.DeadletterStrategy.DLQFunc = fn }
}

// WithHeadersBinding sets custom header arguments used when binding the queue
// to the exchange. Use this with exchanges of type "headers".
func WithHeadersBinding(headers map[string]any) Option {
	return func(c *Consumer) { c.params.HeadersBinding = headers }
}

// WithShutdownDrainTimeout sets how long [Consumer.Disconnect] waits for
// in-flight message handlers to finish (and ack) before closing the
// channel. When the timeout elapses the channel is closed anyway and any
// still-unacked deliveries are redelivered by the broker. A non-positive
// duration falls back to the default (30s).
func WithShutdownDrainTimeout(d time.Duration) Option {
	return func(c *Consumer) {
		if d > 0 {
			c.drainTimeout = d
		}
	}
}

// --- Consumer creation ---

// New creates a [Consumer] bound to the given queue. The callback is
// invoked for every delivered message. Use functional options to
// customize routing keys, prefetch, retry, and dead letter behaviour.
// The consumer automatically registers itself with the [ConnProvider]
// for lifecycle management.
func New(conn ConnProvider, queue string, callback Handler, options ...Option) (*Consumer, error) {
	retryCopy := *defaults.RetryStrategy
	dlCopy := *defaults.DeadletterStrategy
	defs := *defaults
	defs.RetryStrategy = &retryCopy
	defs.DeadletterStrategy = &dlCopy

	c := &Consumer{params: defs, conn: conn, drainTimeout: defaultDrainTimeout}
	c.setOptions(queue, callback, options)

	if err := c.setup(); err != nil {
		return nil, err
	}

	conn.RegisterConsumer(c.consumerName, c)

	return c, nil
}

// Name returns the consumer's unique name.
func (c *Consumer) Name() string { return c.consumerName }

// setup creates the AMQP channel and declares all queues and bindings.
// It is called on initial creation and after reconnection.
func (c *Consumer) setup() error {
	ch, err := c.conn.Channel()
	if err != nil {
		return &rabbitmq.ChannelError{Operation: opChannelOpen, Err: err}
	}
	c.channel = ch

	if err = ch.Qos(c.params.Prefetch, 0, false); err != nil {
		return &rabbitmq.ChannelError{Operation: opChannelQos, Err: err}
	}

	queueTable := amqp.Table{
		amqpx.KeyQueueType: amqpx.QueueTypeQuorum,
	}
	queueTable.SetClientConnectionName(c.conn.Host())

	if err := c.declareDLQueue(queueTable); err != nil {
		return err
	}

	if _, err = ch.QueueDeclare(c.params.Queue, true, c.params.AutoDelete, false, false, queueTable); err != nil {
		return &QueueError{
			Operation: opQueueDeclare,
			Queue:     fmt.Sprintf("%s[%s]", c.params.Queue, strings.Join(c.params.RoutingKey, ",")),
			Err:       err,
		}
	}

	if err := c.declareRetryQueue(); err != nil {
		return err
	}

	for _, rk := range c.params.RoutingKey {
		if err = ch.QueueBind(c.params.Queue, rk, c.params.ExchangeName, false, amqp.Table(c.params.HeadersBinding)); err != nil {
			return &QueueError{Operation: opQueueBind, Queue: c.params.Queue, Err: err}
		}
	}

	return nil
}

func (c *Consumer) setOptions(queue string, callback Handler, options []Option) {
	c.params.Queue = queue
	c.params.Callback = callback
	c.params.DeadletterStrategy.DLQueueName = queue + amqpx.SuffixDLQ
	cID, _ := irand.ID(defaultRandomIDLen)
	c.consumerName = fmt.Sprintf(consumerNameFmt, c.conn.Host(), c.params.ExchangeName, cID)

	for _, o := range options {
		o(c)
	}
}

// --- Consumer lifecycle ---

// Begin starts the consumer loop. It blocks and continuously processes
// messages until the consumer is disconnected or the context is
// cancelled. It automatically reconnects on channel or connection drops.
func (c *Consumer) Begin(groups ...string) error {
	for {
		c.conn.Logger().Info(fmt.Sprintf("beginning message consumer %s", c.params.Queue))
		c.consume()
		c.wg.Wait()

		if c.closing.Load() || c.conn.Closing() {
			return nil
		}

		c.conn.Logger().Info(fmt.Sprintf("consumer %s channel closed, waiting for reconnection...", c.params.Queue))

		if !c.waitForReconnect() {
			return nil
		}

		if err := c.setup(); err != nil {
			c.conn.Logger().Error(fmt.Sprintf("failed to reattach consumer %s: %v", c.params.Queue, err))
			time.Sleep(setupRetryDelay)
			continue
		}
	}
}

func (c *Consumer) waitForReconnect() bool {
	for !c.conn.Connected() {
		if c.closing.Load() || c.conn.Closing() {
			return false
		}
		time.Sleep(pollInterval)
	}

	return true
}

func (c *Consumer) consume() {
	msgs, err := c.channel.Consume(c.params.Queue, c.consumerName, false, false, false, false, nil)
	if err != nil {
		c.conn.Logger().Error(fmt.Sprintf("error beginning consumer %s: %v", c.params.Queue, err))
		return
	}

	for msg := range msgs {
		d := Delivery{
			delivery:      msg,
			Body:          msg.Body,
			Headers:       msg.Headers,
			RoutingKey:    msg.RoutingKey,
			ContentType:   msg.ContentType,
			CorrelationId: msg.CorrelationId,
		}
		c.wg.Go(func() {
			c.processDelivery(d)
		})
	}
}

func (c *Consumer) processDelivery(d Delivery) {
	start := time.Now()
	var handlerErr error
	var retried bool

	defer func() {
		if r := recover(); r != nil {
			c.conn.Logger().Error(fmt.Sprintf("recovered from panic in consumer: %v", r))
			handlerErr = fmt.Errorf("panic: %v", r)
		}

		if handlerErr != nil {
			retried = c.retry(d, handlerErr)
		} else {
			if err := d.delivery.Ack(false); err != nil {
				c.conn.Logger().Error(fmt.Sprintf("could not ack message: %v", err))
			}
		}

		elapsed := time.Since(start)
		if c.conn.LogType().Includes(rabbitmq.LogTypeConsumer) || handlerErr != nil {
			c.inspect(d, elapsed, handlerErr, handlerErr != nil && !retried)
		}
	}()

	handlerErr = c.params.Callback(d, c.params.Queue)
}

// Disconnect stops delivering messages, waits for in-flight handlers
// (up to the configured drain timeout), closes the AMQP channel, and
// unregisters the consumer from the client. It is idempotent and safe to
// call multiple times and from concurrent goroutines: the teardown runs
// exactly once and every caller blocks until it completes.
func (c *Consumer) Disconnect() {
	c.disconnectOnce.Do(func() {
		c.closing.Store(true)
		c.conn.Logger().Info(fmt.Sprintf("stopping deliveries to consumer %s", c.consumerName))

		if err := c.channel.Cancel(c.consumerName, false); err != nil {
			c.conn.Logger().Error(fmt.Sprintf("cancel consumer %s: %v", c.consumerName, err))
		}

		if !c.waitDrain() {
			c.conn.Logger().Error(fmt.Sprintf(
				"drain timeout for consumer %s after %s; closing channel, unacked messages will be redelivered",
				c.consumerName, c.drainTimeout))
		}

		if err := c.channel.Close(); err != nil {
			c.conn.Logger().Error(fmt.Sprintf("close consumer channel %s [RK: %s]: %v", c.params.Queue, c.params.RoutingKey, err))
		}

		c.conn.UnregisterConsumer(c.consumerName)
	})
}

// waitDrain blocks until all in-flight handlers finish or the drain
// timeout elapses. It reports whether the drain completed in time.
func (c *Consumer) waitDrain() bool {
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(c.drainTimeout):
		return false
	}
}

// --- Retry and dead letter ---

func (c *Consumer) retry(d Delivery, err error) bool {
	retryCount, ok := d.Headers[amqpx.KeyRetriesCount].(int32)
	if !ok {
		retryCount = 1
	}
	delayAmount := c.params.RetryStrategy.RetryFunc(d, retryCount, err)

	if !c.params.RetryStrategy.Enabled || retryCount >= int32(c.params.RetryStrategy.MaxAttempt) || delayAmount < 0 {
		c.deadletter(d)
		return false
	}

	c.publishRetry(d, retryCount, delayAmount, err)
	return true
}

func (c *Consumer) publishRetry(d Delivery, retryCount int32, delayAmount int32, err error) {
	headers := amqpx.MergeTable(d.Headers, amqp.Table{
		amqpx.KeyRetriesCount: retryCount + 1,
	})
	retryQueue := c.params.Queue + amqpx.SuffixRetry

	pubErr := c.channel.Publish(defaultExchange, retryQueue, false, false, amqp.Publishing{
		Expiration:  strconv.Itoa(int(delayAmount)),
		ContentType: d.ContentType,
		Body:        d.Body,
		Headers:     headers,
	})

	if pubErr != nil {
		c.conn.Logger().Error(fmt.Sprintf("failed to publish retry: %v", pubErr))
		d.delivery.Nack(false, false)
		return
	}

	if ackErr := d.delivery.Ack(false); ackErr != nil {
		c.conn.Logger().Error(fmt.Sprintf("failed to ack original - retry: %v", ackErr))
	}
}

func (c *Consumer) deadletter(d Delivery) {
	if !c.params.DeadletterStrategy.Enabled {
		c.conn.Logger().Info("dlq strategy disabled, acking message")
		if err := d.delivery.Ack(false); err != nil {
			c.conn.Logger().Error(fmt.Sprintf("failed to ack (dlq disabled): %v", err))
		}
		return
	}

	c.sendToDeadletter(d)
}

func (c *Consumer) sendToDeadletter(d Delivery) {
	defer func() {
		if r := recover(); r != nil {
			c.conn.Logger().Error(fmt.Sprintf("panic in DLQ callback, forcing nack: %v", r))
			d.delivery.Nack(false, false)
		}
	}()

	sendToDLQ := c.params.DeadletterStrategy.DLQFunc == nil ||
		c.params.DeadletterStrategy.DLQFunc(d)

	if sendToDLQ {
		if err := d.delivery.Nack(false, false); err != nil {
			c.conn.Logger().Error(fmt.Sprintf("failed to nack - deadletter: %v", err))
		}
		return
	}

	if err := d.delivery.Ack(false); err != nil {
		c.conn.Logger().Error(fmt.Sprintf("failed to ack - deadletter: %v", err))
	}
}

// --- Inspection logging ---

func (c *Consumer) inspect(d Delivery, elapsed time.Duration, err error, isDead bool) {
	title := fmt.Sprintf("[AMQP] [CONSUMER] [%s] [%s] [%s]",
		c.params.ExchangeName, d.RoutingKey, c.params.Queue)

	data := map[string]any{
		"type":          "consumer",
		"duration":      elapsed.Milliseconds(),
		"correlationId": d.CorrelationId,
		"binding": map[string]any{
			"exchange":   c.params.ExchangeName,
			"routingKey": d.RoutingKey,
			"queue":      c.params.Queue,
		},
		"isDead": isDead,
		"consumedMessage": map[string]any{
			"content": string(d.Body),
			"headers": d.Headers,
		},
	}

	if err != nil {
		data["error"] = err.Error()
		c.conn.Logger().Error(title, data)
		return
	}

	c.conn.Logger().Info(title, data)
}

// --- Queue declarations ---

func (c *Consumer) declareDLQueue(queueTable amqp.Table) error {
	if !c.params.DeadletterStrategy.Enabled {
		return nil
	}

	_, err := c.channel.QueueDeclare(c.params.DeadletterStrategy.DLQueueName, true, false, false, false,
		amqp.Table{amqpx.KeyQueueType: amqpx.QueueTypeQuorum})
	if err != nil {
		return &QueueError{Operation: opDeclareDLQ, Queue: c.params.DeadletterStrategy.DLQueueName, Err: err}
	}

	queueTable[amqpx.KeyDeadLetterExchange] = defaultExchange
	queueTable[amqpx.KeyDeadLetterRouteKey] = c.params.DeadletterStrategy.DLQueueName

	return nil
}

func (c *Consumer) declareRetryQueue() error {
	if !c.params.RetryStrategy.Enabled {
		return nil
	}

	retryQueue := c.params.Queue + amqpx.SuffixRetry
	_, err := c.channel.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
		amqpx.KeyQueueType:          amqpx.QueueTypeQuorum,
		amqpx.KeyDeadLetterExchange: defaultExchange,
		amqpx.KeyDeadLetterRouteKey: c.params.Queue,
	})
	if err != nil {
		return &QueueError{Operation: opDeclareRetry, Queue: retryQueue, Err: err}
	}

	return nil
}
