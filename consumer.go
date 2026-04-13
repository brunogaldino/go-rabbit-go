package rabbitmq

import (
	"crypto/rand"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	amqp.Delivery
}

// GetHeader returns the value of a header by key, or nil if not present.
func (d Delivery) GetHeader(key string) any {
	if d.Headers == nil {
		return nil
	}

	return d.Headers[key]
}

func (d Delivery) GetRoutingKey() string {
	rk, ok := d.Headers["x-original-routing-key"]
	if !ok {
		return d.RoutingKey
	}

	return rk.(string)
}

type Consumer struct {
	client       *Client
	channel      *amqp.Channel
	params       ConsumerOptions
	wg           sync.WaitGroup
	consumerName string
	closing      atomic.Bool
}

type ConsumerRetry struct {
	Enabled    bool
	MaxAttempt int
	DelayFn    func(content Delivery, attempt int32, err error) int32
}

type ConsumerDeadletter struct {
	Enabled     bool
	DLQueueName string
	CallbackFn  func(string) bool
}

type ConsumerOptions struct {
	Queue              string
	RoutingKey         []string
	ExchangeName       string
	AutoDelete         bool
	Prefetch           int
	Callback           func(Delivery, string) error
	RetryStrategy      *ConsumerRetry
	DeadletterStrategy *ConsumerDeadletter
	HeadersBinding     map[string]any
}

var consumerDefaults = &ConsumerOptions{
	AutoDelete: false,
	Prefetch:   10,
	RetryStrategy: &ConsumerRetry{
		Enabled:    true,
		MaxAttempt: 5,
		DelayFn: func(d Delivery, attempt int32, err error) int32 {
			return attempt * 1000
		},
	},
	DeadletterStrategy: &ConsumerDeadletter{
		Enabled:    true,
		CallbackFn: nil,
	},
}

func (c *Client) NewConsumer(queue string, callback func(Delivery, string) error, options ...func(*Consumer)) (*Consumer, error) {
	retryCopy := *consumerDefaults.RetryStrategy
	dlCopy := *consumerDefaults.DeadletterStrategy
	defaults := *consumerDefaults
	defaults.RetryStrategy = &retryCopy
	defaults.DeadletterStrategy = &dlCopy

	consumer := &Consumer{params: defaults}
	consumer.client = c
	consumer.setOptions(queue, callback, options)

	if err := consumer.setup(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.consumerMap[consumer.consumerName] = consumer
	c.mu.Unlock()
	return consumer, nil
}

// setup creates the AMQP channel and declares all queues and bindings.
// It is called on initial creation and after reconnection.
func (c *Consumer) setup() error {
	ch, err := c.client.consumerConn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel: %w", err)
	}
	c.channel = ch

	if err = ch.Qos(c.params.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set consumer QoS: %w", err)
	}

	queueTable := amqp.Table{
		"x-queue-type": "quorum",
	}

	queueTable.SetClientConnectionName(c.client.hostname)
	if err := c.setDLQueue(queueTable); err != nil {
		return err
	}

	if _, err = ch.QueueDeclare(c.params.Queue, true, c.params.AutoDelete, false, false, queueTable); err != nil {
		return fmt.Errorf("could not declare consumer queue %s[%s]: %w", c.params.Queue, strings.Join(c.params.RoutingKey, ","), err)
	}

	if err := c.setRetryQueue(); err != nil {
		return err
	}

	for _, ex := range c.params.RoutingKey {
		if err = ch.QueueBind(c.params.Queue, ex, c.params.ExchangeName, false, amqp.Table(c.params.HeadersBinding)); err != nil {
			return fmt.Errorf("could not bind consumer to exchange: %w", err)
		}
	}

	return nil
}

func (c *Consumer) setOptions(queue string, callback func(Delivery, string) error, options []func(*Consumer)) {
	c.params.Queue = queue
	c.params.Callback = callback
	c.params.DeadletterStrategy.DLQueueName = fmt.Sprintf("%s.dlq", queue)
	cID, _ := randomID(4)
	c.consumerName = fmt.Sprintf("%s:%s:%s", c.client.hostname, c.params.ExchangeName, cID)

	for _, o := range options {
		o(c)
	}
}

func (c *Consumer) Begin(groups ...string) error {
	for {
		fmt.Printf("Beginning message consumer %s\n", c.params.Queue)
		c.consume()
		c.wg.Wait()

		if c.closing.Load() || c.client.isClosing.Load() {
			return nil
		}

		fmt.Printf("consumer %s channel closed, waiting for reconnection...\n", c.params.Queue)

		for !c.client.isConConnected.Load() {
			if c.closing.Load() || c.client.isClosing.Load() {
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}

		if err := c.setup(); err != nil {
			fmt.Printf("failed to reattach consumer %s: %v\n", c.params.Queue, err)
			time.Sleep(1 * time.Second)
			continue
		}
	}
}

func (c *Consumer) consume() {
	msgs, err := c.channel.Consume(c.params.Queue, c.consumerName, false, false, false, false, nil)
	if err != nil {
		fmt.Printf("error beginning consumer %s: %v\n", c.params.Queue, err)
		return
	}

	for d := range msgs {
		c.wg.Add(1)

		go func(d Delivery) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("recovered from panic in goroutine consumer: %v\n", r)
					c.retry(d, fmt.Errorf("panic: %v", r))
				}

				c.wg.Done()
			}()

			if err := c.params.Callback(d, c.params.Queue); err != nil {
				fmt.Printf("error when processing message: %v\n", err)
				c.retry(d, err)
				return
			}

			err := d.Ack(false)
			if err != nil {
				fmt.Printf("could not ack message: %v\n", err)
			}
		}(Delivery{d})
	}
}

func (c *Consumer) Disconnect() {
	c.closing.Store(true)
	fmt.Printf("Stopping delivering messages to consumer %s\n", c.consumerName)
	err := c.channel.Cancel(c.consumerName, false)
	if err != nil {
		fmt.Println(err)
	}
	c.wg.Wait()

	if err := c.channel.Close(); err != nil {
		fmt.Printf("error closing consumer channel %s with RK: %s: %v", c.params.Queue, c.params.RoutingKey, err)
	}

	c.client.mu.Lock()
	delete(c.client.consumerMap, c.consumerName)
	c.client.mu.Unlock()
}

func (c *Consumer) retry(d Delivery, err error) {
	retryCount, ok := d.Headers["x-retries-count"].(int32)
	if !ok {
		retryCount = 1
	}

	if c.params.RetryStrategy.Enabled && (retryCount < int32(c.params.RetryStrategy.MaxAttempt)) {
		delayAmount := c.params.RetryStrategy.DelayFn(d, int32(retryCount), err)

		headers := mergeTable(d.Headers, amqp.Table{
			"x-retries-count": retryCount + 1,
		})

		retryQueue := fmt.Sprintf("%s.retry", c.params.Queue)
		if pubErr := c.channel.Publish("", retryQueue, false, false, amqp.Publishing{
			Expiration:  strconv.Itoa(int(delayAmount)),
			ContentType: d.ContentType,
			Body:        d.Body,
			Headers:     headers,
		}); pubErr != nil {
			fmt.Printf("failed to publish message on retry: %v\n", pubErr)
			d.Nack(false, false)
			return
		}

		if ackErr := d.Ack(false); ackErr != nil {
			fmt.Printf("failed to ack original message - retry: %v\n", ackErr)
		}
	} else {
		c.deadletter(d)
	}
}

func (c *Consumer) deadletter(d Delivery) {
	if c.params.DeadletterStrategy.Enabled {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("panic when running DLQFn, sending to DLQ forcefully")
				d.Nack(false, false)
			}
		}()

		sendToDLQ := true
		if c.params.DeadletterStrategy.CallbackFn != nil {
			sendToDLQ = c.params.DeadletterStrategy.CallbackFn(string(d.Body))
		}

		if sendToDLQ {
			if err := d.Nack(false, false); err != nil {
				fmt.Printf("failed to nack - deadletter: %v\n", err)
			}
		} else {
			if err := d.Ack(false); err != nil {
				fmt.Printf("failed to ack - deadletter: %v\n", err)
			}
		}
	} else {
		fmt.Printf("dlq strategy disabled")
		if err := d.Ack(false); err != nil {
			fmt.Printf("failed skip dlq strategy to ack - deadletter: %v\n", err)
		}
	}
}

func (c *Consumer) setDLQueue(queueTable amqp.Table) error {
	if c.params.DeadletterStrategy.Enabled {
		_, err := c.channel.QueueDeclare(c.params.DeadletterStrategy.DLQueueName, true, false, false, false,
			amqp.Table{
				"x-queue-type": "quorum",
			})
		if err != nil {
			return fmt.Errorf("could not declare DLQ: %w", err)
		}
		queueTable["x-dead-letter-exchange"] = ""
		queueTable["x-dead-letter-routing-key"] = c.params.DeadletterStrategy.DLQueueName
	}
	return nil
}

func (c *Consumer) setRetryQueue() error {
	if !c.params.RetryStrategy.Enabled {
		return nil
	}

	retryQueue := fmt.Sprintf("%s.retry", c.params.Queue)
	_, err := c.channel.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": c.params.Queue,
	})
	if err != nil {
		return fmt.Errorf("cannot declare retry queue: %w", err)
	}
	return nil
}

func mergeTable(base, override amqp.Table) amqp.Table {
	merged := maps.Clone(base)
	if merged == nil {
		merged = amqp.Table{}
	}
	maps.Copy(merged, override)
	return merged
}

func randomID(n int) (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	for i, b := range bytes {
		bytes[i] = letters[int(b)%len(letters)]
	}
	return string(bytes), nil
}
