package rabbitmq

import (
	"crypto/rand"
	"fmt"
	"maps"
	"os"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	channel      *amqp.Channel
	params       ConsumerOptions
	wg           sync.WaitGroup
	consumerName string
}

type ConsumerRetry struct {
	Enabled    bool
	Exchange   string
	MaxAttempt int
	DelayFn    func(attempt int32) int32
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
	Callback           func(string) error
	RetryStrategy      *ConsumerRetry
	DeadletterStrategy *ConsumerDeadletter
}

var consumerDefaults = &ConsumerOptions{
	AutoDelete: false,
	Prefetch:   10,
	RetryStrategy: &ConsumerRetry{
		Enabled:    true,
		Exchange:   "retry",
		MaxAttempt: 5,
		DelayFn: func(attempt int32) int32 {
			return attempt * 1000
		},
	},
	DeadletterStrategy: &ConsumerDeadletter{
		Enabled:    true,
		CallbackFn: nil,
	},
}

func (c *Client) NewConsumer(queue string, callback func(string) error, options ...func(*Consumer)) *Consumer {
	consumer := &Consumer{params: *consumerDefaults}
	consumer.setOptions(queue, callback, options)

	hostname, _ := os.Hostname()
	ch, err := c.conn.Channel()
	failOnError(err, "open consumer channel")
	consumer.channel = ch

	err = ch.Qos(consumer.params.Prefetch, 0, false)
	failOnError(err, "set consumer QoS")

	queueTable := amqp.Table{}
	queueTable.SetClientConnectionName(hostname)
	consumer.setDLQueue(queueTable)

	_, err = ch.QueueDeclare(consumer.params.Queue, true, consumer.params.AutoDelete, false, false, queueTable)
	failOnError(err, "could not declare consumer queue")
	consumer.setRetryExchange()

	for _, ex := range consumer.params.RoutingKey {
		err = ch.QueueBind(consumer.params.Queue, ex, consumer.params.ExchangeName, false, nil)
		failOnError(err, "could not bind consumer to exchange")
	}

	c.consumerMap = append(c.consumerMap, consumer)
	return consumer
}

func (c *Consumer) setOptions(queue string, callback func(string) error, options []func(*Consumer)) {
	hostname, _ := os.Hostname()
	c.params.Queue = queue
	c.params.Callback = callback
	c.params.DeadletterStrategy.DLQueueName = fmt.Sprintf("%s.dlq", queue)
	cID, _ := randomID(4)
	c.consumerName = fmt.Sprintf("%s:%s:%s", hostname, c.params.ExchangeName, cID)

	for _, o := range options {
		o(c)
	}
}

func (c *Consumer) Begin() {
	fmt.Printf("Beginning message consumer %s\n", c.params.Queue)
	msgs, err := c.channel.Consume(c.params.Queue, c.consumerName, false, false, false, false, nil)
	failOnError(err, "error beginning consumer")

	var forever chan struct{}
	for d := range msgs {
		c.wg.Add(1)

		go func(d amqp.Delivery) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("recovered from panic in goroutine consumer: %v\n", r)
					c.retry(d)
				}

				c.wg.Done()
			}()

			if err := c.params.Callback(string(d.Body)); err != nil {
				fmt.Printf("error when processing message: %v\n", err)
				c.retry(d)
			}

			err := d.Ack(false)
			failOnError(err, "could not ack message")
		}(d)
	}
	<-forever
}

func (c *Consumer) Disconnect() {
	fmt.Printf("Stopping delivering messages to consumer %s\n", c.consumerName)
	err := c.channel.Cancel(c.consumerName, false)
	if err != nil {
		fmt.Println(err)
	}
	c.wg.Wait()

	if err := c.channel.Close(); err != nil {
		fmt.Printf("error closing consumer channel %s with RK: %s: %v", c.params.Queue, c.params.RoutingKey, err)
	}
}

func (c *Consumer) retry(d amqp.Delivery) {
	retryCount, ok := d.Headers["x-retries-count"].(int32)
	if !ok {
		retryCount = 1
	}

	if retryCount < int32(c.params.RetryStrategy.MaxAttempt) {
		delayAmount := c.params.RetryStrategy.DelayFn(int32(retryCount))
		fmt.Printf("Retrying attempt %d in %d ms\n", retryCount, delayAmount)
		headers := mergeTable(d.Headers, amqp.Table{
			"x-delay":         delayAmount,
			"x-retries-count": retryCount + 1,
		})
		err := c.channel.Publish("retry", c.params.Queue, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        d.Body,
			Headers:     headers,
		})
		failOnError(err, "failed to publish message on retry")

		err = d.Ack(false)
		failOnError(err, "failed to ack original message - retry")
	} else {
		c.deadletter(d)
	}
}

func (c *Consumer) deadletter(d amqp.Delivery) {
	if c.params.DeadletterStrategy != nil && c.params.DeadletterStrategy.Enabled {
		// TODO: Deal with recover and forcefully send to DeadLetter
		skip := c.params.DeadletterStrategy.CallbackFn(string(d.Body))

		if skip {
			err := d.Ack(false)
			failOnError(err, "failed to ack - deadletter")
		} else {
			err := d.Nack(false, false)
			failOnError(err, "failed to nack - deadletter")
		}
	} else {
		err := d.Ack(false)
		failOnError(err, "failed skip dlq strategy to ack - deadletter")
	}
}

func (c *Consumer) setDLQueue(queueTable amqp.Table) {
	if c.params.DeadletterStrategy.Enabled {
		_, err := c.channel.QueueDeclare(c.params.DeadletterStrategy.DLQueueName, true, false, false, false,
			amqp.Table{
				"x-queue-type": "quorum",
			})
		failOnError(err, "could not declare DLQ")
		queueTable["x-dead-letter-exchange"] = ""
		queueTable["x-dead-letter-routing-key"] = c.params.DeadletterStrategy.DLQueueName
		queueTable["x-queue-type"] = "quorum"
	}
}

func (c *Consumer) setRetryExchange() {
	if c.params.RetryStrategy.Enabled {
		err := c.channel.ExchangeDeclare(c.params.RetryStrategy.Exchange, "x-delayed-message", true, false, false, false, amqp.Table{
			"x-delayed-type": "direct",
		})
		failOnError(err, "cannot declare retry exchange")

		err = c.channel.QueueBind(c.params.Queue, c.params.Queue, c.params.RetryStrategy.Exchange, false, nil)
		failOnError(err, "could not bind to queue")
	} else {
		err := c.channel.QueueUnbind(c.params.Queue, c.params.Queue, c.params.RetryStrategy.Exchange, nil)
		failOnError(err, "could not unbind to retry to queue")
	}
}

func mergeTable(old amqp.Table, new amqp.Table) amqp.Table {
	merged := amqp.Table{}
	maps.Copy(merged, old)
	maps.Copy(merged, new)
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
