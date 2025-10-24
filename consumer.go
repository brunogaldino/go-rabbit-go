package rabbitmq

import (
	"crypto/rand"
	"fmt"
	"log"
	"maps"
	"os"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	amqp.Delivery
}

type Consumer struct {
	client       *Client
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
	Callback           func(Delivery) error
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

func (c *Client) NewConsumer(queue string, callback func(Delivery) error, options ...func(*Consumer)) *Consumer {
	consumer := &Consumer{params: *consumerDefaults}
	consumer.setOptions(queue, callback, options)

	hostname, _ := os.Hostname()
	ch, err := c.conn.Channel()
	failOnError(err, "open consumer channel")
	consumer.channel = ch

	err = ch.Qos(consumer.params.Prefetch, 0, false)
	failOnError(err, "set consumer QoS")

	queueTable := amqp.Table{
		"x-queue-type": "quorum",
	}
	queueTable.SetClientConnectionName(hostname)
	consumer.setDLQueue(queueTable)

	if _, err = ch.QueueDeclare(consumer.params.Queue, true, consumer.params.AutoDelete, false, false, queueTable); err != nil {
		log.Fatalf("could not declare consumer queue %s[%s]: %v", consumer.params.Queue, strings.Join(consumer.params.RoutingKey, ","), err)
	}

	consumer.setRetryExchange()

	for _, ex := range consumer.params.RoutingKey {
		err = ch.QueueBind(consumer.params.Queue, ex, consumer.params.ExchangeName, false, nil)
		failOnError(err, "could not bind consumer to exchange")
	}

	c.consumerMap[consumer.consumerName] = consumer
	return consumer
}

func (c *Consumer) setOptions(queue string, callback func(Delivery) error, options []func(*Consumer)) {
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

		go func(d Delivery) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("recovered from panic in goroutine consumer: %v\n", r)
					c.retry(d)
					return
				}

				c.wg.Done()
			}()

			if err := c.params.Callback(d); err != nil {
				fmt.Printf("error when processing message: %v\n", err)
				c.retry(d)
				return
			}

			err := d.Ack(false)
			failOnError(err, "could not ack message")
		}(Delivery{d})
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

	delete(c.client.consumerMap, c.consumerName)
}

func (c *Consumer) retry(d Delivery) {
	retryCount, ok := d.Headers["x-retries-count"].(int32)
	if !ok {
		retryCount = 1
	}

	if c.params.RetryStrategy.Enabled && (retryCount < int32(c.params.RetryStrategy.MaxAttempt)) {
		delayAmount := c.params.RetryStrategy.DelayFn(int32(retryCount))
		fmt.Printf("Retrying attempt %d in %d ms\n", retryCount, delayAmount)
		headers := mergeTable(d.Headers, amqp.Table{
			"x-delay":         delayAmount,
			"x-retries-count": retryCount + 1,
		})
		err := c.channel.Publish(c.params.RetryStrategy.Exchange, c.params.Queue, false, false, amqp.Publishing{
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

func (c *Consumer) deadletter(d Delivery) {
	if c.params.DeadletterStrategy.Enabled {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("panic when running DLQFn, sending to DLQ forcefully")
				d.Nack(false, false)
			}
		}()

		skip := false
		if c.params.DeadletterStrategy.CallbackFn != nil {
			skip = c.params.DeadletterStrategy.CallbackFn(string(d.Body))
		}

		if skip {
			err := d.Ack(false)
			failOnError(err, "failed to ack - deadletter")
		} else {
			err := d.Nack(false, false)
			failOnError(err, "failed to nack - deadletter")
		}
	} else {
		fmt.Printf("dlq strategy disabled")
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
