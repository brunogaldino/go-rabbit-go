package client

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
	params       ConsumerParams
	wg           sync.WaitGroup
	consumerName string
}

type ConsumerRetry struct {
	Enabled    bool
	MaxAttempt int
	DelayFn    func(attempt int32) int32
}

type ConsumerDeadletter struct {
	Enabled    bool
	CallbackFn func(string) bool
}

type ConsumerParams struct {
	Queue              string
	RoutingKey         string
	ExchangeName       string
	AutoDelete         bool
	Prefetch           int
	Callback           func(string) error
	RetryStrategy      *ConsumerRetry
	DeadletterStrategy *ConsumerDeadletter
}

func (c *Client) NewConsumer(params ConsumerParams) *Consumer {
	dlqName := fmt.Sprintf("%s.dlq", params.Queue)
	hostname, _ := os.Hostname()
	ch, err := c.conn.Channel()
	failOnError(err, "open consumer channel")

	err = ch.Qos(params.Prefetch, 0, false)
	failOnError(err, "set consumer QoS")

	queueTable := amqp.Table{}
	queueTable.SetClientConnectionName(hostname)

	if params.DeadletterStrategy != nil && params.DeadletterStrategy.Enabled {
		_, err := ch.QueueDeclare(fmt.Sprintf("%s.dlq", params.Queue), true, false, false, false,
			amqp.Table{
				"x-queue-type": "quorum",
			})
		failOnError(err, "could not declare DLQ")
		queueTable["x-dead-letter-exchange"] = ""
		queueTable["x-dead-letter-routing-key"] = dlqName
		queueTable["x-queue-type"] = "quorum"
	}

	_, err = ch.QueueDeclare(params.Queue, true, params.AutoDelete, false, false, queueTable)
	failOnError(err, "could not declare consumer queue")
	err = ch.QueueBind(params.Queue, params.RoutingKey, params.ExchangeName, false, nil)
	failOnError(err, "could not bind consumer to exchange")

	if params.RetryStrategy != nil {
		if params.RetryStrategy.Enabled {
			err := ch.ExchangeDeclare("retry", "x-delayed-message", true, false, false, false, amqp.Table{
				"x-delayed-type": "direct",
			})
			failOnError(err, "cannot declare retry exchange")

			err = ch.QueueBind(params.Queue, params.Queue, "retry", false, nil)
			failOnError(err, "could not bind to queue")
		} else {
			err := ch.QueueUnbind(params.Queue, params.Queue, "retry", nil)
			failOnError(err, "could not unbind to retry to queue")
		}
	}

	cID, _ := randomID(4)
	consumer := &Consumer{
		channel:      ch,
		params:       params,
		consumerName: fmt.Sprintf("%s:%s:%s:%s", hostname, params.ExchangeName, params.RoutingKey, cID),
	}

	c.consumerMap = append(c.consumerMap, consumer)
	return consumer
}

func (c *Consumer) Begin() {
	fmt.Printf("Beginning message consumer %s. Closed: %t\n", c.params.Queue, c.channel.IsClosed())
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
