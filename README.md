# go-rabbit-go

An opinionated Go library for RabbitMQ with for the lazy

## Table of Contents

- [Installation](#installation)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
  - [Single connection](#single-connection)
  - [Multi-vhost connections](#multi-vhost-connections)
- [Consumers](#consumers)
  - [Creating a consumer](#creating-a-consumer)
  - [Handler signature](#handler-signature)
  - [Consumer options](#consumer-options)
- [Publishers](#publishers)
  - [Creating a publisher](#creating-a-publisher)
  - [Publishing messages](#publishing-messages)
- [Retry Strategy](#retry-strategy)
  - [Default behavior](#default-behavior)
  - [Custom delay function](#custom-delay-function)
  - [Disabling retries](#disabling-retries)
  - [How it works](#how-it-works)
- [Dead Letter Strategy](#dead-letter-strategy)
  - [DLQ callback](#dlq-callback)
- [Custom Header Metadata](#custom-header-metadata)
- [Health Check](#health-check)
- [Graceful Shutdown](#graceful-shutdown)
- [Exchange Types](#exchange-types)
- [License](#license)

## Installation

```shell
go get github.com/brunogaldino/go-rabbit-go
```

## Requirements

- Go 1.25+
- RabbitMQ 3.10+ (quorum queue per-message TTL support)

## Getting Started

### Single connection

For applications that connect to a single RabbitMQ broker/vhost, use `New()` to
create a client:

```go
package main

import (
    "context"
    "os/signal"
    "syscall"
    "time"

    rabbitmq "github.com/brunogaldino/go-rabbit-go"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    client, wg := rabbitmq.New(ctx, rabbitmq.Config{
        URI:                  "amqp://guest:guest@localhost:5672/",
        Heartbeat:            10 * time.Second,
        MaxReconnectAttempts: 10,
    })

    if err := client.Connect(); err != nil {
        panic(err)
    }

    // Set up publisher and consumers...
    pub, err := client.NewPublisher([]rabbitmq.ExchangeOption{
        {Name: "orders", Type: rabbitmq.ExchangeTopic},
    })
    if err != nil {
        panic(err)
    }

    cn, err := client.NewConsumer("orders.process", handleOrder,
        rabbitmq.WithExchangeName("orders"),
        rabbitmq.WithRoutingKey([]string{"order.created"}),
    )
    if err != nil {
        panic(err)
    }
    go cn.Begin()

    // Block until shutdown signal
    wg.Wait()
}

func handleOrder(d rabbitmq.Delivery, queue string, originalRoutingKey string) error {
    // process message
    return nil
}
```

### Multi-vhost connections

When your application needs to consume from or publish to multiple RabbitMQ
vhosts (or entirely different brokers), use `NewConnectionManager()`. Each
connection is a self-contained unit with its own URI, exchanges, consumers,
and reconnection loop.

```go
cm, wg := rabbitmq.NewConnectionManager(ctx, []rabbitmq.ConnectionConfig{
    {
        Name:      "default",
        URI:       "amqp://guest:guest@localhost:5672/",
        Heartbeat: 10 * time.Second,
        Exchanges: []rabbitmq.ExchangeOption{
            {Name: "orders", Type: rabbitmq.ExchangeTopic},
        },
    },
    {
        Name:      "payments",
        URI:       "amqp://guest:guest@payments-rabbit:5672/payments",
        Heartbeat: 10 * time.Second,
        Exchanges: []rabbitmq.ExchangeOption{
            {Name: "payments", Type: rabbitmq.ExchangeTopic},
        },
    },
})

if err := cm.ConnectAll(); err != nil {
    panic(err)
}

// Create consumers on specific connections
orderConsumer, err := cm.NewConsumer("default", "orders.process", handleOrder,
    rabbitmq.WithExchangeName("orders"),
    rabbitmq.WithRoutingKey([]string{"order.created"}),
)
if err != nil {
    panic(err)
}
go orderConsumer.Begin()

paymentConsumer, err := cm.NewConsumer("payments", "payments.process", handlePayment,
    rabbitmq.WithExchangeName("payments"),
    rabbitmq.WithRoutingKey([]string{"payment.confirmed"}),
)
if err != nil {
    panic(err)
}
go paymentConsumer.Begin()

// Publish to a specific connection
cm.Publisher("default").Publish(rabbitmq.PublishMessage{
    Exchange:   "orders",
    RoutingKey: "order.created",
    Message:    []byte(`{"orderId": "123"}`),
})

cm.Wait()
```

The single-connection API (`New()` + `Connect()`) remains fully available.
`ConnectionManager` is an opt-in layer for multi-vhost scenarios.

## Consumers

### Creating a consumer

Consumers are created via `client.NewConsumer()`. Each consumer gets its own
AMQP channel, declares its queue (as a quorum queue), sets up retry and dead
letter infrastructure, and binds to the specified exchange.

```go
consumer, err := client.NewConsumer("my.queue", handler,
    rabbitmq.WithExchangeName("my-exchange"),
    rabbitmq.WithRoutingKey([]string{"routing.key.one", "routing.key.two"}),
    rabbitmq.WithPrefetch(5),
)
if err != nil {
    // handle error
}

go consumer.Begin()
```

Call `Begin()` in a goroutine — it blocks and continuously processes messages
until the consumer is disconnected or the channel is closed.

Since each consumer is started individually, you have full control over which
consumers to activate per deployment:

```go
// API deployment — only webhook consumers
s.consumeWebhooks()

// Worker deployment — only processing consumers
s.consumeOrderProcessing()
s.consumePaymentProcessing()
```

### Handler signature

Every consumer handler follows the same signature:

```go
func handler(d rabbitmq.Delivery, queue string, originalRoutingKey string) error
```

| Parameter | Description |
|-----------|-------------|
| `d` | The message delivery, wrapping `amqp.Delivery` with a `GetHeader(key)` helper |
| `queue` | The queue name the consumer is bound to |
| `originalRoutingKey` | The original routing key (from `x-original-routing-key` header, or the current routing key if the header is not set) |

Return `nil` to acknowledge the message. Return an `error` to trigger the
retry strategy.

### Consumer options

| Option | Description | Default |
|--------|-------------|---------|
| `WithExchangeName(name)` | Exchange to bind to | `""` |
| `WithRoutingKey([]string{...})` | One or more routing keys | `nil` |
| `WithPrefetch(n)` | Channel prefetch count | `10` |
| `WithAutoDelete()` | Mark queue as auto-delete | `false` |
| `WithRetryDisabled()` | Disable retry strategy | enabled |
| `WithRetryMaxAttempt(n)` | Max retry attempts | `5` |
| `WithRetryFn(fn)` | Custom delay function | `attempt * 1000` ms |
| `WithDLQFn(fn)` | Callback before sending to DLQ | `nil` |
| `WithHeadersBinding(map)` | Headers for headers-exchange binding | `nil` |

## Publishers

### Creating a publisher

A publisher is created per client and declares the exchanges it needs:

```go
pub, err := client.NewPublisher([]rabbitmq.ExchangeOption{
    {Name: "orders", Type: rabbitmq.ExchangeTopic},
    {Name: "notifications", Type: rabbitmq.ExchangeDirect},
})
if err != nil {
    // handle error
}
```

Each client supports a single publisher instance. Calling `NewPublisher()`
again returns the existing one.

### Publishing messages

```go
err := pub.Publish(rabbitmq.PublishMessage{
    Exchange:   "orders",
    RoutingKey: "order.created",
    Message:    []byte(`{"orderId": "123", "amount": 99.90}`),
    Headers:    map[string]any{"x-custom": "value"},
})
```

Publishing uses [Publisher Confirms](https://www.rabbitmq.com/docs/confirms#publisher-confirms)
by default to guarantee the message was accepted by the broker before
returning. The method blocks until the confirmation is received.

If the connection is blocked or reconnecting, `Publish()` waits up to 25
seconds before returning an error. If the connection is closing, it returns
immediately with `errConnClosed`.

## Retry Strategy

### Default behavior

By default, every consumer has retries enabled with:

- **Max attempts:** 5
- **Delay function:** `attempt * 1000` (1s, 2s, 3s, 4s, 5s)

When the handler returns an error (or panics), the message is published to
the retry queue with a TTL. Once the TTL expires, the message is routed back
to the original queue for another attempt.

### Custom delay function

The delay function receives the delivery, the current attempt number, and the
error. It returns the delay in milliseconds:

```go
rabbitmq.WithRetryFn(func(d rabbitmq.Delivery, attempt int32, err error) int32 {
    // Exponential backoff: 2s, 4s, 8s, 16s, 32s
    return int32(math.Pow(2, float64(attempt))) * 1000
})
```

### Disabling retries

```go
consumer, err := client.NewConsumer("my.queue", handler,
    rabbitmq.WithRetryDisabled(),
)
```

When retries are disabled, failed messages go directly to the dead letter
strategy.

### How it works

The retry mechanism uses native RabbitMQ features — no plugins required:

1. For each consumer queue `{queue}`, the library declares:
   - `{queue}.retry` — a quorum queue with `x-dead-letter-exchange: ""` and `x-dead-letter-routing-key: {queue}`

2. On failure, the message is published directly to `{queue}.retry` via the
   AMQP default exchange (`""`) with the `expiration` property set to the
   delay amount. The message sits in `{queue}.retry` until the TTL expires.

3. When TTL expires, RabbitMQ dead-letters the message back to the original
   `{queue}` via the DLX configuration.

4. The `x-retries-count` header tracks the current attempt number.

## Dead Letter Strategy

When a message exhausts all retry attempts (or retries are disabled), it
enters the dead letter strategy. By default, the message is nacked and routed
to `{queue}.dlq` — a quorum queue declared alongside the main queue.

### DLQ callback

You can run a callback before the message is sent to the DLQ:

```go
rabbitmq.WithDLQFn(func(content string) bool {
    // Log, alert, or inspect the failed message
    log.Printf("Message failed permanently: %s", content)

    // Return true to send to DLQ (nack)
    // Return false to acknowledge and drop the message
    return true
})
```

| Return value | Behavior |
|:---:|---|
| `true` | Message is **nacked** and sent to the DLQ |
| `false` | Message is **acknowledged** and dropped (does not go to DLQ) |

If the callback panics, the message is sent to the DLQ regardless.

## Custom Header Metadata

Every published message automatically includes the following headers:

```json
{
  "x-original-exchange": "exchange_name",
  "x-original-routing-key": "routing.key",
  "x-published-at": "2026-04-10 18:30:00 +0000 UTC"
}
```

These headers preserve the original exchange and routing key, which would
otherwise be lost when a message is routed through retry queues or the DLQ.

The `originalRoutingKey` parameter in the consumer handler is derived from
`x-original-routing-key` when available, falling back to the message's current
routing key.

User-provided headers in `PublishMessage.Headers` are merged with the
automatic headers. User headers take precedence if there are conflicts.

## Health Check

Check the connection status of a client:

```go
status := client.CheckHealth()

fmt.Println(status.Connected)    // true/false
fmt.Println(status.Blocked)      // true if the connection is blocked by the broker
fmt.Println(status.Reconnecting) // true if a reconnection is in progress
```

With `ConnectionManager`, get per-connection health:

```go
allStatus := cm.CheckHealth()
// map["default"]  → HealthStatus{Connected: true, ...}
// map["payments"] → HealthStatus{Connected: true, ...}
```

## Graceful Shutdown

The library integrates with Go's `context.Context` for graceful shutdown. When
the context is cancelled, the client:

1. Stops delivering new messages to consumers
2. Waits for in-flight message handlers to complete
3. Closes all consumer channels
4. Closes the publisher channel
5. Closes the connection

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

client, wg := rabbitmq.New(ctx, config)
client.Connect()

// ... set up consumers and publisher ...

wg.Wait() // blocks until shutdown is complete
```

With `ConnectionManager`:

```go
cm, _ := rabbitmq.NewConnectionManager(ctx, configs)
cm.ConnectAll()

// ... set up consumers and publishers ...

cm.Wait() // blocks until all connections are shut down
```

You can also trigger a manual disconnect:

```go
client.Disconnect()
// or
cm.Disconnect()
```

## Exchange Types

Convenience constants for exchange types:

```go
rabbitmq.ExchangeDirect  // "direct"
rabbitmq.ExchangeTopic   // "topic"
rabbitmq.ExchangeFanout  // "fanout"
rabbitmq.ExchangeHeader  // "header"
```

## License

[MIT License](https://github.com/brunogaldino/go-rabbit-go/blob/master/LICENSE)
