# go-rabbit-go

[![Go Reference](https://pkg.go.dev/badge/github.com/brunogaldino/go-rabbit-go.svg)](https://pkg.go.dev/github.com/brunogaldino/go-rabbit-go)

An opinionated Go library for RabbitMQ — automatic reconnection, retry
queues, dead letter handling, and publisher confirms out of the box.

## Table of Contents

- [Installation](#installation)
- [Requirements](#requirements)
- [Packages](#packages)
- [Getting Started](#getting-started)
  - [Single connection](#single-connection)
  - [Multi-vhost connections](#multi-vhost-connections)
- [Architecture](#architecture)
  - [Dual connections, dialed lazily](#dual-connections-dialed-lazily)
  - [Consumer auto-recovery](#consumer-auto-recovery)
  - [Project layout](#project-layout)
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
- [Custom Logger](#custom-logger)
- [Inspection Logging](#inspection-logging)
  - [Log types](#log-types)
  - [Environment variable](#environment-variable)
  - [What gets logged](#what-gets-logged)
- [Error Types](#error-types)
- [License](#license)

## Installation

```shell
go get github.com/brunogaldino/go-rabbit-go
```

## Requirements

- Go 1.25+
- RabbitMQ 3.10+ (quorum queue per-message TTL support)

## Packages

The library is split into focused, independent packages:

| Package | Import | Description |
|---------|--------|-------------|
| `rabbitmq` | `github.com/brunogaldino/go-rabbit-go` | Root — shared types: `Logger`, `ChannelError`, sentinel errors |
| `client` | `github.com/brunogaldino/go-rabbit-go/client` | Connection lifecycle, reconnection, health checks |
| `consumer` | `github.com/brunogaldino/go-rabbit-go/consumer` | Queue consumers with retry and dead-letter strategies |
| `publisher` | `github.com/brunogaldino/go-rabbit-go/publisher` | Exchange publishers with confirms |
| `manager` | `github.com/brunogaldino/go-rabbit-go/manager` | Multi-vhost connection manager |
| `amqpx` | `github.com/brunogaldino/go-rabbit-go/amqpx` | AMQP interface abstractions (`AMQPConnection`, `AMQPChannel`, `Dialer`) |

## Getting Started

### Single connection

For applications that connect to a single RabbitMQ broker/vhost:

```go
package main

import (
    "context"
    "os/signal"
    "syscall"
    "time"

    "github.com/brunogaldino/go-rabbit-go/client"
    "github.com/brunogaldino/go-rabbit-go/consumer"
    "github.com/brunogaldino/go-rabbit-go/publisher"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    c, wg := client.New(ctx, client.Config{
        URI:                  "amqp://guest:guest@localhost:5672/",
        Heartbeat:            10 * time.Second,
        MaxReconnectAttempts: 10,
        FatalOnDisconnnect:   false,
    })

    // Optional: validates the URI. Connections are dialed lazily when the
    // first publisher/consumer is created.
    if err := c.Connect(); err != nil {
        panic(err)
    }

    // Set up publisher
    pub, err := publisher.New(c.PublisherConn(), []publisher.ExchangeOption{
        {Name: "orders", Type: publisher.ExchangeTopic},
    })
    if err != nil {
        panic(err)
    }

    // Set up consumer
    cons, err := consumer.New(c, "orders.process", handleOrder,
        consumer.WithExchangeName("orders"),
        consumer.WithRoutingKey([]string{"order.created"}),
    )
    if err != nil {
        panic(err)
    }
    wg.Go(func() { cons.Begin() })

    // Publish a message
    pub.Publish(publisher.Message{
        Exchange:   "orders",
        RoutingKey: "order.created",
        Message:    []byte(`{"orderId": "123"}`),
    })

    // Block until shutdown signal
    wg.Wait()
}

func handleOrder(d consumer.Delivery, queue string) error {
    // process message
    return nil
}
```

### Multi-vhost connections

When your application needs to consume from or publish to multiple RabbitMQ
vhosts (or entirely different brokers), use the `manager` package. Each
connection is a self-contained unit with its own URI, reconnection loop, and
lifecycle.

```go
import (
    "github.com/brunogaldino/go-rabbit-go/client"
    "github.com/brunogaldino/go-rabbit-go/consumer"
    "github.com/brunogaldino/go-rabbit-go/manager"
    "github.com/brunogaldino/go-rabbit-go/publisher"
)

cm := manager.New(ctx, []manager.ConnectionConfig{
    {
        Name:   "default",
        Config: client.Config{
            URI:       "amqp://guest:guest@localhost:5672/",
            Heartbeat: 10 * time.Second,
        },
    },
    {
        Name:   "payments",
        Config: client.Config{
            URI:       "amqp://guest:guest@payments-rabbit:5672/payments",
            Heartbeat: 10 * time.Second,
        },
    },
})

if err := cm.ConnectAll(); err != nil {
    panic(err)
}

// Get a specific client and create consumers/publishers on it
defaultClient := cm.Client("default")

pub, _ := publisher.New(defaultClient.PublisherConn(), []publisher.ExchangeOption{
    {Name: "orders", Type: publisher.ExchangeTopic},
})

cons, _ := consumer.New(defaultClient, "orders.process", handleOrder,
    consumer.WithExchangeName("orders"),
    consumer.WithRoutingKey([]string{"order.created"}),
)
go cons.Begin()

cm.Wait()
```

The single-connection API (`client.New()` + `Connect()`) remains fully available.
`manager` is an opt-in layer for multi-vhost scenarios.

## Architecture

### Dual connections, dialed lazily

Each `Client` manages **up to two independent AMQP connections** to the broker.
A connection is only dialed when the first channel for its role is requested —
that is, when the first publisher or consumer is created on the client. An
application that only publishes (or only consumes) opens a single connection,
and no empty connections are left idling on the broker.

| Connection | Purpose | Identified as | Dialed when |
|-----------|---------|---------------|-------------|
| Publisher | All publish operations | `{hostname}-publisher` | First `publisher.New()` |
| Consumer | All consumer channels | `{hostname}-consumer` | First `consumer.New()` |

This isolation means:

- **Flow control** (broker blocking) only affects the publisher — consumers keep processing.
- **Reconnection** of one side doesn't tear down the other.
- Each established connection has its own monitor goroutine and reconnection loop.

`Connect()` (and `manager.ConnectAll()`) no longer dial the broker: they only
validate the configured URI and are optional. Broker-unreachable errors
surface at `publisher.New()` / `consumer.New()`, which already return errors.

### Consumer auto-recovery

`Begin()` automatically reconnects when the consumer channel or connection drops:

1. The delivery channel closes → `Begin()` waits for in-flight handlers to finish.
2. It polls until the consumer connection is re-established.
3. It re-creates the AMQP channel, re-declares queues/bindings, and resumes consuming.

This means callers don't need to implement retry logic around `Begin()` — it
handles transient failures internally and only returns when the consumer is
explicitly disconnected or the context is cancelled.

### Project layout

```
go-rabbit-go/
├── doc.go                  # Package overview (root = shared types)
├── logger.go               # Logger interface + default implementation
├── errors.go               # ChannelError + sentinel errors (shared)
├── errors_test.go
├── amqpx/                  # AMQP interface abstractions
│   ├── interfaces.go       # AMQPConnection, AMQPChannel, Dialer
│   ├── adapter.go          # ConnAdapter, DefaultDialer
│   ├── adapter_test.go
│   ├── keys.go             # AMQP table key constants
│   ├── keys_test.go
│   ├── table.go            # MergeTable helper
│   └── table_test.go
├── client/
│   ├── client.go           # Client, Config, HealthStatus, connection lifecycle
│   ├── client_test.go
│   ├── errors.go           # DialError
│   └── mocks.go            # Test doubles
├── consumer/
│   ├── consumer.go         # ConnProvider, Consumer, Delivery, options, retry/DLQ
│   ├── consumer_test.go
│   ├── errors.go           # QueueError
│   └── mocks.go            # Test doubles
├── publisher/
│   ├── publisher.go        # ConnProvider, Publisher, ExchangeOption, Message
│   ├── publisher_test.go
│   ├── errors.go           # ExchangeError, PublishError
│   └── mocks.go            # Test doubles
├── manager/
│   ├── manager.go          # ConnectionManager, ConnectionConfig
│   ├── manager_test.go
│   ├── errors.go           # ConnectionError
│   └── mocks.go            # Test doubles
└── internal/
    ├── conn/               # Managed connection state
    │   ├── managed.go
    │   ├── managed_test.go
    │   └── mocks.go        # Test doubles
    └── rand/               # Random ID generation
        ├── rand.go
        └── rand_test.go
```

Each sub-package defines its own `ConnProvider` interface describing exactly
what it needs from the connection owner. The `client` package implements both
interfaces, keeping packages decoupled and independently testable.

## Consumers

### Creating a consumer

Consumers are created via `consumer.New()`. Each consumer gets its own AMQP
channel, declares its queue (as a quorum queue), sets up retry and dead letter
infrastructure, and binds to the specified exchange.

```go
cons, err := consumer.New(c, "my.queue", handler,
    consumer.WithExchangeName("my-exchange"),
    consumer.WithRoutingKey([]string{"routing.key.one", "routing.key.two"}),
    consumer.WithPrefetch(5),
)
if err != nil {
    // handle error
}

wg.Go(func() { cons.Begin() })
```

Call `Begin()` in a goroutine — it blocks and continuously processes messages
until the consumer is disconnected or the context is cancelled. If the
underlying channel or connection drops, `Begin()` automatically reconnects and
resumes consuming (see [Consumer auto-recovery](#consumer-auto-recovery)).

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
func handler(d consumer.Delivery, queue string) error
```

| Parameter | Description |
|-----------|-------------|
| `d` | The message delivery, wrapping `amqp.Delivery` with `GetHeader(key)` and `GetRoutingKey()` helpers |
| `queue` | The queue name the consumer is bound to |

Use `d.GetRoutingKey()` to get the original routing key — it reads from the
`x-original-routing-key` header if present, falling back to the current
routing key.

Return `nil` to acknowledge the message. Return an `error` to trigger the
retry strategy.

### Consumer options

| Option | Description | Default |
|--------|-------------|---------|
| `consumer.WithExchangeName(name)` | Exchange to bind to | `""` |
| `consumer.WithRoutingKey([]string{...})` | One or more routing keys | `nil` |
| `consumer.WithPrefetch(n)` | Channel prefetch count | `10` |
| `consumer.WithAutoDelete()` | Mark queue as auto-delete | `false` |
| `consumer.WithRetryDisabled()` | Disable retry strategy | enabled |
| `consumer.WithRetryMaxAttempt(n)` | Max retry attempts | `5` |
| `consumer.WithRetryFn(fn)` | Custom delay function | `attempt * 1000` ms |
| `consumer.WithDLQFn(fn)` | Callback before sending to DLQ | `nil` |
| `consumer.WithHeadersBinding(map)` | Headers for headers-exchange binding | `nil` |

## Publishers

### Creating a publisher

A publisher is created per client and declares the exchanges it needs:

```go
pub, err := publisher.New(c.PublisherConn(), []publisher.ExchangeOption{
    {Name: "orders", Type: publisher.ExchangeTopic},
    {Name: "notifications", Type: publisher.ExchangeDirect},
})
if err != nil {
    // handle error
}
```

Each client supports a single publisher instance. The client's `PublisherConn()`
method returns a connection provider that routes to the publisher connection.

### Publishing messages

```go
err := pub.Publish(publisher.Message{
    Exchange:      "orders",
    RoutingKey:    "order.created",
    Message:       []byte(`{"orderId": "123", "amount": 99.90}`),
    Headers:       map[string]any{"x-custom": "value"},
    CorrelationId: "abc-123",
})
```

Publishing uses [Publisher Confirms](https://www.rabbitmq.com/docs/confirms#publisher-confirms)
by default to guarantee the message was accepted by the broker before
returning. The method blocks until the confirmation is received.

If the connection is blocked or reconnecting, `Publish()` waits up to 25
seconds before returning an error. If the connection is closing, it returns
immediately with `ErrConnectionClosed`.

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
consumer.WithRetryFn(func(d consumer.Delivery, attempt int32, err error) int32 {
    // Exponential backoff: 2s, 4s, 8s, 16s, 32s
    return int32(math.Pow(2, float64(attempt))) * 1000
})
```

Returning `< 0` will skip the retry entirely and send the message to the callback DLQ.

### Disabling retries

```go
cons, err := consumer.New(c, "my.queue", handler,
    consumer.WithRetryDisabled(),
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
consumer.WithDLQFn(func(content string) bool {
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

The original routing key is accessible via `d.GetRoutingKey()` in the consumer
handler — it reads from `x-original-routing-key` when available, falling back
to the message's current routing key.

User-provided headers in `Message.Headers` are merged with the
automatic headers. User headers take precedence if there are conflicts.

## Health Check

Check the connection status of a client:

```go
status := c.CheckHealth()

fmt.Println(status.Connected)          // true when every *requested* connection is up
fmt.Println(status.PublisherConnected)  // publisher connection status
fmt.Println(status.ConsumerConnected)   // consumer connection status
fmt.Println(status.Blocked)            // true if the publisher is blocked by the broker
fmt.Println(status.Reconnecting)       // true if any reconnection is in progress
```

Connections are dialed lazily, so `Connected` only accounts for the roles the
application actually uses: a publisher-only app reports `Connected: true` with
`ConsumerConnected: false`, since the consumer connection was never requested.
After `Disconnect()`, `Connected` is always `false`.

With `manager`, get per-connection health:

```go
allStatus := cm.CheckHealth()
// map["default"]  → client.HealthStatus{Connected: true, ...}
// map["payments"] → client.HealthStatus{Connected: true, ...}
```

## Graceful Shutdown

The library integrates with Go's `context.Context` for graceful shutdown. When
the context is cancelled, the client:

1. Stops delivering new messages to consumers
2. Waits for in-flight message handlers to complete
3. Closes all consumer channels
4. Closes the publisher channel
5. Closes any established connections (publisher and/or consumer)

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

c, wg := client.New(ctx, client.Config{
    URI: "amqp://guest:guest@localhost:5672/",
})
c.Connect()

// ... set up consumers and publisher ...

wg.Wait() // blocks until shutdown is complete
```

With `manager`:

```go
cm := manager.New(ctx, configs)
cm.ConnectAll()

// ... set up consumers and publishers ...

cm.Wait() // blocks until all connections are shut down
```

You can also trigger a manual disconnect:

```go
c.Disconnect()
// or
cm.Disconnect()
```

## Exchange Types

Convenience constants for exchange types (in the `publisher` package):

```go
publisher.ExchangeDirect  // "direct"
publisher.ExchangeTopic   // "topic"
publisher.ExchangeFanout  // "fanout"
publisher.ExchangeHeader  // "header"
```

## Custom Logger

By default the library logs to stdout with `[INFO]`/`[ERROR]` prefixes.
Provide a custom logger via `Config.Logger`:

```go
type Logger interface {
    Info(msg string, data ...map[string]any)
    Error(msg string, data ...map[string]any)
}
```

Operational logs (reconnections, channel errors, etc.) pass only `msg` with no
data map. Inspection logs pass a title string plus a structured `map[string]any`
containing message metadata — see [Inspection Logging](#inspection-logging).

```go
c, wg := client.New(ctx, client.Config{
    URI:    "amqp://localhost/",
    Logger: myZapLogger,
})
```

**Example adapter** (zerolog):

```go
type zerologAdapter struct{ log zerolog.Logger }

func (z *zerologAdapter) Info(msg string, data ...map[string]any) {
    e := z.log.Info()
    if len(data) > 0 {
        e = e.Fields(data[0])
    }
    e.Msg(msg)
}

func (z *zerologAdapter) Error(msg string, data ...map[string]any) {
    e := z.log.Error()
    if len(data) > 0 {
        e = e.Fields(data[0])
    }
    e.Msg(msg)
}
```

Use `rabbitmq.NewDefaultLogger()` to get the built-in logger if needed.

## Inspection Logging

The library can emit structured logs for every consumed and published message,
useful for debugging message flows and monitoring performance. This is disabled
by default and must be explicitly enabled.

### Log types

Control which operations are logged via `Config.LogType`:

```go
import rabbitmq "github.com/brunogaldino/go-rabbit-go"

c, wg := client.New(ctx, client.Config{
    URI:     "amqp://localhost/",
    LogType: rabbitmq.LogTypeAll,
})
```

| Value | Constant | Description |
|-------|----------|-------------|
| `"none"` | `rabbitmq.LogTypeNone` | No inspection logs (default) |
| `"consumer"` | `rabbitmq.LogTypeConsumer` | Log consumed messages |
| `"publisher"` | `rabbitmq.LogTypePublisher` | Log published messages |
| `"all"` | `rabbitmq.LogTypeAll` | Log both consumed and published messages |

### Environment variable

Set `GORABBIT_LOG_TYPE` to override the `Config.LogType` value without code
changes. This is resolved once at client creation:

```shell
GORABBIT_LOG_TYPE=all ./my-service
```

The env var takes precedence over `Config.LogType` when set.

### What gets logged

**Errors are always logged** regardless of the `LogType` setting. Successful
operations are only logged when the matching category is enabled.

Inspection logs call `Logger.Info()` (or `Logger.Error()`) with a title string
and a structured `map[string]any`. Your logger decides the output format.

**Consumer inspection** — logged after each message is processed:

```
title: "[AMQP] [CONSUMER] [my-exchange] [order.created] [orders.process]"
data:  map[string]any{
    "type":          "consumer",
    "duration":      12,                  // milliseconds
    "correlationId": "abc-123",
    "binding": map[string]any{
        "exchange":   "my-exchange",
        "routingKey": "order.created",
        "queue":      "orders.process",
    },
    "isDead": false,
    "consumedMessage": map[string]any{
        "content": `{"orderId": "123"}`,
        "headers": map[string]any{ ... },
    },
    // "error": "connection timeout"      // only present on failure
}
```

**Publisher inspection** — logged after each publish:

```
title: "[AMQP] [PUBLISH] [orders] [order.created]"
data:  map[string]any{
    "type":          "publisher",
    "duration":      1,                   // milliseconds
    "correlationId": "abc-123",
    "binding": map[string]any{
        "exchange":   "orders",
        "routingKey": "order.created",
    },
    "publishedMessage": map[string]any{
        "content": `{"orderId": "123"}`,
        "headers": map[string]any{ ... },
    },
    // "error": "nack for delivery tag 5" // only present on failure
}
```

The `isDead` field in consumer logs indicates whether the message was sent to
the dead letter queue (all retries exhausted and no further retry was possible).

## Error Types

Each package defines domain-specific error types:

| Package | Error | Description |
|---------|-------|-------------|
| `rabbitmq` | `ChannelError` | AMQP channel failure (shared) |
| `rabbitmq` | `ErrConnectionClosed` | Connection is closing (sentinel) |
| `rabbitmq` | `ErrConnectionBlocked` | Publisher blocked by broker (sentinel) |
| `rabbitmq` | `ErrMaxReconnectAttempts` | Reconnection attempts exhausted (sentinel) |
| `client` | `DialError` | AMQP dial failure |
| `consumer` | `QueueError` | Queue declaration/binding failure |
| `publisher` | `ExchangeError` | Exchange declaration failure |
| `publisher` | `PublishError` | Message publish/confirmation failure |
| `manager` | `ConnectionError` | Named connection failure |

All error types implement `Unwrap()` for use with `errors.Is()` and `errors.As()`.

## License

[MIT License](https://github.com/brunogaldino/go-rabbit-go/blob/master/LICENSE)
