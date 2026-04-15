// Package rabbitmq provides shared types for the go-rabbit-go library.
//
// The library is organized into focused sub-packages:
//
//   - [github.com/brunogaldino/go-rabbit-go/client] — AMQP client with dual
//     connections, reconnection, and lifecycle management.
//   - [github.com/brunogaldino/go-rabbit-go/consumer] — Message consumer with
//     retry queues and dead letter support.
//   - [github.com/brunogaldino/go-rabbit-go/publisher] — Message publisher with
//     publisher confirms and exchange declaration.
//   - [github.com/brunogaldino/go-rabbit-go/manager] — Multi-vhost connection
//     manager for applications connecting to several brokers.
//   - [github.com/brunogaldino/go-rabbit-go/amqpx] — AMQP interface abstractions
//     for testability.
//
// This root package exports the [Logger] interface and shared error types
// ([ChannelError], [ErrConnectionClosed], [ErrConnectionBlocked],
// [ErrMaxReconnectAttempts]) used across sub-packages.
//
// # Getting Started
//
//	import (
//	    "github.com/brunogaldino/go-rabbit-go/client"
//	    "github.com/brunogaldino/go-rabbit-go/consumer"
//	    "github.com/brunogaldino/go-rabbit-go/publisher"
//	)
//
//	c, wg := client.New(ctx, client.Config{
//	    URI: "amqp://guest:guest@localhost:5672/",
//	})
//	c.Connect()
//
//	pub, _ := publisher.New(c, []publisher.ExchangeOption{
//	    {Name: "orders", Type: publisher.ExchangeTopic},
//	})
//
//	cons, _ := consumer.New(c, "orders.process", handler,
//	    consumer.WithExchangeName("orders"),
//	    consumer.WithRoutingKey([]string{"order.created"}),
//	)
//	wg.Go(func() { cons.Begin() })
//
//	c.Disconnect()
//	wg.Wait()
package rabbitmq
