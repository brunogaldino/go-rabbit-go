// Command reconnect-smoke is a manual smoke test for the client's
// automatic reconnection. It opens a publisher and a consumer against a
// local RabbitMQ, publishes a message every second, and prints the
// client's health on every tick so you can watch it survive a forced
// connection drop or a broker restart.
//
// See README.md in this directory for how to run it and how to force a
// disconnect.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brunogaldino/go-rabbit-go/client"
	"github.com/brunogaldino/go-rabbit-go/consumer"
	"github.com/brunogaldino/go-rabbit-go/publisher"
)

func main() {
	uri := os.Getenv("RABBITMQ_URI")
	if uri == "" {
		uri = "amqp://guest:guest@localhost:5672/"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, wg := client.New(ctx, client.Config{
		URI:                  uri,
		Heartbeat:            5 * time.Second,
		MaxReconnectAttempts: 100,
	})

	if err := c.Connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}

	pub, err := publisher.New(c.PublisherConn(), []publisher.ExchangeOption{
		{Name: "smoke", Type: publisher.ExchangeTopic},
	})
	if err != nil {
		log.Fatalf("publisher: %v", err)
	}

	cons, err := consumer.New(c, "smoke.process", handle,
		consumer.WithExchangeName("smoke"),
		consumer.WithRoutingKey([]string{"smoke.tick"}),
	)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	wg.Go(func() { cons.Begin() })

	fmt.Println("smoke test running — force a drop and watch it recover.")
	fmt.Println("  drop:    rabbitmqctl close_all_connections \"smoke test\"")
	fmt.Println("  restart: docker restart <rabbit-container>")
	fmt.Println("  stop:    Ctrl-C")
	fmt.Println()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var n int
	for {
		select {
		case <-ctx.Done():
			fmt.Println("shutting down…")
			wg.Wait()
			return

		case <-ticker.C:
			n++
			h := c.CheckHealth()
			perr := pub.Publish(publisher.Message{
				Exchange:   "smoke",
				RoutingKey: "smoke.tick",
				Message:    []byte(fmt.Sprintf(`{"tick":%d}`, n)),
			})

			fmt.Printf("tick=%-4d connected=%-5t pub=%-5t con=%-5t blocked=%-5t reconnecting=%-5t publishErr=%v\n",
				n, h.Connected, h.PublisherConnected, h.ConsumerConnected,
				h.Blocked, h.Reconnecting, perr)
		}
	}
}

func handle(d consumer.Delivery, queue string) error {
	fmt.Printf("  <- received on %q: %s\n", queue, string(d.Body))
	return nil
}
