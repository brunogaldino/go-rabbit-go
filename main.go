package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kanastra-Tech/go-rabbitmq-sdk/pkg/client"
)

var i int = 0

func main() {
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())

	cli, rwg := client.New(ctx, client.Config{
		URI:                  "amqp://guest:guest@localhost:5672/",
		Heartbeat:            10 * time.Second,
		MaxReconnectAttempts: 5,
	})
	err := cli.Connect()

	go func() {
		sig := <-stopChan
		fmt.Println("\nCapturou o sinal", sig)
		cancel()
	}()

	failOnError(err, "failed to connect to RabbitMQ")
	fmt.Println("Connected to Rabbit successfully")

	cli.NewPublisher([]client.ExchangeOption{
		{
			Name:    "test1",
			Type:    client.ExchangeTopic,
			Options: client.ExchangeDeclarionOptions{},
		},
	})

	cn := cli.NewConsumer(client.ConsumerParams{
		Queue:        "test-queue",
		RoutingKey:   "test",
		ExchangeName: "test1",
		AutoDelete:   false,
		Prefetch:     10,
		Callback:     procesMessage,
		RetryStrategy: &client.ConsumerRetry{
			Enabled:    true,
			MaxAttempt: 5,
			DelayFn: func(attempt int32) int32 {
				return attempt * 5000
			},
		},
		DeadletterStrategy: &client.ConsumerDeadletter{
			Enabled: true,
			CallbackFn: func(d string) bool {
				fmt.Printf("Sending to DLQ: %s\n", d)
				return false
			},
		},
	})

	go func() { cn.Begin() }()

	// pb.Publish([]client.PublishMessage{
	// 	{
	// 		Exchange:   "test1",
	// 		RoutingKey: "test",
	// 		Message:    "teste",
	// 	},
	// })

	<-ctx.Done()
	fmt.Println("Main context canceled — waiting for graceful shutdown")

	rwg.Wait()
}

func failOnError(err error, title string) {
	if err != nil {
		log.Fatalf("%s: %v", title, err)
	}
}

func procesMessage(d string) error {
	i++
	fmt.Printf("Received message: %s - %d\n", d, i)
	time.Sleep(20 * time.Second)
	fmt.Printf("Finishing processing %s - %d\n", d, i)
	return nil
}
