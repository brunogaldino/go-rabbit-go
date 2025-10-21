package main

import (
	"context"
	"errors"
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
	failOnError(err, "failed to connect to RabbitMQ")

	go func() {
		sig := <-stopChan
		fmt.Println("\nCapturou o sinal", sig)
		cancel()
	}()

	fmt.Println("Connected to Rabbit successfully")

	pb := cli.NewPublisher([]client.ExchangeOption{
		{
			Name:    "test1",
			Type:    client.ExchangeTopic,
			Options: client.ExchangeDeclarionOptions{},
		},
	})

	cn := cli.NewConsumer("test-queue", procesMessage,
		client.WithPrefetch(10),
		client.WithExchangeName("test1"),
		client.WithRoutingKey([]string{"teste-rk"}),
		client.WithRetryFn(func(attempt int32) int32 {
			return attempt * 5000
		}),
	)

	go func() { cn.Begin() }()

	pb.Publish([]client.PublishMessage{
		{
			Exchange:   "test1",
			RoutingKey: "test",
			Message:    "teste",
		},
	})

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
	time.Sleep(5 * time.Second)
	fmt.Printf("Finishing processing %s - %d\n", d, i)
	return nil
}
