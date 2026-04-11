package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ConnectionConfig struct {
	Name                 string
	URI                  string
	Heartbeat            time.Duration
	MaxReconnectAttempts int
	Exchanges            []ExchangeOption
}

type ConnectionManager struct {
	ctx     context.Context
	clients map[string]*Client
}

func NewConnectionManager(ctx context.Context, configs []ConnectionConfig) *ConnectionManager {
	cm := &ConnectionManager{
		ctx:     ctx,
		clients: make(map[string]*Client, len(configs)),
	}

	for _, cfg := range configs {
		client, _ := New(ctx, Config{
			URI:                  cfg.URI,
			Heartbeat:            cfg.Heartbeat,
			MaxReconnectAttempts: cfg.MaxReconnectAttempts,
		})
		cm.clients[cfg.Name] = client
	}

	return cm
}

func (cm *ConnectionManager) ConnectAll() error {
	for name, client := range cm.clients {
		if err := client.Connect(); err != nil {
			return fmt.Errorf("connection %q: %w", name, err)
		}
	}
	return nil
}

func (cm *ConnectionManager) Client(name string) *Client {
	c, ok := cm.clients[name]
	if !ok {
		panic(fmt.Sprintf("rabbitmq: connection %q not found", name))
	}

	return c
}

func (cm *ConnectionManager) CheckHealth() map[string]HealthStatus {
	status := make(map[string]HealthStatus, len(cm.clients))
	for name, client := range cm.clients {
		status[name] = client.CheckHealth()
	}

	return status
}

func (cm *ConnectionManager) Publisher(name string) *Publisher {
	return cm.Client(name).publisherCh
}

func (cm *ConnectionManager) NewConsumer(connName, queue string, callback func(Delivery, string, string) error, opts ...func(*Consumer)) *Consumer {
	return cm.Client(connName).NewConsumer(queue, callback, opts...)
}

func (cm *ConnectionManager) Wait() {
	for _, client := range cm.clients {
		client.wg.Wait()
	}
}

func (cm *ConnectionManager) Disconnect() {
	var wg sync.WaitGroup

	for _, client := range cm.clients {
		wg.Go(func() {
			client.Disconnect()
		})
	}

	wg.Wait()
}
