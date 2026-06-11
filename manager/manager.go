// Package manager provides a multi-connection manager for applications
// that need to publish to or consume from several independent RabbitMQ
// brokers or vhosts.
package manager

import (
	"context"
	"sync"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/client"
)

// ConnectionConfig describes a single broker connection.
type ConnectionConfig struct {
	// Name is a unique identifier for this connection.
	Name string
	// Config holds the AMQP connection parameters.
	Config client.Config
}

// ConnectionManager manages multiple [client.Client] instances, one per
// broker or vhost.
type ConnectionManager struct {
	ctx     context.Context
	clients map[string]*client.Client
	wgs     map[string]*sync.WaitGroup
	logger  rabbitmq.Logger
}

// New creates a [ConnectionManager] with one [client.Client] per entry
// in configs. Call [ConnectionManager.ConnectAll] to establish all
// connections.
func New(ctx context.Context, configs []ConnectionConfig) *ConnectionManager {
	cm := &ConnectionManager{
		ctx:     ctx,
		clients: make(map[string]*client.Client, len(configs)),
		wgs:     make(map[string]*sync.WaitGroup, len(configs)),
	}

	for _, cfg := range configs {
		c, wg := client.New(ctx, cfg.Config)
		cm.clients[cfg.Name] = c
		cm.wgs[cfg.Name] = wg
		if cm.logger == nil && cfg.Config.Logger != nil {
			cm.logger = cfg.Config.Logger
		}
	}

	if cm.logger == nil {
		cm.logger = rabbitmq.NewDefaultLogger()
	}

	return cm
}

// ConnectAll validates the configuration of every managed client.
// Connections are dialed lazily by each client when its first
// publisher or consumer is created.
func (cm *ConnectionManager) ConnectAll() error {
	for name, c := range cm.clients {
		if err := c.Connect(); err != nil {
			return &ConnectionError{Name: name, Err: err}
		}
	}

	return nil
}

// Client returns the [client.Client] registered under name.
// It panics if the name is not found.
func (cm *ConnectionManager) Client(name string) *client.Client {
	c, ok := cm.clients[name]
	if !ok {
		panic("rabbitmq: connection " + name + " not found")
	}

	return c
}

// CheckHealth returns a per-connection [client.HealthStatus] map.
func (cm *ConnectionManager) CheckHealth() map[string]client.HealthStatus {
	status := make(map[string]client.HealthStatus, len(cm.clients))
	for name, c := range cm.clients {
		status[name] = c.CheckHealth()
	}

	return status
}

// Wait blocks until all managed clients have finished shutting down.
func (cm *ConnectionManager) Wait() {
	for _, wg := range cm.wgs {
		wg.Wait()
	}
}

// Disconnect gracefully shuts down all managed clients in parallel.
func (cm *ConnectionManager) Disconnect() {
	var wg sync.WaitGroup
	for _, c := range cm.clients {
		wg.Go(func() {
			c.Disconnect()
		})
	}
	wg.Wait()
}
