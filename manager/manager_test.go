package manager

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunogaldino/go-rabbit-go/amqpx"
	"github.com/brunogaldino/go-rabbit-go/client"
)

func successDialFn() func(string, amqp.Config) (amqpx.AMQPConnection, error) {
	return func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		return &mockAMQPConnection{}, nil
	}
}

func newConfigs(dialFn func(string, amqp.Config) (amqpx.AMQPConnection, error), names ...string) []ConnectionConfig {
	logger := &mockLogger{disabled: true}
	configs := make([]ConnectionConfig, len(names))
	for i, name := range names {
		configs[i] = ConnectionConfig{
			Name: name,
			Config: client.Config{
				URI:    "amqp://test:test@localhost/",
				Dialer: &mockDialer{dialFn: dialFn},
				Logger: logger,
			},
		}
	}

	return configs
}

func TestNew_CreatesClients(t *testing.T) {
	cm := New(context.Background(), newConfigs(successDialFn(), "alpha", "beta"))

	// Verify both clients are accessible.
	_ = cm.Client("alpha")
	_ = cm.Client("beta")
}

func TestConnectAll_Success(t *testing.T) {
	cm := New(context.Background(), newConfigs(successDialFn(), "primary"))
	if err := cm.ConnectAll(); err != nil {
		t.Fatalf("ConnectAll() error: %v", err)
	}
	defer cm.Disconnect()

	health := cm.CheckHealth()
	h, ok := health["primary"]
	if !ok {
		t.Fatal("expected health entry for 'primary'")
	}

	if !h.Connected {
		t.Fatal("expected 'primary' to be connected")
	}
}

func TestConnectAll_Failure(t *testing.T) {
	dialErr := errors.New("broker down")
	cm := New(context.Background(), newConfigs(
		func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
			return nil, dialErr
		},
		"fail-conn",
	))

	err := cm.ConnectAll()
	if err == nil {
		t.Fatal("expected ConnectAll to return an error")
	}

	var ce *ConnectionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ConnectionError, got %T: %v", err, err)
	}

	if ce.Name != "fail-conn" {
		t.Fatalf("expected Name %q, got %q", "fail-conn", ce.Name)
	}
}

func TestClient_Panics(t *testing.T) {
	cm := New(context.Background(), newConfigs(successDialFn(), "exists"))

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nonexistent client name")
		}
	}()

	_ = cm.Client("nonexistent")
}

func TestCheckHealth_Empty(t *testing.T) {
	cm := New(context.Background(), nil)
	health := cm.CheckHealth()
	if len(health) != 0 {
		t.Fatalf("expected empty health map, got %d entries", len(health))
	}
}

func TestDisconnect_Multiple(t *testing.T) {
	cm := New(context.Background(), newConfigs(successDialFn(), "one", "two"))
	if err := cm.ConnectAll(); err != nil {
		t.Fatalf("ConnectAll() error: %v", err)
	}

	// Disconnect should shut down all clients without panic.
	cm.Disconnect()

	health := cm.CheckHealth()
	for name, h := range health {
		if h.Connected {
			t.Fatalf("expected %q to be disconnected", name)
		}
	}
}
