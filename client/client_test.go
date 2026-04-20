package client

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/brunogaldino/go-rabbit-go/amqpx"
	"github.com/brunogaldino/go-rabbit-go/consumer"
	"github.com/brunogaldino/go-rabbit-go/publisher"
)

// newTestClient creates a Client with a mock dialer and silent logger.
func newTestClient(t *testing.T, dialFn func(string, amqp.Config) (amqpx.AMQPConnection, error)) (*Client, *sync.WaitGroup) {
	t.Helper()
	ctx := context.Background()
	return New(ctx, Config{
		URI:       "amqp://test:test@localhost/",
		Dialer:    &mockDialer{dialFn: dialFn},
		Logger:    &mockLogger{disabled: true},
		Heartbeat: 0,
	})
}

func successDialFn() func(string, amqp.Config) (amqpx.AMQPConnection, error) {
	return func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		return &mockAMQPConnection{}, nil
	}
}

// --- Tests ---

func TestNew_ReturnsClientAndWaitGroup(t *testing.T) {
	c, wg := newTestClient(t, successDialFn())
	if c == nil {
		t.Fatal("expected non-nil Client")
	}

	if wg == nil {
		t.Fatal("expected non-nil WaitGroup")
	}
}

func TestNew_UsesCustomLogger(t *testing.T) {
	logger := &mockLogger{}
	ctx := context.Background()
	c, _ := New(ctx, Config{
		URI:    "amqp://localhost/",
		Dialer: &mockDialer{dialFn: successDialFn()},
		Logger: logger,
	})

	if c.logger != logger {
		t.Fatal("expected Client to use the provided custom logger")
	}
}

func TestNew_UsesCustomDialer(t *testing.T) {
	called := false
	d := &mockDialer{dialFn: func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		called = true
		return &mockAMQPConnection{}, nil
	}}

	ctx := context.Background()
	c, _ := New(ctx, Config{
		URI:    "amqp://localhost/",
		Dialer: d,
		Logger: &mockLogger{disabled: true},
	})

	// The dialer is stored but not called until Connect().
	if c.dialer != d {
		t.Fatal("expected Client to use the provided custom dialer")
	}

	_ = c.Connect()
	if !called {
		t.Fatal("expected custom dialer to be called during Connect")
	}
}

func TestConnect_Success(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		return &mockAMQPConnection{}, nil
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	defer c.Disconnect()

	if dialCount != 2 {
		t.Fatalf("expected 2 dial calls (pub+con), got %d", dialCount)
	}

	h := c.CheckHealth()
	if !h.Connected {
		t.Fatal("expected Connected to be true after Connect")
	}

	if !h.PublisherConnected {
		t.Fatal("expected PublisherConnected to be true")
	}

	if !h.ConsumerConnected {
		t.Fatal("expected ConsumerConnected to be true")
	}
}

func TestConnect_PublisherDialFails(t *testing.T) {
	dialErr := errors.New("connection refused")
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		return nil, dialErr
	})

	err := c.Connect()
	if err == nil {
		t.Fatal("expected Connect to return an error")
	}

	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DialError, got %T: %v", err, err)
	}

	if de.Role != amqpx.SuffixPublisher {
		t.Fatalf("expected Role %q, got %q", amqpx.SuffixPublisher, de.Role)
	}

	if !errors.Is(err, dialErr) {
		t.Fatalf("expected wrapped error to be %v", dialErr)
	}
}

func TestConnect_ConsumerDialFails_ClosesPublisher(t *testing.T) {
	pubClosed := false
	callNum := 0
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		callNum++
		if callNum == 1 {
			// Publisher dial succeeds.
			return &mockAMQPConnection{
				closeFn: func() error {
					pubClosed = true
					return nil
				},
			}, nil
		}
		// Consumer dial fails.
		return nil, errors.New("consumer dial error")
	})

	err := c.Connect()
	if err == nil {
		t.Fatal("expected Connect to return an error")
	}

	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DialError, got %T", err)
	}

	if de.Role != amqpx.SuffixConsumer {
		t.Fatalf("expected Role %q, got %q", amqpx.SuffixConsumer, de.Role)
	}

	if !pubClosed {
		t.Fatal("expected publisher connection to be closed when consumer dial fails")
	}
}

func TestCheckHealth_Defaults(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())

	h := c.CheckHealth()
	if h.Connected {
		t.Fatal("expected Connected false before Connect")
	}

	if h.PublisherConnected {
		t.Fatal("expected PublisherConnected false")
	}

	if h.ConsumerConnected {
		t.Fatal("expected ConsumerConnected false")
	}

	if h.Blocked {
		t.Fatal("expected Blocked false")
	}

	if h.Reconnecting {
		t.Fatal("expected Reconnecting false")
	}
}

func TestDisconnect_Idempotent(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	// First disconnect.
	c.Disconnect()
	// Second disconnect should be a no-op.
	c.Disconnect()

	h := c.CheckHealth()
	if h.Connected {
		t.Fatal("expected Connected false after Disconnect")
	}
}

func TestClient_ImplementsConsumerConnProvider(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())
	var _ consumer.ConnProvider = c
}

func TestClient_PublisherConn(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())
	pc := c.PublisherConn()
	if pc == nil {
		t.Fatal("expected PublisherConn() to return non-nil")
	}

	var _ publisher.ConnProvider = pc
}

func TestClient_RegisterUnregisterConsumer(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())

	// Use a nil consumer pointer just to test map registration.
	c.RegisterConsumer("test-consumer", nil)

	c.mu.Lock()
	_, ok := c.consumerMap["test-consumer"]
	c.mu.Unlock()
	if !ok {
		t.Fatal("expected consumer to be registered")
	}

	c.UnregisterConsumer("test-consumer")

	c.mu.Lock()
	_, ok = c.consumerMap["test-consumer"]
	c.mu.Unlock()
	if ok {
		t.Fatal("expected consumer to be unregistered")
	}
}
