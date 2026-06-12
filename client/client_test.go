package client

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
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

	// The dialer is stored but not called until the first channel request.
	if c.dialer != d {
		t.Fatal("expected Client to use the provided custom dialer")
	}

	_ = c.Connect()
	if called {
		t.Fatal("expected Connect not to dial")
	}

	if _, err := c.Channel(); err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	if !called {
		t.Fatal("expected custom dialer to be called on first channel request")
	}
}

func TestConnect_DoesNotDial(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		return &mockAMQPConnection{}, nil
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	if dialCount != 0 {
		t.Fatalf("expected 0 dial calls after Connect, got %d", dialCount)
	}
}

func TestConnect_InvalidURI(t *testing.T) {
	ctx := context.Background()
	c, _ := New(ctx, Config{
		URI:    "not-a-valid-uri",
		Dialer: &mockDialer{dialFn: successDialFn()},
		Logger: &mockLogger{disabled: true},
	})

	if err := c.Connect(); err == nil {
		t.Fatal("expected Connect to fail for an invalid URI")
	}
}

func TestLazyDial_ConsumerRoleOnly(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		return &mockAMQPConnection{}, nil
	})
	defer c.Disconnect()

	if _, err := c.Channel(); err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	// Repeated channel requests must not dial again.
	if _, err := c.Channel(); err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	if dialCount != 1 {
		t.Fatalf("expected 1 dial call (consumer only), got %d", dialCount)
	}

	h := c.CheckHealth()
	if !h.Connected {
		t.Fatal("expected Connected true when the only requested role is up")
	}

	if !h.ConsumerConnected {
		t.Fatal("expected ConsumerConnected true")
	}

	if h.PublisherConnected {
		t.Fatal("expected PublisherConnected false for an unused role")
	}
}

func TestLazyDial_PublisherRoleOnly(t *testing.T) {
	var dialCount int
	var dialSuffix string
	c, _ := newTestClient(t, func(_ string, cfg amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		dialSuffix, _ = cfg.Properties[amqpx.KeyConnectionName].(string)
		return &mockAMQPConnection{}, nil
	})
	defer c.Disconnect()

	pc := c.PublisherConn()
	if _, err := pc.Channel(); err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	if dialCount != 1 {
		t.Fatalf("expected 1 dial call (publisher only), got %d", dialCount)
	}

	if !strings.HasSuffix(dialSuffix, amqpx.SuffixPublisher) {
		t.Fatalf("expected publisher connection name, got %q", dialSuffix)
	}

	h := c.CheckHealth()
	if !h.Connected {
		t.Fatal("expected Connected true when the only requested role is up")
	}

	if !h.PublisherConnected {
		t.Fatal("expected PublisherConnected true")
	}

	if h.ConsumerConnected {
		t.Fatal("expected ConsumerConnected false for an unused role")
	}
}

func TestLazyDial_BothRoles(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		return &mockAMQPConnection{}, nil
	})
	defer c.Disconnect()

	if _, err := c.Channel(); err != nil {
		t.Fatalf("consumer Channel() error: %v", err)
	}

	if _, err := c.PublisherConn().Channel(); err != nil {
		t.Fatalf("publisher Channel() error: %v", err)
	}

	if dialCount != 2 {
		t.Fatalf("expected 2 dial calls (pub+con), got %d", dialCount)
	}

	h := c.CheckHealth()
	if !h.Connected || !h.PublisherConnected || !h.ConsumerConnected {
		t.Fatalf("expected both roles connected, got %+v", h)
	}
}

func TestLazyDial_Concurrent_SingleDial(t *testing.T) {
	var dialCount atomic.Int32
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount.Add(1)
		return &mockAMQPConnection{}, nil
	})
	defer c.Disconnect()

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for range 10 {
		wg.Go(func() {
			if _, err := c.Channel(); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent Channel() error: %v", err)
	}

	if n := dialCount.Load(); n != 1 {
		t.Fatalf("expected exactly 1 dial across concurrent calls, got %d", n)
	}
}

func TestLazyDial_PublisherDialFails(t *testing.T) {
	dialErr := errors.New("connection refused")
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		return nil, dialErr
	})

	_, err := c.PublisherConn().Channel()
	if err == nil {
		t.Fatal("expected Channel to return an error")
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

func TestLazyDial_ConsumerDialFails(t *testing.T) {
	dialErr := errors.New("connection refused")
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		return nil, dialErr
	})

	_, err := c.Channel()
	if err == nil {
		t.Fatal("expected Channel to return an error")
	}

	var de *DialError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DialError, got %T: %v", err, err)
	}

	if de.Role != amqpx.SuffixConsumer {
		t.Fatalf("expected Role %q, got %q", amqpx.SuffixConsumer, de.Role)
	}
}

func TestLazyDial_RetriesAfterInitialFailure(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		if dialCount == 1 {
			return nil, errors.New("broker down")
		}
		return &mockAMQPConnection{}, nil
	})
	defer c.Disconnect()

	if _, err := c.Channel(); err == nil {
		t.Fatal("expected first Channel() to fail")
	}

	if _, err := c.Channel(); err != nil {
		t.Fatalf("expected second Channel() to retry the dial and succeed: %v", err)
	}

	if dialCount != 2 {
		t.Fatalf("expected 2 dial attempts, got %d", dialCount)
	}
}

func TestChannel_AfterDisconnect_NoRedial(t *testing.T) {
	var dialCount int
	c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
		dialCount++
		return &mockAMQPConnection{}, nil
	})

	c.Disconnect()

	if _, err := c.Channel(); !errors.Is(err, rabbitmq.ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}

	if _, err := c.PublisherConn().Channel(); !errors.Is(err, rabbitmq.ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}

	if dialCount != 0 {
		t.Fatalf("expected no dial after Disconnect, got %d", dialCount)
	}
}

func TestChannel_AfterContextCancel_NoDial(t *testing.T) {
	var dialCount int
	ctx, cancel := context.WithCancel(context.Background())
	c, _ := New(ctx, Config{
		URI: "amqp://test:test@localhost/",
		Dialer: &mockDialer{dialFn: func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
			dialCount++
			return &mockAMQPConnection{}, nil
		}},
		Logger: &mockLogger{disabled: true},
	})

	cancel()

	if _, err := c.Channel(); !errors.Is(err, rabbitmq.ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed after ctx cancel, got %v", err)
	}

	if _, err := c.PublisherConn().Channel(); !errors.Is(err, rabbitmq.ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed after ctx cancel, got %v", err)
	}

	if dialCount != 0 {
		t.Fatalf("expected no dial after ctx cancel, got %d", dialCount)
	}
}

func TestDisconnect_ConcurrentWithLazyDial_NoLeak(t *testing.T) {
	for range 50 {
		var dialed, closed atomic.Bool
		c, _ := newTestClient(t, func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
			dialed.Store(true)
			return &mockAMQPConnection{
				closeFn: func() error {
					closed.Store(true)
					return nil
				},
			}, nil
		})

		var wg sync.WaitGroup
		wg.Go(func() { _, _ = c.Channel() })
		wg.Go(func() { c.Disconnect() })
		wg.Wait()

		// Whichever side wins the race, a dialed connection must never
		// survive Disconnect.
		if dialed.Load() && !closed.Load() {
			t.Fatal("connection dialed concurrently with Disconnect was leaked")
		}
	}
}

func TestCheckHealth_Defaults(t *testing.T) {
	c, _ := newTestClient(t, successDialFn())

	h := c.CheckHealth()
	// No role was requested yet, so nothing can be "down".
	if !h.Connected {
		t.Fatal("expected Connected true when no role has been requested")
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

	// Establish the consumer connection so Disconnect has work to do.
	if _, err := c.Channel(); err != nil {
		t.Fatalf("Channel() error: %v", err)
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

// dropConnectionsClient builds a client whose initial dials succeed and
// whose redials always fail, capturing the NotifyError channels so the
// test can simulate the broker dropping the connections (amqp091-go
// sends the close error and then closes the registered channel).
func dropConnectionsClient(t *testing.T, maxReconnect int) (*Client, *sync.WaitGroup, *atomic.Int32, []chan *amqp.Error) {
	t.Helper()

	var dials atomic.Int32
	notify := make(chan chan *amqp.Error, 2)

	c, wg := New(context.Background(), Config{
		URI:                  "amqp://test:test@localhost/",
		MaxReconnectAttempts: maxReconnect,
		Logger:               &mockLogger{disabled: true},
		Dialer: &mockDialer{dialFn: func(_ string, _ amqp.Config) (amqpx.AMQPConnection, error) {
			if dials.Add(1) <= 2 {
				return &mockAMQPConnection{
					notifyCloseFn: func(ch chan *amqp.Error) chan *amqp.Error {
						notify <- ch
						return ch
					},
				}, nil
			}
			return nil, errors.New("broker still down")
		}},
	})

	if err := c.Connect(); err != nil {
		t.Fatalf("Connect() error: %v", err)
	}

	// With lazy dialing, Connect() no longer dials. Trigger both
	// connections by requesting a channel for each role so the test
	// can capture their NotifyError channels.
	if _, err := c.PublisherConn().Channel(); err != nil {
		t.Fatalf("publisher Channel() error: %v", err)
	}

	if _, err := c.Channel(); err != nil {
		t.Fatalf("consumer Channel() error: %v", err)
	}

	// publisher dials first (PublisherConn.Channel above), then consumer.
	pubNotify := <-notify
	conNotify := <-notify

	return c, wg, &dials, []chan *amqp.Error{pubNotify, conNotify}
}

func dropConn(ch chan *amqp.Error) {
	ch <- &amqp.Error{Code: amqp.ConnectionForced, Reason: "test drop", Recover: true}
	close(ch)
}

func waitOrFail(t *testing.T, wg *sync.WaitGroup, msg string) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(msg)
	}
}

func TestMonitors_ExitAfterMaxReconnectAttempts(t *testing.T) {
	oldDelay := reconnectDelay
	reconnectDelay = 10 * time.Millisecond
	defer func() { reconnectDelay = oldDelay }()

	c, wg, dials, notify := dropConnectionsClient(t, 1)

	dropConn(notify[0])
	dropConn(notify[1])

	// Before the fix the monitors spun forever on the closed NotifyError
	// channels and wg.Wait() never returned.
	waitOrFail(t, wg, "monitors did not exit after exhausting reconnect attempts (hot loop)")

	// 2 initial dials + 1 failed redial per role.
	if n := dials.Load(); n != 4 {
		t.Fatalf("expected 4 dials (2 initial + 2 failed redials), got %d", n)
	}

	h := c.CheckHealth()
	if h.PublisherConnected || h.ConsumerConnected {
		t.Fatalf("expected both connections down, got %+v", h)
	}

	if h.Reconnecting {
		t.Fatal("expected Reconnecting false after giving up")
	}
}

func TestMonitors_ExitImmediately_WhenMaxReconnectAttemptsIsZero(t *testing.T) {
	oldDelay := reconnectDelay
	reconnectDelay = 10 * time.Millisecond
	defer func() { reconnectDelay = oldDelay }()

	c, wg, dials, notify := dropConnectionsClient(t, 0)

	dropConn(notify[0])
	dropConn(notify[1])

	waitOrFail(t, wg, "monitors did not exit with MaxReconnectAttempts=0 (hot loop)")

	// No redial may happen: only the 2 initial dials.
	if n := dials.Load(); n != 2 {
		t.Fatalf("expected 2 dials (no redials), got %d", n)
	}

	if h := c.CheckHealth(); h.Reconnecting {
		t.Fatal("expected Reconnecting false after giving up")
	}
}

func TestMonitors_ExhaustsAllAttempts_BothRoles(t *testing.T) {
	// Validates that BOTH roles exhaust their full MaxReconnectAttempts
	// before giving up — not just the first failed attempt.
	oldDelay := reconnectDelay
	reconnectDelay = 5 * time.Millisecond
	defer func() { reconnectDelay = oldDelay }()

	const maxAttempts = 3

	c, wg, dials, notify := dropConnectionsClient(t, maxAttempts)

	dropConn(notify[0])
	dropConn(notify[1])

	waitOrFail(t, wg, "monitors did not exit after exhausting all reconnect attempts")

	// 2 initial + maxAttempts redials per role = 2 + 2*maxAttempts
	want := int32(2 + 2*maxAttempts)
	if n := dials.Load(); n != want {
		t.Fatalf("expected %d total dials (2 initial + %d redials each role), got %d",
			want, maxAttempts, n)
	}

	h := c.CheckHealth()
	if h.PublisherConnected || h.ConsumerConnected {
		t.Fatalf("expected both connections down after give-up, got %+v", h)
	}

	if h.Reconnecting {
		t.Fatal("expected Reconnecting false after giving up")
	}
}
