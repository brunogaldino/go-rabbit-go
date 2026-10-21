package consumer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

// newTestConsumer builds a Consumer with sensible defaults for white-box tests,
// bypassing New() so that setup() is not called.
func newTestConsumer(ch amqpx.AMQPChannel, conn ConnProvider) *Consumer {
	retryCopy := *defaults.RetryStrategy
	dlCopy := *defaults.DeadletterStrategy
	defs := *defaults
	defs.RetryStrategy = &retryCopy
	defs.DeadletterStrategy = &dlCopy

	c := &Consumer{
		conn:         conn,
		channel:      ch,
		params:       defs,
		consumerName: "test-consumer",
		drainTimeout: defaultDrainTimeout,
	}
	c.params.Queue = "test-queue"
	c.params.ExchangeName = "test-exchange"
	c.params.RoutingKey = []string{"test.key"}
	c.params.Callback = func(d Delivery, queue string) error { return nil }
	c.params.DeadletterStrategy.DLQueueName = "test-queue" + amqpx.SuffixDLQ
	return c
}

// testDelivery builds a Delivery from an amqp.Delivery, copying fields the
// same way the production range loop does.
func testDelivery(d amqp.Delivery) Delivery {
	return Delivery{
		delivery:      d,
		Body:          d.Body,
		Headers:       d.Headers,
		RoutingKey:    d.RoutingKey,
		ContentType:   d.ContentType,
		CorrelationId: d.CorrelationId,
	}
}

// --- New ---

func TestNew_Success(t *testing.T) {
	registered := false
	conn := &mockConnProvider{
		registerConsumerFn: func(_ string, _ *Consumer) { registered = true },
	}

	c, err := New(conn, "my-queue", func(d Delivery, q string) error { return nil },
		WithExchangeName("ex"), WithRoutingKey([]string{"rk"}))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if c == nil {
		t.Fatal("expected non-nil consumer")
	}

	if !registered {
		t.Fatal("expected RegisterConsumer to be called")
	}

	if c.params.Queue != "my-queue" {
		t.Fatalf("expected queue 'my-queue', got %q", c.params.Queue)
	}
}

func TestNew_ChannelError(t *testing.T) {
	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) {
			return nil, errors.New("dial failed")
		},
	}

	c, err := New(conn, "q", func(d Delivery, q string) error { return nil })
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if c != nil {
		t.Fatal("expected nil consumer on error")
	}

	var chErr *rabbitmq.ChannelError
	if !errors.As(err, &chErr) {
		t.Fatalf("expected ChannelError, got %T: %v", err, err)
	}

	if chErr.Operation != opChannelOpen {
		t.Fatalf("expected operation %q, got %q", opChannelOpen, chErr.Operation)
	}
}

// --- setup errors ---

func TestSetup_QoSError(t *testing.T) {
	ch := &mockAMQPChannel{
		qosFn: func(int, int, bool) error { return errors.New("qos fail") },
	}

	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	_, err := New(conn, "q", func(d Delivery, q string) error { return nil })
	if err == nil {
		t.Fatal("expected error")
	}

	var chErr *rabbitmq.ChannelError
	if !errors.As(err, &chErr) {
		t.Fatalf("expected ChannelError, got %T: %v", err, err)
	}

	if chErr.Operation != opChannelQos {
		t.Fatalf("expected operation %q, got %q", opChannelQos, chErr.Operation)
	}
}

func TestSetup_QueueDeclareError(t *testing.T) {
	callCount := 0
	ch := &mockAMQPChannel{
		queueDeclareFn: func(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
			callCount++
			// First call is DLQ declare (succeeds), second is main queue (fails).
			if callCount == 2 {
				return amqp.Queue{}, errors.New("declare fail")
			}
			return amqp.Queue{}, nil
		},
	}

	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	_, err := New(conn, "q", func(d Delivery, q string) error { return nil },
		WithRoutingKey([]string{"rk"}), WithExchangeName("ex"))
	if err == nil {
		t.Fatal("expected error")
	}

	var qErr *QueueError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected QueueError, got %T: %v", err, err)
	}

	if qErr.Operation != opQueueDeclare {
		t.Fatalf("expected operation %q, got %q", opQueueDeclare, qErr.Operation)
	}
}

func TestSetup_BindError(t *testing.T) {
	ch := &mockAMQPChannel{
		queueBindFn: func(string, string, string, bool, amqp.Table) error {
			return errors.New("bind fail")
		},
	}

	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	_, err := New(conn, "q", func(d Delivery, q string) error { return nil },
		WithRoutingKey([]string{"rk"}), WithExchangeName("ex"))
	if err == nil {
		t.Fatal("expected error")
	}

	var qErr *QueueError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected QueueError, got %T: %v", err, err)
	}

	if qErr.Operation != opQueueBind {
		t.Fatalf("expected operation %q, got %q", opQueueBind, qErr.Operation)
	}
}

// --- processDelivery ---

func TestProcessDelivery_Success(t *testing.T) {
	called := false
	acked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.Callback = func(d Delivery, q string) error {
		called = true
		return nil
	}

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("hello"),
	})

	c.processDelivery(d)

	if !called {
		t.Fatal("expected callback to be called")
	}

	if !acked {
		t.Fatal("expected message to be acked")
	}
}

func TestProcessDelivery_Error_TriggersRetry(t *testing.T) {
	published := false
	acked := false
	ch := &mockAMQPChannel{
		publishFn: func(_, key string, _, _ bool, msg amqp.Publishing) error {
			published = true
			if key != "test-queue"+amqpx.SuffixRetry {
				t.Fatalf("expected publish to retry queue, got key %q", key)
			}
			return nil
		},
	}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.Callback = func(d Delivery, q string) error {
		return errors.New("processing error")
	}

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body:    []byte("hello"),
		Headers: amqp.Table{},
	})

	c.processDelivery(d)

	if !published {
		t.Fatal("expected retry publish")
	}

	if !acked {
		t.Fatal("expected original message to be acked after retry publish")
	}
}

func TestProcessDelivery_Panic_Recovers(t *testing.T) {
	published := false
	ch := &mockAMQPChannel{
		publishFn: func(string, string, bool, bool, amqp.Publishing) error {
			published = true
			return nil
		},
	}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.Callback = func(d Delivery, q string) error {
		panic("kaboom")
	}

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{},
		Body:         []byte("data"),
		Headers:      amqp.Table{},
	})

	// Should not panic the test.
	c.processDelivery(d)

	if !published {
		t.Fatal("expected retry publish after panic recovery")
	}
}

// --- retry ---

func TestRetry_ExhaustedGoesToDeadletter(t *testing.T) {
	nacked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body: []byte("exhausted"),
		Headers: amqp.Table{
			amqpx.KeyRetriesCount: int32(defaults.RetryStrategy.MaxAttempt),
		},
	})

	c.retry(d, errors.New("some error"))

	if !nacked {
		t.Fatal("expected message to be nacked (sent to DLQ) after retries exhausted")
	}
}

func TestRetry_DisabledGoesToDeadletter(t *testing.T) {
	nacked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.RetryStrategy.Enabled = false

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body:    []byte("no-retry"),
		Headers: amqp.Table{},
	})

	c.retry(d, errors.New("fail"))

	if !nacked {
		t.Fatal("expected nack (DLQ) when retry is disabled")
	}
}

// --- deadletter ---

func TestDeadletter_Disabled_AcksMessage(t *testing.T) {
	acked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.DeadletterStrategy.Enabled = false

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("drop"),
	})

	c.deadletter(d)

	if !acked {
		t.Fatal("expected ack when DLQ disabled")
	}
}

func TestDeadletter_CallbackReturnsFalse_AcksMessage(t *testing.T) {
	acked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.DeadletterStrategy.DLQFunc = func(body Delivery) bool {
		return false
	}

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("skip-dlq"),
	})

	c.deadletter(d)

	if !acked {
		t.Fatal("expected ack when DLQ callback returns false")
	}
}

func TestDeadletter_CallbackPanics_Nacks(t *testing.T) {
	nacked := false
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.DeadletterStrategy.DLQFunc = func(body Delivery) bool {
		panic("callback panic")
	}

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body: []byte("panic-body"),
	})

	// Should not panic the test.
	c.sendToDeadletter(d)

	if !nacked {
		t.Fatal("expected nack when DLQ callback panics")
	}
}

// --- Disconnect ---

func TestDisconnect(t *testing.T) {
	cancelled := false
	closed := false
	unregistered := false

	ch := &mockAMQPChannel{
		cancelFn: func(string, bool) error { cancelled = true; return nil },
		closeFn:  func() error { closed = true; return nil },
	}
	conn := &mockConnProvider{
		unregisterConsumerFn: func(string) { unregistered = true },
	}
	c := newTestConsumer(ch, conn)

	c.Disconnect()

	if !c.closing.Load() {
		t.Fatal("expected closing flag to be set")
	}

	if !cancelled {
		t.Fatal("expected channel Cancel to be called")
	}

	if !closed {
		t.Fatal("expected channel Close to be called")
	}

	if !unregistered {
		t.Fatal("expected UnregisterConsumer to be called")
	}
}

// --- consume ---

func TestConsume_WithMessages(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 2)
	deliveries <- amqp.Delivery{
		Acknowledger: &mockAcknowledger{},
		Body:         []byte("msg1"),
		Headers:      amqp.Table{},
	}
	deliveries <- amqp.Delivery{
		Acknowledger: &mockAcknowledger{},
		Body:         []byte("msg2"),
		Headers:      amqp.Table{},
	}
	close(deliveries)

	var count atomic.Int32
	ch := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			return deliveries, nil
		},
	}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)
	c.params.Callback = func(d Delivery, q string) error {
		count.Add(1)
		return nil
	}

	c.consume()
	c.wg.Wait()

	if got := count.Load(); got != 2 {
		t.Fatalf("expected 2 messages processed, got %d", got)
	}
}

// --- Delivery helpers ---

func TestDelivery_GetHeader(t *testing.T) {
	d := testDelivery(amqp.Delivery{
		Headers: amqp.Table{"foo": "bar"},
	})

	if got := d.GetHeader("foo"); got != "bar" {
		t.Fatalf("expected 'bar', got %v", got)
	}

	if got := d.GetHeader("missing"); got != nil {
		t.Fatalf("expected nil for missing header, got %v", got)
	}

	// nil headers
	d2 := testDelivery(amqp.Delivery{})
	if got := d2.GetHeader("any"); got != nil {
		t.Fatalf("expected nil for nil headers, got %v", got)
	}
}

func TestDelivery_GetRoutingKey(t *testing.T) {
	// With original routing key header.
	d := testDelivery(amqp.Delivery{
		RoutingKey: "current-rk",
		Headers:    amqp.Table{amqpx.KeyOriginalRouteKey: "original-rk"},
	})

	if got := d.GetRoutingKey(); got != "original-rk" {
		t.Fatalf("expected 'original-rk', got %q", got)
	}

	// Without header — falls back to RoutingKey.
	d2 := testDelivery(amqp.Delivery{
		RoutingKey: "current-rk",
		Headers:    amqp.Table{},
	})

	if got := d2.GetRoutingKey(); got != "current-rk" {
		t.Fatalf("expected 'current-rk', got %q", got)
	}
}

// --- Functional options ---

func TestFunctionalOptions(t *testing.T) {
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)

	// WithPrefetch
	WithPrefetch(42)(c)
	if c.params.Prefetch != 42 {
		t.Fatalf("expected prefetch 42, got %d", c.params.Prefetch)
	}

	// WithAutoDelete
	WithAutoDelete()(c)
	if !c.params.AutoDelete {
		t.Fatal("expected AutoDelete true")
	}

	// WithRetryDisabled
	WithRetryDisabled()(c)
	if c.params.RetryStrategy.Enabled {
		t.Fatal("expected retry disabled")
	}

	// WithRetryMaxAttempt
	WithRetryMaxAttempt(10)(c)
	if c.params.RetryStrategy.MaxAttempt != 10 {
		t.Fatalf("expected max attempt 10, got %d", c.params.RetryStrategy.MaxAttempt)
	}

	// WithRetryFn
	customFn := func(d Delivery, attempt int32, err error) int32 { return 999 }
	WithRetryFn(customFn)(c)
	if c.params.RetryStrategy.RetryFunc(Delivery{}, 1, nil) != 999 {
		t.Fatal("expected custom delay fn to return 999")
	}

	// WithDLQFn
	dlqCalled := false
	WithDLQFn(func(Delivery) bool { dlqCalled = true; return true })(c)
	c.params.DeadletterStrategy.DLQFunc(Delivery{})
	if !dlqCalled {
		t.Fatal("expected DLQ callback to be set and called")
	}

	// WithRoutingKey
	WithRoutingKey([]string{"a", "b"})(c)
	if len(c.params.RoutingKey) != 2 || c.params.RoutingKey[0] != "a" {
		t.Fatalf("expected routing keys [a b], got %v", c.params.RoutingKey)
	}

	// WithExchangeName
	WithExchangeName("my-exc")(c)
	if c.params.ExchangeName != "my-exc" {
		t.Fatalf("expected exchange 'my-exc', got %q", c.params.ExchangeName)
	}

	// WithHeadersBinding
	WithHeadersBinding(map[string]any{"x-match": "all"})(c)
	if c.params.HeadersBinding["x-match"] != "all" {
		t.Fatal("expected headers binding to contain x-match=all")
	}
}

// --- Edge cases ---

func TestConsume_ConsumeError(t *testing.T) {
	ch := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			return nil, errors.New("consume error")
		},
	}

	logged := false
	conn := &mockConnProvider{
		loggerFn: func() rabbitmq.Logger {
			return &mockLogger{
				errorFn: func(msg string, data ...map[string]any) {
					logged = true
				},
			}
		},
	}
	c := newTestConsumer(ch, conn)

	c.consume()
	c.wg.Wait()

	if !logged {
		t.Fatal("expected error to be logged when Consume fails")
	}
}

func TestSetup_DLQDeclareError(t *testing.T) {
	ch := &mockAMQPChannel{
		queueDeclareFn: func(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
			if name == "q"+amqpx.SuffixDLQ {
				return amqp.Queue{}, errors.New("dlq declare fail")
			}
			return amqp.Queue{}, nil
		},
	}

	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	_, err := New(conn, "q", func(d Delivery, q string) error { return nil })
	if err == nil {
		t.Fatal("expected error")
	}

	var qErr *QueueError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected QueueError, got %T: %v", err, err)
	}

	if qErr.Operation != opDeclareDLQ {
		t.Fatalf("expected operation %q, got %q", opDeclareDLQ, qErr.Operation)
	}
}

func TestSetup_RetryQueueDeclareError(t *testing.T) {
	callCount := 0
	ch := &mockAMQPChannel{
		queueDeclareFn: func(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
			callCount++
			// 1=DLQ, 2=main queue, 3=retry queue
			if callCount == 3 {
				return amqp.Queue{}, errors.New("retry declare fail")
			}
			return amqp.Queue{}, nil
		},
	}

	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	_, err := New(conn, "q", func(d Delivery, q string) error { return nil },
		WithRoutingKey([]string{"rk"}), WithExchangeName("ex"))
	if err == nil {
		t.Fatal("expected error")
	}

	var qErr *QueueError
	if !errors.As(err, &qErr) {
		t.Fatalf("expected QueueError, got %T: %v", err, err)
	}

	if qErr.Operation != opDeclareRetry {
		t.Fatalf("expected operation %q, got %q", opDeclareRetry, qErr.Operation)
	}
}

func TestPublishRetry_PublishError_NacksMessage(t *testing.T) {
	nacked := false
	ch := &mockAMQPChannel{
		publishFn: func(string, string, bool, bool, amqp.Publishing) error {
			return errors.New("publish fail")
		},
	}

	conn := &mockConnProvider{}
	c := newTestConsumer(ch, conn)

	d := testDelivery(amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body:    []byte("data"),
		Headers: amqp.Table{},
	})

	c.publishRetry(d, 1, 1, errors.New("err"))

	if !nacked {
		t.Fatal("expected nack when retry publish fails")
	}
}

func TestQueueError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("inner")
	qErr := &QueueError{Operation: opQueueDeclare, Queue: "q1", Err: inner}

	got := qErr.Error()
	want := fmt.Sprintf("rabbitmq: queue declare q1 failed: %v", inner)
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
	if qErr.Unwrap() != inner {
		t.Fatal("Unwrap should return inner error")
	}
}

func TestDisconnect_Idempotent_Concurrent(t *testing.T) {
	var cancelCalls, closeCalls, unregisterCalls atomic.Int32

	ch := &mockAMQPChannel{
		cancelFn: func(string, bool) error { cancelCalls.Add(1); return nil },
		closeFn:  func() error { closeCalls.Add(1); return nil },
	}
	conn := &mockConnProvider{
		unregisterConsumerFn: func(string) { unregisterCalls.Add(1) },
	}
	c := newTestConsumer(ch, conn)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { c.Disconnect() })
	}
	wg.Wait()

	if n := cancelCalls.Load(); n != 1 {
		t.Fatalf("expected Cancel called once, got %d", n)
	}
	if n := closeCalls.Load(); n != 1 {
		t.Fatalf("expected Close called once, got %d", n)
	}
	if n := unregisterCalls.Load(); n != 1 {
		t.Fatalf("expected UnregisterConsumer called once, got %d", n)
	}
}

func TestDisconnect_WaitsForInflightBeforeClose(t *testing.T) {
	var closed atomic.Bool
	var ackedBeforeClose atomic.Bool

	ch := &mockAMQPChannel{
		cancelFn: func(string, bool) error { return nil },
		closeFn:  func() error { closed.Store(true); return nil },
	}
	c := newTestConsumer(ch, &mockConnProvider{})

	// Simulate an in-flight handler that finishes shortly; it must
	// complete (recording ack) before the channel is closed.
	c.wg.Go(func() {
		time.Sleep(50 * time.Millisecond)
		if !closed.Load() {
			ackedBeforeClose.Store(true)
		}
	})

	c.Disconnect()

	if !closed.Load() {
		t.Fatal("expected channel to be closed")
	}
	if !ackedBeforeClose.Load() {
		t.Fatal("expected in-flight handler to finish before channel close")
	}
}

func TestDisconnect_DrainTimeout_DoesNotHang(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	var closed atomic.Bool
	ch := &mockAMQPChannel{
		cancelFn: func(string, bool) error { return nil },
		closeFn:  func() error { closed.Store(true); return nil },
	}
	c := newTestConsumer(ch, &mockConnProvider{})
	c.drainTimeout = 20 * time.Millisecond

	// A handler that blocks past the drain timeout.
	c.wg.Go(func() { <-release })

	done := make(chan struct{})
	go func() {
		c.Disconnect()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect hung despite drain timeout")
	}

	if !closed.Load() {
		t.Fatal("expected channel to be closed after drain timeout")
	}
}

// disconnectWithin fails the test if Disconnect does not return within d.
func disconnectWithin(t *testing.T, c *Consumer, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		c.Disconnect()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("Disconnect did not return in time")
	}
}

func TestDisconnect_DrainsDeliveriesBufferedBeforeCancelOk(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	consuming := make(chan struct{})
	var closed, handled, ackedAfterClose atomic.Bool

	ack := &mockAcknowledger{
		ackFn: func(uint64, bool) error {
			if closed.Load() {
				ackedAfterClose.Store(true)
			}
			return nil
		},
	}
	ch := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			close(consuming)
			return deliveries, nil
		},
		// Like amqp091, a delivery buffered before cancel-ok still reaches
		// the loop after Cancel returns; then the delivery channel closes.
		cancelFn: func(string, bool) error {
			go func() {
				time.Sleep(20 * time.Millisecond)
				deliveries <- amqp.Delivery{Acknowledger: ack, Body: []byte("late"), Headers: amqp.Table{}}
				close(deliveries)
			}()
			return nil
		},
		closeFn: func() error { closed.Store(true); return nil },
	}
	c := newTestConsumer(ch, &mockConnProvider{})
	c.params.Callback = func(Delivery, string) error {
		handled.Store(true)
		return nil
	}

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case <-consuming:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not start consuming")
	}

	disconnectWithin(t, c, 2*time.Second)

	if !handled.Load() {
		t.Fatal("expected the late delivery to be handled before Disconnect returned")
	}
	if ackedAfterClose.Load() {
		t.Fatal("expected the late delivery to be acked before the channel was closed")
	}

	select {
	case <-begun:
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}
}

func TestDisconnect_DuringConsume_DrainsLoop(t *testing.T) {
	deliveries := make(chan amqp.Delivery, 1)
	inConsume := make(chan struct{})
	release := make(chan struct{})
	var closed, handled, ackedAfterClose atomic.Bool

	deliveries <- amqp.Delivery{
		Acknowledger: &mockAcknowledger{ackFn: func(uint64, bool) error {
			if closed.Load() {
				ackedAfterClose.Store(true)
			}
			return nil
		}},
		Body:    []byte("first"),
		Headers: amqp.Table{},
	}
	ch := &mockAMQPChannel{
		// Consume is still in flight when Disconnect starts.
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			close(inConsume)
			<-release
			return deliveries, nil
		},
		cancelFn: func(string, bool) error { close(deliveries); return nil },
		closeFn:  func() error { closed.Store(true); return nil },
	}
	c := newTestConsumer(ch, &mockConnProvider{})
	c.params.Callback = func(Delivery, string) error {
		handled.Store(true)
		return nil
	}

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case <-inConsume:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not start consuming")
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()
	disconnectWithin(t, c, 2*time.Second)

	if !handled.Load() {
		t.Fatal("expected the delivery from the in-flight Consume to be handled before Disconnect returned")
	}
	if ackedAfterClose.Load() {
		t.Fatal("expected the delivery to be acked before the channel was closed")
	}

	select {
	case <-begun:
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}
}

func TestDisconnect_DuringFailedConsume_DoesNotReconnect(t *testing.T) {
	inConsume := make(chan struct{})
	release := make(chan struct{})
	var reopened atomic.Bool

	ch := &mockAMQPChannel{
		// Consume is in flight when Disconnect starts, then fails.
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			close(inConsume)
			<-release
			return nil, errors.New("consume failed")
		},
	}
	conn := &mockConnProvider{
		channelFn: func() (amqpx.AMQPChannel, error) {
			reopened.Store(true)
			return &mockAMQPChannel{}, nil
		},
	}
	c := newTestConsumer(ch, conn)

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case <-inConsume:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not start consuming")
	}

	disconnected := make(chan struct{})
	go func() {
		c.Disconnect()
		close(disconnected)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !c.closing.Load() {
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("expected Disconnect to set closing while Consume was in flight")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)

	select {
	case <-disconnected:
	case <-time.After(2 * time.Second):
		t.Fatal("Disconnect did not return in time")
	}

	select {
	case <-begun:
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}

	if reopened.Load() {
		t.Fatal("expected Begin not to reopen the channel after Disconnect")
	}
}

func TestDisconnect_DuringReconnect_DoesNotConsume(t *testing.T) {
	var c *Consumer
	var consumedAfterDisconnect atomic.Bool

	dropped := make(chan amqp.Delivery)
	close(dropped)
	first := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			return dropped, nil
		},
	}
	reopened := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			consumedAfterDisconnect.Store(true)
			deliveries := make(chan amqp.Delivery)
			close(deliveries)
			return deliveries, nil
		},
	}
	conn := &mockConnProvider{
		// Disconnect lands while Begin reopens the channel after a drop.
		channelFn: func() (amqpx.AMQPChannel, error) {
			c.Disconnect()
			return reopened, nil
		},
	}
	c = newTestConsumer(first, conn)

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case err := <-begun:
		if err != nil {
			t.Fatalf("expected Begin to return nil after Disconnect, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}

	if consumedAfterDisconnect.Load() {
		t.Fatal("expected Begin not to consume on the channel reopened after Disconnect")
	}
}

func TestDisconnect_BeforeBegin_DoesNotConsume(t *testing.T) {
	var consumed atomic.Bool
	ch := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			consumed.Store(true)
			deliveries := make(chan amqp.Delivery)
			close(deliveries)
			return deliveries, nil
		},
	}
	c := newTestConsumer(ch, &mockConnProvider{})

	disconnectWithin(t, c, 2*time.Second)

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case err := <-begun:
		if err != nil {
			t.Fatalf("expected Begin to return nil after Disconnect, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}

	if consumed.Load() {
		t.Fatal("expected Begin not to start consuming after Disconnect")
	}
}

func TestDisconnect_StuckConsumeLoop_RespectsDrainTimeout(t *testing.T) {
	deliveries := make(chan amqp.Delivery)
	consuming := make(chan struct{})
	var closed atomic.Bool
	ch := &mockAMQPChannel{
		consumeFn: func(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
			close(consuming)
			return deliveries, nil
		},
		// If Cancel fails without closing the delivery channel, only
		// closing the channel ends the loop.
		cancelFn: func(string, bool) error { return errors.New("cancel failed") },
		closeFn: func() error {
			closed.Store(true)
			close(deliveries)
			return nil
		},
	}
	c := newTestConsumer(ch, &mockConnProvider{})
	c.drainTimeout = 20 * time.Millisecond

	begun := make(chan error, 1)
	go func() { begun <- c.Begin() }()

	select {
	case <-consuming:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not start consuming")
	}

	disconnectWithin(t, c, 2*time.Second)

	if !closed.Load() {
		t.Fatal("expected channel to be closed after drain timeout")
	}

	select {
	case <-begun:
	case <-time.After(2 * time.Second):
		t.Fatal("Begin did not return after Disconnect")
	}
}
