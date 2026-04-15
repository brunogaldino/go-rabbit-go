package consumer

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

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
	}
	c.params.Queue = "test-queue"
	c.params.ExchangeName = "test-exchange"
	c.params.RoutingKey = []string{"test.key"}
	c.params.Callback = func(d Delivery, queue string) error { return nil }
	c.params.DeadletterStrategy.DLQueueName = "test-queue" + amqpx.SuffixDLQ
	return c
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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("hello"),
	}}

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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body:    []byte("hello"),
		Headers: amqp.Table{},
	}}

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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{},
		Body:         []byte("data"),
		Headers:      amqp.Table{},
	}}

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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body: []byte("exhausted"),
		Headers: amqp.Table{
			amqpx.KeyRetriesCount: int32(defaults.RetryStrategy.MaxAttempt),
		},
	}}

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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body:    []byte("no-retry"),
		Headers: amqp.Table{},
	}}

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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("drop"),
	}}

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
	c.params.DeadletterStrategy.CallbackFn = func(body string) bool {
		return false
	}

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			ackFn: func(_ uint64, _ bool) error { acked = true; return nil },
		},
		Body: []byte("skip-dlq"),
	}}

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
	c.params.DeadletterStrategy.CallbackFn = func(body string) bool {
		panic("callback panic")
	}

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body: []byte("panic-body"),
	}}

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
	d := Delivery{amqp.Delivery{
		Headers: amqp.Table{"foo": "bar"},
	}}

	if got := d.GetHeader("foo"); got != "bar" {
		t.Fatalf("expected 'bar', got %v", got)
	}

	if got := d.GetHeader("missing"); got != nil {
		t.Fatalf("expected nil for missing header, got %v", got)
	}

	// nil headers
	d2 := Delivery{amqp.Delivery{}}
	if got := d2.GetHeader("any"); got != nil {
		t.Fatalf("expected nil for nil headers, got %v", got)
	}
}

func TestDelivery_GetRoutingKey(t *testing.T) {
	// With original routing key header.
	d := Delivery{amqp.Delivery{
		RoutingKey: "current-rk",
		Headers:    amqp.Table{amqpx.KeyOriginalRouteKey: "original-rk"},
	}}

	if got := d.GetRoutingKey(); got != "original-rk" {
		t.Fatalf("expected 'original-rk', got %q", got)
	}

	// Without header — falls back to RoutingKey.
	d2 := Delivery{amqp.Delivery{
		RoutingKey: "current-rk",
		Headers:    amqp.Table{},
	}}

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
	if c.params.RetryStrategy.DelayFn(Delivery{}, 1, nil) != 999 {
		t.Fatal("expected custom delay fn to return 999")
	}

	// WithDLQFn
	dlqCalled := false
	WithDLQFn(func(s string) bool { dlqCalled = true; return true })(c)
	c.params.DeadletterStrategy.CallbackFn("test")
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
				errorFn: func(msg string, args ...any) {
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

	d := Delivery{amqp.Delivery{
		Acknowledger: &mockAcknowledger{
			nackFn: func(_ uint64, _ bool, _ bool) error { nacked = true; return nil },
		},
		Body:    []byte("data"),
		Headers: amqp.Table{},
	}}

	c.publishRetry(d, 1, errors.New("err"))

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
