package publisher

import (
	"context"
	"errors"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	rabbitmq "github.com/brunogaldino/go-rabbit-go"
	"github.com/brunogaldino/go-rabbit-go/amqpx"
)

func TestConnect_Success(t *testing.T) {
	ch := &mockAMQPChannel{}
	conn := &mockConnProvider{
		ChannelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	p := &Publisher{
		conn:            conn,
		publishConfirms: true,
		wg:              &sync.WaitGroup{},
	}

	if err := p.Connect(); err != nil {
		t.Fatalf("Connect() error = %v, want nil", err)
	}
	defer close(p.notifyChanClose)

	if !p.isConnected {
		t.Fatal("expected isConnected to be true")
	}

	if p.ch != ch {
		t.Fatal("expected channel to be set")
	}
}

func TestConnect_ChannelError(t *testing.T) {
	conn := &mockConnProvider{
		ChannelFn: func() (amqpx.AMQPChannel, error) {
			return nil, errors.New("dial failed")
		},
	}

	p := &Publisher{
		conn:            conn,
		publishConfirms: true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Connect()
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var chErr *rabbitmq.ChannelError
	if !errors.As(err, &chErr) {
		t.Fatalf("expected *ChannelError, got %T", err)
	}

	if chErr.Operation != "open" {
		t.Fatalf("expected operation %q, got %q", "open", chErr.Operation)
	}
}

func TestConnect_ConfirmError(t *testing.T) {
	ch := &mockAMQPChannel{
		ConfirmFn: func(bool) error { return errors.New("confirm failed") },
	}

	conn := &mockConnProvider{
		ChannelFn: func() (amqpx.AMQPChannel, error) { return ch, nil },
	}

	p := &Publisher{
		conn:            conn,
		publishConfirms: true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Connect()
	close(p.notifyChanClose) // clean up monitorChannel goroutine

	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var chErr *rabbitmq.ChannelError
	if !errors.As(err, &chErr) {
		t.Fatalf("expected *ChannelError, got %T", err)
	}

	if chErr.Operation != "confirm" {
		t.Fatalf("expected operation %q, got %q", "confirm", chErr.Operation)
	}
}

func TestDeclareExchanges_Success(t *testing.T) {
	var called int
	ch := &mockAMQPChannel{
		ExchangeDeclareFn: func(string, string, bool, bool, bool, bool, amqp.Table) error {
			called++
			return nil
		},
	}

	p := &Publisher{
		config: []ExchangeOption{
			{Name: "events", Type: ExchangeTopic},
			{Name: "commands", Type: ExchangeDirect},
		},
	}

	if err := p.declareExchanges(ch); err != nil {
		t.Fatalf("declareExchanges() error = %v, want nil", err)
	}

	if called != 2 {
		t.Fatalf("expected 2 ExchangeDeclare calls, got %d", called)
	}
}

func TestDeclareExchanges_Error(t *testing.T) {
	ch := &mockAMQPChannel{
		ExchangeDeclareFn: func(string, string, bool, bool, bool, bool, amqp.Table) error {
			return errors.New("server error")
		},
	}

	p := &Publisher{
		config: []ExchangeOption{
			{Name: "bad-exchange", Type: ExchangeDirect},
		},
	}

	err := p.declareExchanges(ch)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var exErr *ExchangeError
	if !errors.As(err, &exErr) {
		t.Fatalf("expected *ExchangeError, got %T", err)
	}

	if exErr.Name != "bad-exchange" {
		t.Fatalf("expected exchange name %q, got %q", "bad-exchange", exErr.Name)
	}
}

func TestDeclareExchanges_DefaultOptions(t *testing.T) {
	var gotDurable, gotAutoDelete bool

	ch := &mockAMQPChannel{
		ExchangeDeclareFn: func(_, _ string, durable, autoDelete, _, _ bool, _ amqp.Table) error {
			gotDurable = durable
			gotAutoDelete = autoDelete
			return nil
		},
	}

	p := &Publisher{
		config: []ExchangeOption{
			{Name: "defaults", Type: ExchangeFanout},
		},
	}

	if err := p.declareExchanges(ch); err != nil {
		t.Fatalf("declareExchanges() error = %v, want nil", err)
	}

	if !gotDurable {
		t.Fatal("expected durable=true by default")
	}

	if gotAutoDelete {
		t.Fatal("expected autoDelete=false by default")
	}
}

func TestPublish_WithConfirmation(t *testing.T) {
	confirmCh := make(chan amqp.Confirmation, 1)
	confirmCh <- amqp.Confirmation{Ack: true, DeliveryTag: 1}

	ch := &mockAMQPChannel{
		PublishWithDeferredConfirmFn: func(string, string, bool, bool, amqp.Publishing) (*amqp.DeferredConfirmation, error) {
			return nil, nil
		},
	}

	p := &Publisher{
		conn:            &mockConnProvider{},
		ch:              ch,
		publishConfirms: true,
		confirmCh:       confirmCh,
		isConnected:     true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Publish(Message{
		Exchange:   "events",
		RoutingKey: "order.created",
		Message:    []byte(`{"id":1}`),
	})
	if err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
}

func TestPublish_Nack(t *testing.T) {
	confirmCh := make(chan amqp.Confirmation, 1)
	confirmCh <- amqp.Confirmation{Ack: false, DeliveryTag: 42}

	ch := &mockAMQPChannel{
		PublishWithDeferredConfirmFn: func(string, string, bool, bool, amqp.Publishing) (*amqp.DeferredConfirmation, error) {
			return nil, nil
		},
	}

	p := &Publisher{
		conn:            &mockConnProvider{},
		ch:              ch,
		publishConfirms: true,
		confirmCh:       confirmCh,
		isConnected:     true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Publish(Message{
		Exchange:   "events",
		RoutingKey: "order.created",
		Message:    []byte(`{"id":1}`),
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var pubErr *PublishError
	if !errors.As(err, &pubErr) {
		t.Fatalf("expected *PublishError, got %T", err)
	}

	if pubErr.Tag != 42 {
		t.Fatalf("expected delivery tag 42, got %d", pubErr.Tag)
	}
}

func TestPublish_WithoutConfirmation(t *testing.T) {
	called := false
	ch := &mockAMQPChannel{
		PublishWithContextFn: func(context.Context, string, string, bool, bool, amqp.Publishing) error {
			called = true
			return nil
		},
	}

	p := &Publisher{
		conn:            &mockConnProvider{},
		ch:              ch,
		publishConfirms: false,
		isConnected:     true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Publish(Message{
		Exchange:   "events",
		RoutingKey: "order.created",
		Message:    []byte(`{"id":1}`),
	})
	if err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}

	if !called {
		t.Fatal("expected PublishWithContext to be called")
	}
}

func TestWaitForConnection_Closing(t *testing.T) {
	conn := &mockConnProvider{
		ClosingFn: func() bool { return true },
	}

	p := &Publisher{conn: conn}

	err := p.waitForConnection()
	if !errors.Is(err, rabbitmq.ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got %v", err)
	}
}

func TestWaitForConnection_NotBlocked(t *testing.T) {
	p := &Publisher{conn: &mockConnProvider{}}

	if err := p.waitForConnection(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestPublish_CustomContentType(t *testing.T) {
	confirmCh := make(chan amqp.Confirmation, 1)
	confirmCh <- amqp.Confirmation{Ack: true, DeliveryTag: 1}

	var gotContentType string
	ch := &mockAMQPChannel{
		PublishWithDeferredConfirmFn: func(_, _ string, _, _ bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
			gotContentType = msg.ContentType
			return nil, nil
		},
	}

	p := &Publisher{
		conn:            &mockConnProvider{},
		ch:              ch,
		publishConfirms: true,
		confirmCh:       confirmCh,
		isConnected:     true,
		wg:              &sync.WaitGroup{},
	}

	err := p.Publish(Message{
		Exchange:    "events",
		RoutingKey:  "order.created",
		Message:     []byte("<order/>"),
		ContentType: "application/xml",
	})
	if err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}

	if gotContentType != "application/xml" {
		t.Fatalf("expected content type %q, got %q", "application/xml", gotContentType)
	}
}
