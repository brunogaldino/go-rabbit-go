package rabbitmq

import (
	"errors"
	"strings"
	"testing"
)

func TestChannelError_Error(t *testing.T) {
	inner := errors.New("broken pipe")
	ce := &ChannelError{Operation: "open", Err: inner}

	msg := ce.Error()
	if !strings.Contains(msg, "open") {
		t.Fatalf("expected error message to contain 'open', got %q", msg)
	}

	if !strings.Contains(msg, "broken pipe") {
		t.Fatalf("expected error message to contain inner error, got %q", msg)
	}
}

func TestChannelError_Unwrap(t *testing.T) {
	inner := errors.New("broken pipe")
	ce := &ChannelError{Operation: "close", Err: inner}

	if !errors.Is(ce, inner) {
		t.Fatal("expected ChannelError to unwrap to inner error")
	}
}

func TestErrConnectionClosed(t *testing.T) {
	if ErrConnectionClosed == nil {
		t.Fatal("ErrConnectionClosed must not be nil")
	}

	if !strings.Contains(ErrConnectionClosed.Error(), "closed") {
		t.Fatalf("unexpected error message: %q", ErrConnectionClosed.Error())
	}
}

func TestErrConnectionBlocked(t *testing.T) {
	if ErrConnectionBlocked == nil {
		t.Fatal("ErrConnectionBlocked must not be nil")
	}

	if !strings.Contains(ErrConnectionBlocked.Error(), "blocked") {
		t.Fatalf("unexpected error message: %q", ErrConnectionBlocked.Error())
	}
}

func TestErrMaxReconnectAttempts(t *testing.T) {
	if ErrMaxReconnectAttempts == nil {
		t.Fatal("ErrMaxReconnectAttempts must not be nil")
	}

	if !strings.Contains(ErrMaxReconnectAttempts.Error(), "reconnect") {
		t.Fatalf("unexpected error message: %q", ErrMaxReconnectAttempts.Error())
	}
}

func TestSentinelErrors_AreDistinct(t *testing.T) {
	if errors.Is(ErrConnectionClosed, ErrConnectionBlocked) {
		t.Fatal("sentinel errors must be distinct")
	}

	if errors.Is(ErrConnectionClosed, ErrMaxReconnectAttempts) {
		t.Fatal("sentinel errors must be distinct")
	}

	if errors.Is(ErrConnectionBlocked, ErrMaxReconnectAttempts) {
		t.Fatal("sentinel errors must be distinct")
	}
}
