package conn

import (
	"testing"
)

func TestMarkConnected(t *testing.T) {
	mc := &Managed{}
	mc.IsReconnecting.Store(true)

	mc.MarkConnected()

	if !mc.IsConnected.Load() {
		t.Error("expected IsConnected=true")
	}
	if mc.IsReconnecting.Load() {
		t.Error("expected IsReconnecting=false after MarkConnected")
	}
}

func TestMarkDisconnected(t *testing.T) {
	mc := &Managed{}
	mc.IsConnected.Store(true)

	mc.MarkDisconnected()

	if mc.IsConnected.Load() {
		t.Error("expected IsConnected=false")
	}
}

func TestClose_NilConn(t *testing.T) {
	mc := &Managed{Conn: nil}
	// Should not panic.
	mc.Close()
}

func TestClose_AlreadyClosed(t *testing.T) {
	closed := false
	mock := &mockAMQPConnection{
		isClosedFn: func() bool { return true },
		closeFn:    func() error { closed = true; return nil },
	}
	mc := &Managed{Conn: mock}

	mc.Close()

	if closed {
		t.Error("Close() should not call conn.Close() when already closed")
	}
}

func TestClose_OpenConnection(t *testing.T) {
	closed := false
	mock := &mockAMQPConnection{
		isClosedFn: func() bool { return false },
		closeFn:    func() error { closed = true; return nil },
	}
	mc := &Managed{Conn: mock}

	mc.Close()

	if !closed {
		t.Error("Close() should call conn.Close() when connection is open")
	}
}

func TestManaged_ZeroValue(t *testing.T) {
	mc := &Managed{}

	if mc.IsConnected.Load() {
		t.Error("zero-value IsConnected should be false")
	}
	if mc.IsReconnecting.Load() {
		t.Error("zero-value IsReconnecting should be false")
	}
	if mc.ReconnectAttempt != 0 {
		t.Error("zero-value ReconnectAttempt should be 0")
	}
}
