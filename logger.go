package rabbitmq

import "fmt"

// Logger is the interface used by the library for all log output.
// Provide a custom implementation via [Config].Logger to integrate
// with your application's logging framework. When nil, a default
// logger that writes to stdout is used.
type Logger interface {
	// Info logs an informational message with optional format arguments.
	Info(msg string, args ...any)
	// Error logs an error message with optional format arguments.
	Error(msg string, args ...any)
}

type defaultLogger struct{}

func (l *defaultLogger) Info(msg string, args ...any)  { fmt.Printf("[INFO] "+msg+"\n", args...) }
func (l *defaultLogger) Error(msg string, args ...any) { fmt.Printf("[ERROR] "+msg+"\n", args...) }

// NewDefaultLogger returns a [Logger] that writes to stdout with
// [INFO] / [ERROR] prefixes. It is used when no custom logger is
// provided to a client.
func NewDefaultLogger() Logger { return &defaultLogger{} }
