package rabbitmq

import "fmt"

// LogType controls which inspection logs are emitted.
// Use [LogTypeNone] (the default) to suppress inspection output, or
// one of the other constants to enable logging for consumers,
// publishers, or both. Errors are always logged regardless of this
// setting.
type LogType string

const (
	// LogTypeNone disables inspection logging (default).
	LogTypeNone LogType = "none"
	// LogTypeConsumer enables inspection logging for consumers only.
	LogTypeConsumer LogType = "consumer"
	// LogTypePublisher enables inspection logging for publishers only.
	LogTypePublisher LogType = "publisher"
	// LogTypeAll enables inspection logging for both consumers and publishers.
	LogTypeAll LogType = "all"
)

// Includes reports whether lt enables logging for the given category.
// A category is enabled when lt equals [LogTypeAll] or matches the
// category exactly.
func (lt LogType) Includes(category LogType) bool {
	return lt == LogTypeAll || lt == category
}

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
