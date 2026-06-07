// Package logging provides structured logging via the standard library's slog.

package logging

import (
	"context"
	"log/slog"
	"os"
)

// Level controls global log verbosity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

// Logger wraps slog.Logger with level-aware methods.
type Logger struct {
	inner *slog.Logger
}

// New creates a Logger that writes JSON to stderr at the given level.
func New(level Level) *Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slogLevel(level),
	})
	return &Logger{inner: slog.New(handler)}
}

// NewWithHandler allows full control over the underlying slog.Handler.
func NewWithHandler(handler slog.Handler, level Level) *Logger {
	return &Logger{inner: slog.New(handler)}
}

func slogLevel(l Level) slog.Level {
	switch l {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.inner.Debug(msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.inner.Info(msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.inner.Warn(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.inner.Error(msg, args...)
}

// With returns a sub-logger that prepends the given key=value pairs.
func (l *Logger) With(args ...any) *Logger {
	return &Logger{inner: l.inner.With(args...)}
}

// LogAttrs is an internal helper used by packages that need direct slog.Attr access.
func (l *Logger) LogAttrs(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	l.inner.LogAttrs(ctx, level, msg, attrs...)
}
