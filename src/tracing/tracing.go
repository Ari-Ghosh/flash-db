// Package tracing provides OpenTelemetry trace spans for flashDB operations.
//
// Usage:
//
//	exp, err := tracing.New(tracing.Config{
//		ServiceName: "flashdb",
//		Endpoint:    "http://localhost:4318", // OTLP HTTP endpoint
//	})
//	if err != nil { ... }
//	defer exp.Shutdown()
//
//	ctx := exp.Start(context.Background(), "db.put")
//	defer exp.End(ctx)
//
// The exporter is optional — if OTel is not configured, all calls become no-ops.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config configures the OpenTelemetry trace exporter.
type Config struct {
	// ServiceName identifies the service in traces. Default: "flashdb".
	ServiceName string
	// Endpoint is the OTLP HTTP endpoint (e.g. "http://localhost:4318").
	// If empty, tracing is disabled (all calls become no-ops).
	Endpoint string
	// SampleRate is the fraction of traces to sample (0.0–1.0). Default: 1.0.
	SampleRate float64
	// Attributes are global attributes attached to every span.
	Attributes map[string]string
}

// Tracer wraps an OpenTelemetry tracer provider and provides convenience
// methods for creating and ending trace spans.
type Tracer struct {
	tp     trace.TracerProvider
	tracer trace.Tracer
	attrs  []attribute.KeyValue
	shutFn func()
}

// New creates a new Tracer. If cfg.Endpoint is empty, a no-op tracer is
// returned that does nothing — safe to call unconditionally.
func New(cfg Config) (*Tracer, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "flashdb"
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = 1.0
	}

	t := &Tracer{
		attrs: make([]attribute.KeyValue, 0, len(cfg.Attributes)),
	}
	for k, v := range cfg.Attributes {
		t.attrs = append(t.attrs, attribute.String(k, v))
	}

	if cfg.Endpoint == "" {
		// No-op tracer: tracing disabled.
		t.tp = noop.NewTracerProvider()
		t.tracer = t.tp.Tracer("flashdb")
		t.shutFn = func() {}
		return t, nil
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String(cfg.ServiceName)),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}

	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(cfg.Endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRate)),
	)
	otel.SetTracerProvider(tp)

	t.tp = tp
	t.tracer = tp.Tracer("flashdb")
	t.shutFn = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}
	return t, nil
}

// Shutdown flushes and shuts down the tracer provider. Safe to call even
// on a no-op tracer.
func (t *Tracer) Shutdown() {
	t.shutFn()
}

// TracerProvider returns the underlying TracerProvider.
func (t *Tracer) TracerProvider() trace.TracerProvider {
	return t.tp
}

// Start starts a new span with the given name and optional attributes.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if len(attrs) == 0 && len(t.attrs) > 0 {
		attrs = t.attrs
	} else if len(t.attrs) > 0 {
		combined := make([]attribute.KeyValue, 0, len(t.attrs)+len(attrs))
		combined = append(combined, t.attrs...)
		combined = append(combined, attrs...)
		attrs = combined
	}
	if len(attrs) > 0 {
		return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	}
	return t.tracer.Start(ctx, name)
}

// End ends a span. If err is non-nil, it records the error on the span.
func End(span trace.Span, err *error) {
	if err != nil && *err != nil {
		span.RecordError(*err)
		span.SetAttributes(attribute.Bool("error", true))
	}
	span.End()
}

// SpanKind constants.
const (
	SpanKindPut       = "db.put"
	SpanKindGet       = "db.get"
	SpanKindDelete    = "db.delete"
	SpanKindFlush     = "db.flush"
	SpanKindCompact   = "db.compact"
	SpanKindTxnCommit = "db.txn.commit"
	SpanKindBackup    = "db.backup"
	SpanKindRestore   = "db.restore"
	SpanKindReplicate = "db.replicate"
)
