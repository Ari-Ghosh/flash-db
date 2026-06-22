# ADR 008: OpenTelemetry Tracing

**Status:** Accepted  
**Date:** June 2026  
**Driver:** Observability, distributed debugging, performance analysis

## Context

FlashDB had operational visibility through Prometheus metrics (counters, gauges) but lacked **distributed tracing** — the ability to follow a single request through its lifecycle (Put → WAL append → MemTable → flush → compaction). As the system adds Raft consensus, tracing becomes essential for debugging cross-node latency and consensus round-trips.

## Decision

We add an optional **OpenTelemetry tracing** layer with the following design:

1. **New `tracing` package** (`src/tracing/`):
   - Wraps the OTel SDK (trace provider, span exporter, sampler).
   - Returns a **no-op tracer** when no endpoint is configured — zero overhead for users who don't need tracing.
   - Provides typed span constants (`db.put`, `db.get`, `db.flush`, etc.) for consistent naming.

2. **Instrumentation in the engine**:
   - `db.Put()`, `db.Get()`, `db.Delete()` each create a span named after the operation.
   - `db.flushImmutable()` creates a span with `duration_ms` attribute.
   - `db.ApplyTxn()` creates a span with operation count.
   - Spans carry relevant attributes (key length, value length, op count).

3. **Configuration**:
   - `engine.Config.Tracing *tracing.Config` is nil by default (disabled).
   - When non-nil, the engine initializes the tracer in `Open()` and shuts it down in `Close()`.
   - CLI exposes `--otel-endpoint` flag.

4. **Export**:
   - OTLP HTTP (`otlptracehttp`) for broad compatibility.
   - Batch span processor for efficiency (export every 5s or 512 spans).
   - Configurable sample rate for high-throughput deployments.

## Consequences

### Positive

- **Distributed debugging**: Trace a Put across Raft leader → FSM apply → flush in a single view.
- **Latency breakdown**: See exactly how long each operation phase takes.
- **Backend agnostic**: OTLP works with Jaeger, Tempo, Datadog, and any OTel-compatible backend.
- **Zero overhead when disabled**: No-op tracer compiles away all calls.

### Negative

- **Dependency weight**: Adds ~15 indirect dependencies including gRPC and protobuf.
- **Memory overhead**: Batch span processor buffers spans (configurable, default minimal).
- **Export latency**: Batched export adds ~5s of latency before spans appear in the backend.

## Technical Details

```go
// src/tracing/tracing.go
type Config struct {
    ServiceName string
    Endpoint    string            // OTLP HTTP endpoint; empty = no-op
    SampleRate  float64           // 0.0–1.0
    Attributes  map[string]string // global span attributes
}

type Tracer struct { /* unexported fields */ }

func New(cfg Config) (*Tracer, error)
func (t *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)
func End(span trace.Span, err *error)
func (t *Tracer) Shutdown()
```

Span constants:

```go
const (
    SpanKindPut       = "db.put"
    SpanKindGet       = "db.get"
    SpanKindDelete    = "db.delete"
    SpanKindFlush     = "db.flush"
    SpanKindCompact   = "db.compact"
    SpanKindTxnCommit = "db.txn.commit"
)
```

## References

- [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/)
- [OTLP HTTP Exporter Specification](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
- [Tracing Component Documentation](../components/TRACING.md)
