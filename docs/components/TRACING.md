# OpenTelemetry Tracing

FlashDB emits distributed trace spans for key operations via the OpenTelemetry SDK. Tracing is optional and configured at engine startup — when no endpoint is provided, all tracing calls are no-ops with zero overhead.

## Overview

Tracing gives you end-to-end visibility into database operations, helping debug latency, identify hot spots, and understand request flow across Raft cluster nodes.

```
Client Request
     │
     ▼
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  db.Put()   │────►│  db.flush()      │────►│  compaction     │
│  span       │     │  span            │     │  span           │
│  key.len    │     │  duration_ms     │     │  entries        │
│  value.len  │     └──────────────────┘     └─────────────────┘
└─────────────┘
     │
     ▼
┌─────────────┐
│  db.Get()   │
│  span       │
│  key.len    │
└─────────────┘
```

## Configuration

### Via Engine Config

```go
cfg := engine.DefaultConfig("/tmp/flashdb")
cfg.Tracing = &tracing.Config{
    ServiceName: "flashdb-cluster",           // identifies the service in traces
    Endpoint:    "http://otel-collector:4318", // OTLP HTTP endpoint
    SampleRate:  1.0,                          // 0.0–1.0 fraction to sample
    Attributes: map[string]string{             // global attributes on every span
        "node.id": "node1",
        "env":      "production",
    },
}
db, _ := engine.Open(cfg)
```

### Via CLI

```bash
flashdb serve --dir /tmp/flashdb --otel-endpoint http://localhost:4318
```

### Disabled (No-Op)

When `Endpoint` is empty (the default), the tracer returns a no-op implementation. All `Start`/`End` calls compile away to nothing — safe to leave the tracing config in place regardless of deployment.

## Emitted Spans

| Span Name | Operation | Attributes |
|---|---|---|
| `db.put` | `db.Put()` | `key.len`, `value.len` |
| `db.get` | `db.Get()` | `key.len` |
| `db.delete` | `db.Delete()` | `key.len` |
| `db.flush` | MemTable → SSTable flush | `duration_ms` |
| `db.txn.commit` | `Txn.Commit()` | `ops` (number of operations) |

## Export Backends

Any OTLP-compatible backend works:

- **Jaeger** — `http://jaeger:4318`
- **Grafana Tempo** — `http://tempo:4318`
- **Datadog** — `https://trace.agent.datadoghq.com:4318`
- **OpenTelemetry Collector** — `http://otel-collector:4318`

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌────────────────┐
│  flashDB     │────►│  OTLP Exporter   │────►│  Trace Backend │
│  engine      │     │  (HTTP)          │     │  (Jaeger, etc) │
│              │     │                  │     │                │
│  batch spans │     │  batched export  │     │  visualize     │
│  every 5s    │     │  every 5s        │     │  & query       │
└──────────────┘     └──────────────────┘     └────────────────┘
```

The tracer uses the standard OTel SDK with:
- `otlptracehttp` exporter for OTLP HTTP transport
- Batch span processor (default: export every 5s or 512 spans)
- `TraceIDRatioBased` sampler for probabilistic sampling
- Resource attributes for service identity

## Code Reference

Package: `src/tracing/tracing.go`

```go
type Config struct {
    ServiceName string
    Endpoint    string            // OTLP HTTP; empty = no-op
    SampleRate  float64           // 0.0–1.0, default 1.0
    Attributes  map[string]string // global span attributes
}

type Tracer struct { /* unexported */ }

func New(cfg Config) (*Tracer, error)
func (t *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span)
func End(span trace.Span, err *error)
func (t *Tracer) Shutdown()
```

The `Tracer` is safe for concurrent use. Calling `Shutdown` flushes pending spans and stops the exporter.
