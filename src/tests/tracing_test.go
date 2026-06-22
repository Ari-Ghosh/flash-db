package tests

import (
	"context"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/tracing"
)

// TestTracing_Noop verifies that tracing works as a no-op when no endpoint
// is configured — operations should succeed normally.
func TestTracing_Noop(t *testing.T) {
	tracer, err := tracing.New(tracing.Config{
		ServiceName: "test",
		Endpoint:    "", // no-op
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracer.Shutdown()

	_, span := tracer.Start(context.Background(), "test.op")
	span.End()
}

// TestTracing_EngineNoop verifies the engine tolerates a nil Tracing config.
func TestTracing_EngineNoop(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 128 * 1024
	cfg.L0CompactThreshold = 2
	cfg.Tracing = nil // explicitly nil

	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Basic ops should work without tracing.
	mustPut(t, db, "k", "v")
	mustGet(t, db, "k", "v")
}

// TestTracing_EngineWithConfig verifies the engine tolerates a Tracing config
// with no endpoint (no-op tracer inside the engine).
func TestTracing_EngineWithConfig(t *testing.T) {
	dir := tmpDir(t)
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 128 * 1024
	cfg.L0CompactThreshold = 2
	cfg.Tracing = &tracing.Config{
		ServiceName: "test-flashdb",
		Endpoint:    "", // no-op
	}

	db, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	start := time.Now()
	for i := 0; i < 10; i++ {
		mustPut(t, db, "trace:k", "v")
		mustGet(t, db, "trace:k", "v")
	}
	t.Logf("10 traced ops took %v", time.Since(start))
}
