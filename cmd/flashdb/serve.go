package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/logging"
	"github.com/Ari-Ghosh/flash-db/src/metrics"
	"github.com/Ari-Ghosh/flash-db/src/replication"
	"github.com/Ari-Ghosh/flash-db/src/tracing"
)

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	dir := fs.String("dir", "/tmp/flashdb", "Data directory")
	addr := fs.String("addr", "", "HTTP metrics listen address (e.g. :9090)")
	raftAddr := fs.String("raft-addr", "", "Raft cluster bind address (e.g. :6000)")
	raftJoin := fs.String("raft-join", "", "Existing Raft node to join (address)")
	nodeID := fs.String("node-id", "", "Unique node ID for Raft cluster")
	otelEndpoint := fs.String("otel-endpoint", "", "OpenTelemetry OTLP HTTP endpoint")
	metricsAddr := fs.String("metrics-addr", "", "Prometheus metrics endpoint (e.g. :9090)")
	memSize := fs.Int64("memtable-size", 64*1024*1024, "MemTable size in bytes")
	l0Threshold := fs.Int("l0-threshold", 4, "L0 compaction threshold (files)")
	_ = fs.Parse(args)

	log := logging.New(logging.LevelInfo)
	cfg := engine.DefaultConfig(*dir)
	cfg.MemTableSize = *memSize
	cfg.L0CompactThreshold = *l0Threshold
	cfg.Logger = log

	// Merge deprecated -addr with -metrics-addr.
	metricsListen := *metricsAddr
	if metricsListen == "" {
		metricsListen = *addr
	}

	// OpenTelemetry tracing.
	if *otelEndpoint != "" {
		cfg.Tracing = &tracing.Config{
			ServiceName: "flashdb",
			Endpoint:    *otelEndpoint,
			SampleRate:  1.0,
			Attributes: map[string]string{
				"node.id":    *nodeID,
				"data.dir":   *dir,
				"db.version": "4.0",
			},
		}
	}

	// Raft cluster mode.
	if *raftAddr != "" {
		if *nodeID == "" {
			return fmt.Errorf("--node-id is required when --raft-addr is set")
		}
		raftCfg := replication.RaftConfig{
			NodeID:       *nodeID,
			RaftAddr:     *raftAddr,
			JoinAddr:     *raftJoin,
			DataDir:      *dir,
			MemTableSize: *memSize,
		}
		cfg.Raft = &raftCfg
	}

	db, err := engine.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Prometheus metrics.
	var exp *metrics.Exporter
	if metricsListen != "" {
		exp = metrics.NewExporter(metricsListen)
		exp.Register(db)
		if err := exp.Start(); err != nil {
			return fmt.Errorf("metrics: %w", err)
		}
		defer exp.Stop()
		log.Info("metrics server", "addr", metricsListen)
	}

	log.Info("flashDB server started",
		"dir", *dir,
		"raft", *raftAddr,
		"metrics", metricsListen,
	)

	// Wait for signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info("shutting down...")
	return nil
}
