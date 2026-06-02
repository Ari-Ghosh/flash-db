package tests

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/metrics"
)

func TestMetricsExporter(t *testing.T) {
	addr := freePort(t)

	exporter := metrics.NewExporter(addr)

	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	exporter.Register(db)

	if err := exporter.Start(); err != nil {
		t.Fatalf("failed to start exporter: %v", err)
	}
	defer exporter.Stop()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Make a request to the metrics endpoint
	resp, err := http.Get("http://localhost" + addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	bodyStr := string(body)

	// Check for expected metrics
	expectedMetrics := []string{
		"flashdb_seq_num",
		"flashdb_memtable_size_bytes",
		"flashdb_memtable_count",
		"flashdb_l0_file_count",
		"flashdb_bloom_total_queries",
		"flashdb_bloom_false_positives",
		"flashdb_bloom_current_fpr",
		"flashdb_puts_total",
		"flashdb_deletes_total",
		"flashdb_gets_total",
		"flashdb_compaction_l0_merges_total",
		"flashdb_compaction_l1_merges_total",
		"flashdb_wal_syncs_total",
		"flashdb_replication_follower_count",
		"flashdb_replication_connected",
		"flashdb_replication_last_applied_seq",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(bodyStr, metric) {
			t.Errorf("expected metric %q not found in response", metric)
		}
	}
}

func TestMetricsCountersIncrement(t *testing.T) {
	addr := freePort(t)
	exporter := metrics.NewExporter(addr)

	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	exporter.Register(db)
	if err := exporter.Start(); err != nil {
		t.Fatalf("failed to start exporter: %v", err)
	}
	defer exporter.Stop()

	time.Sleep(100 * time.Millisecond)

	// Perform some operations to increment atomic counters
	mustPut(t, db, "key1", "val1")
	mustPut(t, db, "key2", "val2")
	_ = db.Delete([]byte("key1"))
	mustGet(t, db, "key2", "val2")

	// Fetch metrics
	resp, err := http.Get("http://localhost" + addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify counters incremented correctly
	if !strings.Contains(bodyStr, "flashdb_puts_total 2") {
		t.Errorf("expected 2 puts in metrics, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "flashdb_deletes_total 1") {
		t.Errorf("expected 1 delete in metrics, got:\n%s", bodyStr)
	}
	if !strings.Contains(bodyStr, "flashdb_gets_total 1") {
		t.Errorf("expected 1 get in metrics, got:\n%s", bodyStr)
	}
}

func TestMetricsMultipleCollectors(t *testing.T) {
	addr := freePort(t)
	exporter := metrics.NewExporter(addr)

	db1 := openDB(t, tmpDir(t))
	defer func() { _ = db1.Close() }()

	db2 := openDB(t, tmpDir(t))
	defer func() { _ = db2.Close() }()

	exporter.Register(db1)
	exporter.Register(db2)

	if err := exporter.Start(); err != nil {
		t.Fatalf("failed to start exporter: %v", err)
	}
	defer exporter.Stop()

	time.Sleep(100 * time.Millisecond)

	// Perform operations on both DBs
	mustPut(t, db1, "k1", "v1")
	mustPut(t, db2, "k2", "v2")
	mustPut(t, db2, "k3", "v3")

	resp, err := http.Get("http://localhost" + addr + "/metrics")
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Counters should aggregate across all registered collectors
	if !strings.Contains(bodyStr, "flashdb_puts_total 3") {
		t.Errorf("expected 3 total puts in metrics, got:\n%s", bodyStr)
	}
}
