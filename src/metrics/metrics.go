// Package metrics provides a Prometheus metrics exporter for flashDB.
//
// An Exporter runs an HTTP server on a configurable address exposing the
// /metrics endpoint.  The exporter is opt-in: applications call
// NewExporter, Register their DB, and Start/Stop as needed.
package metrics

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collectors is the interface the exporter needs from a DB instance.
type Collectors interface {
	SeqNum() uint64
	MemTableSize() int64
	MemTableCount() int64
	L0FileCount() int
	BloomTotalQueries() uint64
	BloomFalsePositives() uint64
	BloomCurrentFPR() float64
	PutCount() uint64
	DeleteCount() uint64
	GetCount() uint64
	L0MergeCount() uint64
	L1MergeCount() uint64
	WALSyncCount() uint64
	FollowerCount() int
	IsFollowerConnected() bool
	FollowerLastSeq() uint64
}

// Exporter runs a Prometheus HTTP metrics endpoint.
type Exporter struct {
	addr   string
	srv    *http.Server
	stopCh chan struct{}
	wg     sync.WaitGroup

	mu         sync.RWMutex
	collectors []Collectors
}

// NewExporter creates a metrics exporter that will listen on addr (e.g. ":9090").
func NewExporter(addr string) *Exporter {
	return &Exporter{
		addr:   addr,
		stopCh: make(chan struct{}),
	}
}

// Register adds a DB's collectors to the set scraped on each /metrics request.
func (e *Exporter) Register(c Collectors) {
	e.mu.Lock()
	e.collectors = append(e.collectors, c)
	e.mu.Unlock()
}

// Start begins serving the /metrics endpoint in a background goroutine.
func (e *Exporter) Start() error {
	col := newDBCollector()
	col.exporter = e

	reg := prometheus.NewRegistry()
	reg.MustRegister(col)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	e.srv = &http.Server{Addr: e.addr, Handler: mux}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("metrics: server error: %v\n", err)
		}
	}()
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (e *Exporter) Stop() {
	close(e.stopCh)
	if e.srv != nil {
		_ = e.srv.Close()
	}
	e.wg.Wait()
}

// dbCollector implements prometheus.Collector to generate metrics on each scrape.
type dbCollector struct {
	exporter *Exporter

	seqNum            *prometheus.Desc
	memTableSize      *prometheus.Desc
	memTableCount     *prometheus.Desc
	l0FileCount       *prometheus.Desc
	bloomTotalQueries *prometheus.Desc
	bloomFalsePos     *prometheus.Desc
	bloomCurrentFPR   *prometheus.Desc
	putTotal          *prometheus.Desc
	deleteTotal       *prometheus.Desc
	getTotal          *prometheus.Desc
	l0MergeTotal      *prometheus.Desc
	l1MergeTotal      *prometheus.Desc
	walSyncTotal      *prometheus.Desc
	followerCount     *prometheus.Desc
	followerConnected *prometheus.Desc
	followerLastSeq   *prometheus.Desc
}

func newDBCollector() *dbCollector {
	labels := []string{} // no label dimensions needed for single-DB metrics
	return &dbCollector{
		seqNum: prometheus.NewDesc(
			"flashdb_seq_num", "Current committed sequence number.", labels, nil,
		),
		memTableSize: prometheus.NewDesc(
			"flashdb_memtable_size_bytes", "Active MemTable size in bytes.", labels, nil,
		),
		memTableCount: prometheus.NewDesc(
			"flashdb_memtable_count", "Number of keys in the active MemTable.", labels, nil,
		),
		l0FileCount: prometheus.NewDesc(
			"flashdb_l0_file_count", "Number of L0 SSTable files on disk.", labels, nil,
		),
		bloomTotalQueries: prometheus.NewDesc(
			"flashdb_bloom_total_queries", "Total bloom filter queries.", labels, nil,
		),
		bloomFalsePos: prometheus.NewDesc(
			"flashdb_bloom_false_positives", "Total bloom false positives.", labels, nil,
		),
		bloomCurrentFPR: prometheus.NewDesc(
			"flashdb_bloom_current_fpr", "Adaptive FPR target for next bloom filter.", labels, nil,
		),
		putTotal: prometheus.NewDesc(
			"flashdb_puts_total", "Total Put operations.", labels, nil,
		),
		deleteTotal: prometheus.NewDesc(
			"flashdb_deletes_total", "Total Delete operations.", labels, nil,
		),
		getTotal: prometheus.NewDesc(
			"flashdb_gets_total", "Total Get operations.", labels, nil,
		),
		l0MergeTotal: prometheus.NewDesc(
			"flashdb_compaction_l0_merges_total", "Total L0→L1 compaction merges.", labels, nil,
		),
		l1MergeTotal: prometheus.NewDesc(
			"flashdb_compaction_l1_merges_total", "Total L1→L2 compaction merges.", labels, nil,
		),
		walSyncTotal: prometheus.NewDesc(
			"flashdb_wal_syncs_total", "Total WAL fsync calls.", labels, nil,
		),
		followerCount: prometheus.NewDesc(
			"flashdb_replication_follower_count", "Number of connected followers (leader only).", labels, nil,
		),
		followerConnected: prometheus.NewDesc(
			"flashdb_replication_connected", "1 if connected to leader, 0 otherwise (follower only).", labels, nil,
		),
		followerLastSeq: prometheus.NewDesc(
			"flashdb_replication_last_applied_seq", "Last applied sequence number (follower only).", labels, nil,
		),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.seqNum
	ch <- c.memTableSize
	ch <- c.memTableCount
	ch <- c.l0FileCount
	ch <- c.bloomTotalQueries
	ch <- c.bloomFalsePos
	ch <- c.bloomCurrentFPR
	ch <- c.putTotal
	ch <- c.deleteTotal
	ch <- c.getTotal
	ch <- c.l0MergeTotal
	ch <- c.l1MergeTotal
	ch <- c.walSyncTotal
	ch <- c.followerCount
	ch <- c.followerConnected
	ch <- c.followerLastSeq
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	c.exporter.mu.RLock()
	cols := make([]Collectors, len(c.exporter.collectors))
	copy(cols, c.exporter.collectors)
	c.exporter.mu.RUnlock()

	var (
		seqNum                  uint64
		memSize, memCount       int64
		l0Count                 int
		bloomQ, bloomFP         uint64
		bloomFPR                float64
		putC, delC, getC        uint64
		l0Merge, l1Merge        uint64
		walSync                 uint64
		fCount, fConnected      int
		lastSeq                 uint64
	)

	for _, col := range cols {
		if s := col.SeqNum(); s > seqNum {
			seqNum = s
		}
		memSize += col.MemTableSize()
		memCount += col.MemTableCount()
		l0Count += col.L0FileCount()
		bloomQ += col.BloomTotalQueries()
		bloomFP += col.BloomFalsePositives()
		if f := col.BloomCurrentFPR(); f > 0 {
			bloomFPR = f
		}
		putC += col.PutCount()
		delC += col.DeleteCount()
		getC += col.GetCount()
		l0Merge += col.L0MergeCount()
		l1Merge += col.L1MergeCount()
		walSync += col.WALSyncCount()
		fCount += col.FollowerCount()
		if col.IsFollowerConnected() {
			fConnected++
		}
		if s := col.FollowerLastSeq(); s > lastSeq {
			lastSeq = s
		}
	}

	ch <- prometheus.MustNewConstMetric(c.seqNum, prometheus.GaugeValue, float64(seqNum))
	ch <- prometheus.MustNewConstMetric(c.memTableSize, prometheus.GaugeValue, float64(memSize))
	ch <- prometheus.MustNewConstMetric(c.memTableCount, prometheus.GaugeValue, float64(memCount))
	ch <- prometheus.MustNewConstMetric(c.l0FileCount, prometheus.GaugeValue, float64(l0Count))
	ch <- prometheus.MustNewConstMetric(c.bloomTotalQueries, prometheus.CounterValue, float64(bloomQ))
	ch <- prometheus.MustNewConstMetric(c.bloomFalsePos, prometheus.CounterValue, float64(bloomFP))
	ch <- prometheus.MustNewConstMetric(c.bloomCurrentFPR, prometheus.GaugeValue, bloomFPR)
	ch <- prometheus.MustNewConstMetric(c.putTotal, prometheus.CounterValue, float64(putC))
	ch <- prometheus.MustNewConstMetric(c.deleteTotal, prometheus.CounterValue, float64(delC))
	ch <- prometheus.MustNewConstMetric(c.getTotal, prometheus.CounterValue, float64(getC))
	ch <- prometheus.MustNewConstMetric(c.l0MergeTotal, prometheus.CounterValue, float64(l0Merge))
	ch <- prometheus.MustNewConstMetric(c.l1MergeTotal, prometheus.CounterValue, float64(l1Merge))
	ch <- prometheus.MustNewConstMetric(c.walSyncTotal, prometheus.CounterValue, float64(walSync))
	ch <- prometheus.MustNewConstMetric(c.followerCount, prometheus.GaugeValue, float64(fCount))
	ch <- prometheus.MustNewConstMetric(c.followerConnected, prometheus.GaugeValue, float64(fConnected))
	ch <- prometheus.MustNewConstMetric(c.followerLastSeq, prometheus.GaugeValue, float64(lastSeq))
}
