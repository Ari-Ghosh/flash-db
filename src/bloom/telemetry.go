// Package bloom – telemetry tracks per-SSTable false-positive rates and
// recommends adaptive bloom-filter sizing for new SSTables.
//
// ## Design
//
// Every L0 SSTable read that passes the bloom filter is recorded.  When the
// actual disk lookup reveals the key is absent the event is counted as a
// false positive.  The aggregate observed FPR across all tracked SSTables
// drives the FPR target for the *next* SSTable that is flushed.
//
// Lock-free atomics (sync/atomic.Uint64) are used for per-path counters so
// the hot read path pays almost zero synchronisation cost.  The sync.Map
// backing store is also contention-friendly for the read-heavy / write-rare
// access pattern we have here (paths are added once and read many times).
package bloom

import (
	"math"
	"sync"
	"sync/atomic"
)

// FilterStats holds per-SSTable bloom filter telemetry.
// All fields are updated atomically; no mutex required on the read path.
type FilterStats struct {
	Queries        atomic.Uint64 // bloom said "maybe" → triggered a disk read
	FalsePositives atomic.Uint64 // disk read confirmed key absent
}

// ObservedFPR returns the measured false-positive rate for this SSTable.
// Returns 0 when there have been no queries.
func (s *FilterStats) ObservedFPR() float64 {
	q := s.Queries.Load()
	if q == 0 {
		return 0
	}
	return float64(s.FalsePositives.Load()) / float64(q)
}

// BloomTelemetry aggregates false-positive telemetry across all live SSTables
// and recommends an adaptive FPR for newly created bloom filters.
type BloomTelemetry struct {
	stats sync.Map // path (string) → *FilterStats
}

// NewBloomTelemetry returns an initialised telemetry tracker.
func NewBloomTelemetry() *BloomTelemetry {
	return &BloomTelemetry{}
}

// RecordQuery notes that a bloom filter on path returned "maybe present",
// causing a disk read.
func (t *BloomTelemetry) RecordQuery(path string) {
	s := t.getOrCreate(path)
	s.Queries.Add(1)
}

// RecordFalsePositive notes that the disk read triggered by the bloom filter
// on path found that the key was actually absent.
func (t *BloomTelemetry) RecordFalsePositive(path string) {
	s := t.getOrCreate(path)
	s.FalsePositives.Add(1)
}

// Stats returns the FilterStats for path, or nil if not tracked.
func (t *BloomTelemetry) Stats(path string) *FilterStats {
	if v, ok := t.stats.Load(path); ok {
		return v.(*FilterStats)
	}
	return nil
}

// Remove deletes the telemetry entry for path.
// Called when an SSTable is removed by compaction.
func (t *BloomTelemetry) Remove(path string) {
	t.stats.Delete(path)
}

// AggregateStats returns total queries and total false positives across every
// tracked SSTable.
func (t *BloomTelemetry) AggregateStats() (totalQueries, totalFP uint64) {
	t.stats.Range(func(_, value any) bool {
		s := value.(*FilterStats)
		totalQueries += s.Queries.Load()
		totalFP += s.FalsePositives.Load()
		return true
	})
	return
}

// minQueriesForAdaptation is the minimum number of bloom queries we need
// before we start adapting.  Below this threshold we return the default
// target to avoid noisy adjustments.
const minQueriesForAdaptation = 100

// Recommend computes the FPR that should be used for the next bloom filter
// based on aggregate telemetry.
//
// Parameters:
//   - targetFPR: the baseline / default false-positive rate (e.g. 0.01).
//   - minFPR:    lower bound — never go below this (prevents over-allocation).
//   - maxFPR:    upper bound — never exceed this (prevents under-allocation).
//
// Algorithm:
//   - If not enough data (< minQueriesForAdaptation queries), return targetFPR.
//   - If observed FPR > targetFPR        → halve the target (bigger filter).
//   - If observed FPR < targetFPR * 0.25 → double the target (smaller filter).
//   - Otherwise keep target unchanged.
//   - Clamp result to [minFPR, maxFPR].
func (t *BloomTelemetry) Recommend(targetFPR, minFPR, maxFPR float64) float64 {
	totalQ, totalFP := t.AggregateStats()
	if totalQ < minQueriesForAdaptation {
		return targetFPR
	}

	observed := float64(totalFP) / float64(totalQ)
	recommended := targetFPR

	switch {
	case observed > targetFPR:
		// FPR too high → make filter bigger (lower FPR target).
		recommended = targetFPR * 0.5
	case observed < targetFPR*0.25:
		// FPR much lower than needed → allow a smaller filter.
		recommended = targetFPR * 2.0
	}

	return math.Max(minFPR, math.Min(maxFPR, recommended))
}

// getOrCreate returns the *FilterStats for path, creating it if necessary.
func (t *BloomTelemetry) getOrCreate(path string) *FilterStats {
	if v, ok := t.stats.Load(path); ok {
		return v.(*FilterStats)
	}
	v, _ := t.stats.LoadOrStore(path, &FilterStats{})
	return v.(*FilterStats)
}
