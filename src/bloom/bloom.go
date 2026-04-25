// Package bloom provides a simple Bloom filter used to skip B-tree reads
// when a key is definitely absent.
//
// ## Overview
//
// Bloom filters provide fast negative lookups by probabilistically testing
// key presence. They use multiple hash functions to set bits in a bitset,
// enabling O(1) checks that guarantee absence but allow false positives.
//
// ## Architecture
//
// - **Kirsch-Mitzenmacher**: Two base hashes generate k independent hashes.
// - **Fixed Size**: Pre-allocated bitset based on expected elements.
// - **No False Negatives**: If filter says "no", key is definitely absent.
// - **Configurable FPR**: Trade space for accuracy (~10 bits/element typical).
//
// ## v1 vs v2 Changes
//
// ### v1 (Original Implementation)
// - No bloom filters; all Get operations scanned B-tree.
// - Used for basic key-value storage without optimizations.
//
// ### v2 (Enhanced Implementation)
// - **Bloom Filters**: Added to SSTable footers for fast negative lookups.
// - **Kirsch-Mitzenmacher**: Efficient multiple hash generation.
// - **Optimal Sizing**: Automatic k/m calculation for target FPR.
// - **Space Efficient**: ~1-2 bytes per element at 1% FPR.
//
// ## Key Methods
//
// - **New(n, fpr)**: Create filter for n elements at fpr false positive rate.
// - **Add(key)**: Insert key into filter.
// - **Has(key)**: Test key presence (may be false positive).
// - **optimalK/M**: Calculate optimal parameters.
//
// ## Hash Functions
//
// Uses Kirsch-Mitzenmacher optimization:
// - Hash1(key) → base hash
// - Hash2(key) → secondary hash
// - Hash_i = Hash1 + i * Hash2 (mod m)
//
// ## Performance Characteristics
//
// - **Space**: ~1.44 * ln(1/fpr) bits per element.
// - **Time**: O(k) for Add/Has, k typically 4-10.
// - **FPR**: Configurable, ~1% typical for good space/time balance.
// - **No Deletes**: Filters don't support removal.
//
// ## Usage in HybridDB
//
// Bloom filters optimize read path:
// - SSTable.Get() checks bloom first – skip file if "no".
// - Iterator.Seek() uses bloom to skip irrelevant SSTables.
// - Reduces I/O by avoiding unnecessary B-tree traversals.
package bloom

import (
	"encoding/binary"
	"math"
)

// Filter is a fixed-size Bloom filter.
type Filter struct {
	bits []uint64 // bitset stored in 64-bit words
	k    uint     // number of hash functions
	m    uint     // total bit count
}

// New creates a filter sized for n expected elements at fpr false-positive rate.
func New(n uint, fpr float64) *Filter {
	m := optimalM(n, fpr)
	k := optimalK(m, n)
	return &Filter{
		bits: make([]uint64, (m+63)/64),
		k:    k,
		m:    m,
	}
}

// Add inserts key into the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := baseHashes(key)
	for i := uint(0); i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(f.m)
		f.bits[bit/64] |= 1 << (bit % 64)
	}
}

// MayContain returns true if key might be in the set.
// A false return guarantees the key is absent.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := baseHashes(key)
	for i := uint(0); i < f.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(f.m)
		if f.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// Bytes serialises the filter for persistence alongside B-tree metadata.
func (f *Filter) Bytes() []byte {
	out := make([]byte, 4+4+len(f.bits)*8)
	binary.LittleEndian.PutUint32(out[0:], uint32(f.m))
	binary.LittleEndian.PutUint32(out[4:], uint32(f.k))
	for i, w := range f.bits {
		binary.LittleEndian.PutUint64(out[8+i*8:], w)
	}
	return out
}

// FromBytes deserialises a filter produced by Bytes().
func FromBytes(b []byte) *Filter {
	if len(b) < 8 {
		return nil
	}
	m := uint(binary.LittleEndian.Uint32(b[0:]))
	k := uint(binary.LittleEndian.Uint32(b[4:]))
	words := (m + 63) / 64
	bits := make([]uint64, words)
	for i := range bits {
		if 8+i*8+8 > len(b) {
			break
		}
		bits[i] = binary.LittleEndian.Uint64(b[8+i*8:])
	}
	return &Filter{bits: bits, k: k, m: m}
}

// FalsePositiveRate returns the current theoretical FPR.
func (f *Filter) FalsePositiveRate(n uint) float64 {
	exp := -float64(f.k) * float64(n) / float64(f.m)
	return math.Pow(1-math.Exp(exp), float64(f.k))
}

// ── helpers ───────────────────────────────────────────────────────────────

func optimalM(n uint, fpr float64) uint {
	m := -float64(n) * math.Log(fpr) / (math.Log(2) * math.Log(2))
	return uint(math.Ceil(m))
}

func optimalK(m, n uint) uint {
	k := float64(m) / float64(n) * math.Log(2)
	return uint(math.Max(1, math.Round(k)))
}

// baseHashes returns two 64-bit hashes for the Kirsch-Mitzenmacher trick.
// We use FNV-like mixing to avoid importing crypto packages.
func baseHashes(key []byte) (uint64, uint64) {
	var h1 uint64 = 14695981039346656037

	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= 1099511628211
	}
	// second independent hash via a different seed
	h2 := h1 ^ 0x9e3779b97f4a7c15
	h2 ^= h2 >> 30
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 27
	h2 *= 0x94d049bb133111eb
	h2 ^= h2 >> 31
	return h1, h2
}
