// Package arc implements an Adaptive Replacement Cache (ARC).
//
// ARC dynamically balances between two extremes:
//   - LRU (Least Recently Used): good for one-time sequential scans
//   - LFU (Least Frequently Used): good for repeated access to a hot set
//
// It does this by maintaining four internal lists:
//
//	T1  – pages seen exactly once recently ("recency ghost").
//	T2  – pages seen more than once recently ("frequency ghost").
//	B1  – ghost entries recently evicted from T1 (key only, no page).
//	B2  – ghost entries recently evicted from T2 (key only, no page).
//
// The adaptive parameter p (0 ≤ p ≤ capacity) is the target size for T1.
// A hit in B1 (a page we recently evicted from T1) means we should grow T1,
// so p is incremented.  A hit in B2 means we should grow T2, so p decreases.
// This self-tuning mechanism lets ARC outperform any fixed-ratio strategy.
//
// Concurrency: all public methods are protected by a single mutex.
// The cache is safe for concurrent use from multiple goroutines.
package arc

import (
	"container/list"
	"sync"
)

// Cache is a generic ARC cache keyed on uint64.
// V is the value type stored in the cache.
type Cache[V any] struct {
	mu       sync.Mutex
	capacity int
	p        int // target size for T1

	// Live data lists (key → *list.Element in the respective list)
	t1    *list.List
	t2    *list.List
	t1Map map[uint64]*list.Element
	t2Map map[uint64]*list.Element

	// Ghost lists (track evicted keys so we can adapt p)
	b1    *list.List
	b2    *list.List
	b1Map map[uint64]*list.Element
	b2Map map[uint64]*list.Element
}

type entry[V any] struct {
	key uint64
	val V
}

// New creates an ARC cache with the given capacity (maximum number of live entries).
// capacity must be ≥ 1.
func New[V any](capacity int) *Cache[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &Cache[V]{
		capacity: capacity,
		t1:       list.New(),
		t2:       list.New(),
		b1:       list.New(),
		b2:       list.New(),
		t1Map:    make(map[uint64]*list.Element, capacity),
		t2Map:    make(map[uint64]*list.Element, capacity),
		b1Map:    make(map[uint64]*list.Element, capacity*2),
		b2Map:    make(map[uint64]*list.Element, capacity*2),
	}
}

// Get returns the value for key and true if the key is present, otherwise the
// zero value and false.  A hit on T1 promotes the entry to T2 (now "frequently
// used").
func (c *Cache[V]) Get(key uint64) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// T2 hit – already in the "frequently used" list; move to front.
	if el, ok := c.t2Map[key]; ok {
		c.t2.MoveToFront(el)
		return el.Value.(*entry[V]).val, true
	}

	// T1 hit – first time we see it again; promote to T2.
	if el, ok := c.t1Map[key]; ok {
		v := el.Value.(*entry[V]).val
		// Remove from T1.
		c.t1.Remove(el)
		delete(c.t1Map, key)
		// Insert at front of T2.
		nel := c.t2.PushFront(&entry[V]{key: key, val: v})
		c.t2Map[key] = nel
		return v, true
	}

	var zero V
	return zero, false
}

// Put inserts key→val.  If the key is already present the value is updated in
// place.  If the cache is full, one entry is evicted according to ARC policy.
func (c *Cache[V]) Put(key uint64, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Already in T1: update value and promote to T2.
	if el, ok := c.t1Map[key]; ok {
		c.t1.Remove(el)
		delete(c.t1Map, key)
		nel := c.t2.PushFront(&entry[V]{key: key, val: val})
		c.t2Map[key] = nel
		return
	}

	// Already in T2: update value in place, move to front.
	if el, ok := c.t2Map[key]; ok {
		el.Value.(*entry[V]).val = val
		c.t2.MoveToFront(el)
		return
	}

	// Key is in ghost list B1: increase p (should cache more "recent" pages).
	if _, inB1 := c.b1Map[key]; inB1 {
		delta := 1
		if c.b1.Len() < c.b2.Len() {
			delta = c.b2.Len() / c.b1.Len()
		}
		c.p = min(c.p+delta, c.capacity)
		c.replace(key)
		c.removeFromGhost(c.b1, c.b1Map, key)
		nel := c.t2.PushFront(&entry[V]{key: key, val: val})
		c.t2Map[key] = nel
		return
	}

	// Key is in ghost list B2: decrease p (should cache more "frequent" pages).
	if _, inB2 := c.b2Map[key]; inB2 {
		delta := 1
		if c.b2.Len() < c.b1.Len() {
			delta = c.b1.Len() / c.b2.Len()
		}
		c.p = max(c.p-delta, 0)
		c.replace(key)
		c.removeFromGhost(c.b2, c.b2Map, key)
		nel := c.t2.PushFront(&entry[V]{key: key, val: val})
		c.t2Map[key] = nel
		return
	}

	// Key is brand new.
	t1t2 := c.t1.Len() + c.t2.Len()
	b1b2 := c.b1.Len() + c.b2.Len()

	if t1t2 >= c.capacity {
		// Cache is full – need to evict.
		if c.t1.Len() < c.capacity {
			// Evict from ghost list B1 if it's grown too large.
			if c.b1.Len() > 0 {
				back := c.b1.Back()
				c.b1.Remove(back)
				delete(c.b1Map, back.Value.(uint64))
			}
			c.replace(key)
		} else {
			// T1 alone is already full: just evict from T1.
			c.evictFromT1()
		}
	} else if t1t2 < c.capacity && t1t2+b1b2 >= c.capacity {
		// Cache is not full but the combined history is: trim ghost lists.
		if t1t2+b1b2 >= c.capacity*2 {
			if c.b2.Len() > 0 {
				back := c.b2.Back()
				c.b2.Remove(back)
				delete(c.b2Map, back.Value.(uint64))
			}
		}
		if c.b1.Len() > 0 {
			back := c.b1.Back()
			c.b1.Remove(back)
			delete(c.b1Map, back.Value.(uint64))
		}
	}

	// Insert into T1 (first time seeing this key).
	nel := c.t1.PushFront(&entry[V]{key: key, val: val})
	c.t1Map[key] = nel
}

// Remove evicts key from the cache (live lists only; ghost lists are left intact
// so adaptive history is preserved).
func (c *Cache[V]) Remove(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.t1Map[key]; ok {
		c.t1.Remove(el)
		delete(c.t1Map, key)
	}
	if el, ok := c.t2Map[key]; ok {
		c.t2.Remove(el)
		delete(c.t2Map, key)
	}
}

// Len returns the number of live entries.
func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t1.Len() + c.t2.Len()
}

// ── internal helpers ──────────────────────────────────────────────────────────

// replace evicts one entry from T1 or T2 and puts its key into the appropriate
// ghost list.  The key being inserted is passed so we can handle the edge case
// where it is already in a ghost list.
func (c *Cache[V]) replace(incomingKey uint64) {
	_, inB2 := c.b2Map[incomingKey]
	if c.t1.Len() > 0 && (c.t1.Len() > c.p || (inB2 && c.t1.Len() == c.p)) {
		c.evictFromT1()
	} else if c.t2.Len() > 0 {
		c.evictFromT2()
	} else if c.t1.Len() > 0 {
		c.evictFromT1()
	}
}

func (c *Cache[V]) evictFromT1() {
	if c.t1.Len() == 0 {
		return
	}
	back := c.t1.Back()
	key := back.Value.(*entry[V]).key
	c.t1.Remove(back)
	delete(c.t1Map, key)
	// Add to ghost list B1.
	nel := c.b1.PushFront(key)
	c.b1Map[key] = nel
}

func (c *Cache[V]) evictFromT2() {
	if c.t2.Len() == 0 {
		return
	}
	back := c.t2.Back()
	key := back.Value.(*entry[V]).key
	c.t2.Remove(back)
	delete(c.t2Map, key)
	// Add to ghost list B2.
	nel := c.b2.PushFront(key)
	c.b2Map[key] = nel
}

func (c *Cache[V]) removeFromGhost(l *list.List, m map[uint64]*list.Element, key uint64) {
	if el, ok := m[key]; ok {
		l.Remove(el)
		delete(m, key)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}