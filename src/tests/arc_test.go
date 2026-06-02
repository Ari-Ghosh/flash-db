package tests

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Ari-Ghosh/flash-db/src/arc"
	"github.com/Ari-Ghosh/flash-db/src/btree"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestARCBasicGetPut(t *testing.T) {
	c := arc.New[int](4)

	c.Put(1, 100)
	c.Put(2, 200)

	if v, ok := c.Get(1); !ok || v != 100 {
		t.Fatalf("Get(1) = %v %v, want 100 true", v, ok)
	}
	if v, ok := c.Get(2); !ok || v != 200 {
		t.Fatalf("Get(2) = %v %v, want 200 true", v, ok)
	}
	if _, ok := c.Get(99); ok {
		t.Fatal("Get(99) should be a miss")
	}
}

func TestARCCapacityEviction(t *testing.T) {
	c := arc.New[int](3)

	// Fill cache.
	c.Put(1, 1)
	c.Put(2, 2)
	c.Put(3, 3)
	// Access 1 and 2 to make them "frequently used".
	c.Get(1)
	c.Get(2)
	// Insert a new key – should evict 3 (least recently / frequently used).
	c.Put(4, 4)

	if c.Len() > 3 {
		t.Fatalf("cache grew beyond capacity: len=%d", c.Len())
	}
}

func TestARCUpdate(t *testing.T) {
	c := arc.New[string](4)
	c.Put(1, "a")
	c.Put(1, "b") // update
	v, ok := c.Get(1)
	if !ok || v != "b" {
		t.Fatalf("after update: got %q %v, want b true", v, ok)
	}
}

func TestARCRemove(t *testing.T) {
	c := arc.New[int](4)
	c.Put(1, 10)
	c.Remove(1)
	if _, ok := c.Get(1); ok {
		t.Fatal("key should be gone after Remove")
	}
}

func TestARCAdaptation(t *testing.T) {
	// Demonstrate that ARC adapts: after a recency-heavy workload the cache
	// returns more recency hits; after a frequency-heavy workload it returns
	// more frequency hits.  We just verify it doesn't panic and the length
	// stays bounded.
	c := arc.New[int](16)
	for i := 0; i < 100; i++ {
		c.Put(uint64(i), i)
	}
	if c.Len() > 16 {
		t.Fatalf("cache len %d exceeds capacity 16", c.Len())
	}
	// Repeated access to a small hot set.
	for rep := 0; rep < 50; rep++ {
		for i := 0; i < 4; i++ {
			c.Get(uint64(i))
		}
	}
	if c.Len() > 16 {
		t.Fatalf("after hot-set reads, len %d exceeds capacity", c.Len())
	}
}

func TestARCBTreeIntegration(t *testing.T) {
	// Verify the B-tree uses the ARC cache for page lookups.
	dir := tmpDir(t)
	bt, err := btree.OpenWithCacheSize(filepath.Clean(dir+"/arc_test.bt"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bt.Close() }()

	var entries []types.Entry
	for i := 0; i < 200; i++ {
		entries = append(entries, types.Entry{
			Key:    []byte(fmt.Sprintf("k%04d", i)),
			Value:  []byte(fmt.Sprintf("v%d", i)),
			SeqNum: uint64(i + 1),
		})
	}
	if err := bt.BulkLoad(entries); err != nil {
		t.Fatal(err)
	}

	// Read all entries – warm the ARC cache.
	for _, e := range entries {
		got, err := bt.Get(e.Key)
		if err != nil {
			t.Fatalf("Get(%q): %v", e.Key, err)
		}
		if !bytes.Equal(got.Value, e.Value) {
			t.Fatalf("Get(%q) = %q, want %q", e.Key, got.Value, e.Value)
		}
	}

	// Read again – should come from ARC cache.
	for _, e := range entries[:10] {
		got, err := bt.Get(e.Key)
		if err != nil {
			t.Fatalf("second Get(%q): %v", e.Key, err)
		}
		if !bytes.Equal(got.Value, e.Value) {
			t.Fatalf("second Get(%q) = %q, want %q", e.Key, got.Value, e.Value)
		}
	}
}

func TestARCConcurrent(t *testing.T) {
	c := arc.New[int](64)
	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		// g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := uint64(g*200 + i)
				c.Put(key, i)
				c.Get(key)
			}
		}()
	}
	wg.Wait()
	if c.Len() > 64 {
		t.Fatalf("concurrent: cache len %d exceeds capacity 64", c.Len())
	}
}
