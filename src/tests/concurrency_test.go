package tests

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentPuts(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	const goroutines = 20
	const perGoroutine = 100
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		// g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				k := fmt.Sprintf("g%d:k%d", g, i)
				if err := db.Put([]byte(k), []byte("val")); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("concurrent Put error: %v", e)
	}

	// Spot-check a few keys.
	for g := 0; g < goroutines; g++ {
		k := fmt.Sprintf("g%d:k0", g)
		mustGet(t, db, k, "val")
	}
}

func TestConcurrentPutGet(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	const n = 50
	var wg sync.WaitGroup

	// Writers.
	for i := 0; i < n; i++ {
		// i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Put([]byte(fmt.Sprintf("ck%d", i)), []byte("v"))
		}()
	}
	// Readers — should not panic or data-race.
	for i := 0; i < n; i++ {
		// i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = db.Get([]byte(fmt.Sprintf("ck%d", i)))
		}()
	}
	wg.Wait()
}
