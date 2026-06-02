package tests

import (
	"testing"
	"time"
)

func TestTTLExpiry(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.PutWithTTL([]byte("expires"), []byte("soon"), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// Key should be visible immediately.
	mustGet(t, db, "expires", "soon")

	// After TTL elapses it should vanish.
	time.Sleep(100 * time.Millisecond)
	mustNotFound(t, db, "expires")
}

func TestTTLNotExpiredYet(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.PutWithTTL([]byte("long"), []byte("lived"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "long", "lived")
}

func TestTTLOf(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	if err := db.PutWithTTL([]byte("ttlcheck"), []byte("v"), time.Second); err != nil {
		t.Fatal(err)
	}
	remaining, err := db.TTLOf([]byte("ttlcheck"))
	if err != nil {
		t.Fatalf("TTLOf: %v", err)
	}
	if remaining <= 0 || remaining > time.Second {
		t.Fatalf("unexpected remaining TTL: %v", remaining)
	}
}

func TestTTLOfNoTTL(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "notttl", "v")
	_, err := db.TTLOf([]byte("notttl"))
	if err == nil {
		t.Fatal("expected error for key with no TTL")
	}
}

func TestExpireAt(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "expireat", "v")

	deadline := time.Now().Add(30 * time.Millisecond)
	if err := db.ExpireAt([]byte("expireat"), deadline); err != nil {
		t.Fatal(err)
	}

	mustGet(t, db, "expireat", "v")
	time.Sleep(80 * time.Millisecond)
	mustNotFound(t, db, "expireat")
}

func TestTTLColumnFamilyKey(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_ = db.CreateColumnFamily("ttlcf")
	cf, _ := db.GetColumnFamily("ttlcf")

	if err := cf.PutWithTTL([]byte("tempkey"), []byte("temp"), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	v, err := cf.Get([]byte("tempkey"))
	if err != nil || string(v) != "temp" {
		t.Fatalf("cf TTL get: got %q %v", v, err)
	}
	time.Sleep(100 * time.Millisecond)
	_, err = cf.Get([]byte("tempkey"))
	if err == nil {
		t.Fatal("expected CF key to expire")
	}
}

func TestTTLDoesNotAffectOtherKeys(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "permanent", "stays")
	if err := db.PutWithTTL([]byte("temp"), []byte("goes"), 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	time.Sleep(60 * time.Millisecond)

	mustGet(t, db, "permanent", "stays")
	mustNotFound(t, db, "temp")
}

func TestTTLOverwrite(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	// Write with short TTL, then overwrite with a long TTL.
	if err := db.PutWithTTL([]byte("overwrite"), []byte("v1"), 30*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	// Immediately overwrite with longer TTL.
	if err := db.PutWithTTL([]byte("overwrite"), []byte("v2"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	// Key should still be visible with the new value.
	mustGet(t, db, "overwrite", "v2")
}
