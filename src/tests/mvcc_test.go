package tests

import (
	"testing"

	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestSnapshotIsolation(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "x", "before")
	snap := db.NewSnapshot()
	defer func() { snap.Release() }()

	mustPut(t, db, "x", "after")

	// Snapshot should still see "before".
	old, err := db.GetSnapshot(snap, []byte("x"))
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if string(old) != "before" {
		t.Fatalf("snapshot saw %q, want %q", old, "before")
	}

	// Latest should see "after".
	mustGet(t, db, "x", "after")
}

func TestSnapshotNotAffectedByDelete(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "gone", "here")
	snap := db.NewSnapshot()
	defer func() { snap.Release() }()

	_ = db.Delete([]byte("gone"))

	// Snapshot should still see "here".
	v, err := db.GetSnapshot(snap, []byte("gone"))
	if err != nil {
		t.Fatalf("snapshot should see deleted key: %v", err)
	}
	if string(v) != "here" {
		t.Fatalf("snapshot got %q, want %q", v, "here")
	}

	// Current view should see deletion.
	mustNotFound(t, db, "gone")
}

func TestMultipleSnapshotsIndependent(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	mustPut(t, db, "v", "0")
	s0 := db.NewSnapshot()

	mustPut(t, db, "v", "1")
	s1 := db.NewSnapshot()

	mustPut(t, db, "v", "2")

	check := func(snap *types.Snapshot, want string) {
		t.Helper()
		got, err := db.GetSnapshot(snap, []byte("v"))
		if err != nil || string(got) != want {
			t.Fatalf("snap %v: got %q err %v, want %q", snap.ID(), got, err, want)
		}
	}
	check(s0, "0")
	check(s1, "1")
	mustGet(t, db, "v", "2")

	s0.Release()
	s1.Release()
}

func TestSnapshotTrackerOldestSeq(t *testing.T) {
	tracker := types.NewSnapshotTracker()

	if oldest := tracker.OldestPinnedSeq(); oldest != ^uint64(0) {
		t.Fatalf("empty tracker: expected max uint64, got %d", oldest)
	}

	s10 := tracker.Create(10)
	s5 := tracker.Create(5)
	s20 := tracker.Create(20)

	if oldest := tracker.OldestPinnedSeq(); oldest != 5 {
		t.Fatalf("expected oldest=5, got %d", oldest)
	}

	s5.Release()
	if oldest := tracker.OldestPinnedSeq(); oldest != 10 {
		t.Fatalf("after s5 release, expected oldest=10, got %d", oldest)
	}

	s10.Release()
	s20.Release()
	if oldest := tracker.OldestPinnedSeq(); oldest != ^uint64(0) {
		t.Fatalf("all released: expected max, got %d", oldest)
	}
}

func TestMVCCSnapshotRegression(t *testing.T) {
	db := openDB(t, tmpDir(t))
	mustPut(t, db, "x", "before")
	snap := db.NewSnapshot()
	defer func() { snap.Release() }()
	mustPut(t, db, "x", "after")

	old, err := db.GetSnapshot(snap, []byte("x"))
	if err != nil || string(old) != "before" {
		t.Fatalf("snapshot regression: got %q %v", old, err)
	}
	mustGet(t, db, "x", "after")
}
