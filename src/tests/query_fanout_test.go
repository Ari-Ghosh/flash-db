package tests

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/engine"
	"github.com/Ari-Ghosh/flash-db/src/replication"
	types "github.com/Ari-Ghosh/flash-db/src/types"
)

func TestFanOutQueryLocalAndRemote(t *testing.T) {
	secret := []byte("test-secret-key-32bytes-xxxxxxxxx")

	leaderDir := tmpDir(t)
	followerDir := tmpDir(t)

	addr := freePort(t)

	// Open leader DB with replication.
	leaderCfg := engine.DefaultConfig(leaderDir)
	leaderCfg.Replication = &replication.Config{
		Role:       "leader",
		ListenAddr: addr,
		Secret:     secret,
	}
	leaderDB, err := engine.Open(leaderCfg)
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leaderDB.Close()

	time.Sleep(20 * time.Millisecond)

	// Open follower DB.
	followerCfg := engine.DefaultConfig(followerDir)
	followerCfg.Replication = &replication.Config{
		Role:              "follower",
		LeaderAddr:        addr,
		Secret:            secret,
		DialTimeout:       1 * time.Second,
		ReconnectInterval: 50 * time.Millisecond,
	}
	followerDB, err := engine.Open(followerCfg)
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer followerDB.Close()

	time.Sleep(50 * time.Millisecond)

	// Write on leader.
	for i := 0; i < 20; i++ {
		mustPut(t, leaderDB, fmt.Sprintf("fanout:%02d", i), fmt.Sprintf("v%d", i))
	}

	// Give replication a moment to propagate to follower.
	time.Sleep(100 * time.Millisecond)

	// Execute fan-out query.
	iter, err := leaderDB.FanOut(types.IteratorOptions{})
	if err != nil {
		t.Fatalf("FanOut: %v", err)
	}
	defer func() { _ = iter.Close() }()

	var gotKeys []string
	for iter.Valid() {
		gotKeys = append(gotKeys, string(iter.Key()))
		iter.Next()
	}

	// The merged iterator should deduplicate keys from leader and follower.
	// We expect exactly 20 unique keys.
	if len(gotKeys) != 20 {
		t.Fatalf("expected 20 unique keys, got %d: %v", len(gotKeys), gotKeys)
	}

	sort.Strings(gotKeys)
	for i := 0; i < 20; i++ {
		want := fmt.Sprintf("fanout:%02d", i)
		if gotKeys[i] != want {
			t.Fatalf("key[%d]: got %q, want %q", i, gotKeys[i], want)
		}
	}
}

func TestFanOutQueryWithBounds(t *testing.T) {
	secret := []byte("test-secret-key-32bytes-xxxxxxxxx")

	leaderDir := tmpDir(t)
	followerDir := tmpDir(t)

	addr := freePort(t)

	leaderCfg := engine.DefaultConfig(leaderDir)
	leaderCfg.Replication = &replication.Config{
		Role:       "leader",
		ListenAddr: addr,
		Secret:     secret,
	}
	leaderDB, err := engine.Open(leaderCfg)
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leaderDB.Close()

	time.Sleep(20 * time.Millisecond)

	followerCfg := engine.DefaultConfig(followerDir)
	followerCfg.Replication = &replication.Config{
		Role:              "follower",
		LeaderAddr:        addr,
		Secret:            secret,
		DialTimeout:       1 * time.Second,
		ReconnectInterval: 50 * time.Millisecond,
	}
	followerDB, err := engine.Open(followerCfg)
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer followerDB.Close()

	time.Sleep(50 * time.Millisecond)

	// Write keys a, b, c, d, e
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		mustPut(t, leaderDB, k, "val")
	}

	time.Sleep(100 * time.Millisecond)

	// Fan-out query with bounds [b, d)
	iter, err := leaderDB.FanOut(types.IteratorOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	if err != nil {
		t.Fatalf("FanOut: %v", err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}

	want := []string{"b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("bounds: got %v, want %v", got, want)
	}
}

func TestFanOutQueryWithPrefix(t *testing.T) {
	secret := []byte("test-secret-key-32bytes-xxxxxxxxx")

	leaderDir := tmpDir(t)
	followerDir := tmpDir(t)

	addr := freePort(t)

	leaderCfg := engine.DefaultConfig(leaderDir)
	leaderCfg.Replication = &replication.Config{
		Role:       "leader",
		ListenAddr: addr,
		Secret:     secret,
	}
	leaderDB, err := engine.Open(leaderCfg)
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leaderDB.Close()

	time.Sleep(20 * time.Millisecond)

	followerCfg := engine.DefaultConfig(followerDir)
	followerCfg.Replication = &replication.Config{
		Role:              "follower",
		LeaderAddr:        addr,
		Secret:            secret,
		DialTimeout:       1 * time.Second,
		ReconnectInterval: 50 * time.Millisecond,
	}
	followerDB, err := engine.Open(followerCfg)
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer followerDB.Close()

	time.Sleep(50 * time.Millisecond)

	// Write keys with different prefixes
	mustPut(t, leaderDB, "user:alice", "1")
	mustPut(t, leaderDB, "user:bob", "2")
	mustPut(t, leaderDB, "order:1", "3")

	time.Sleep(100 * time.Millisecond)

	// Fan-out query with prefix
	iter, err := leaderDB.FanOut(types.IteratorOptions{
		Prefix: []byte("user:"),
	})
	if err != nil {
		t.Fatalf("FanOut: %v", err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}

	want := []string{"user:alice", "user:bob"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("prefix: got %v, want %v", got, want)
	}
}

func TestFanOutQueryNotLeader(t *testing.T) {
	db := openDB(t, tmpDir(t))
	defer func() { _ = db.Close() }()

	_, err := db.FanOut(types.IteratorOptions{})
	if err == nil {
		t.Fatal("expected error when calling FanOut on non-leader")
	}
	if err.Error() != "fan-out: not a leader" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFanOutQueryReverse(t *testing.T) {
	secret := []byte("test-secret-key-32bytes-xxxxxxxxx")

	leaderDir := tmpDir(t)
	followerDir := tmpDir(t)

	addr := freePort(t)

	leaderCfg := engine.DefaultConfig(leaderDir)
	leaderCfg.Replication = &replication.Config{
		Role:       "leader",
		ListenAddr: addr,
		Secret:     secret,
	}
	leaderDB, err := engine.Open(leaderCfg)
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leaderDB.Close()

	time.Sleep(20 * time.Millisecond)

	followerCfg := engine.DefaultConfig(followerDir)
	followerCfg.Replication = &replication.Config{
		Role:              "follower",
		LeaderAddr:        addr,
		Secret:            secret,
		DialTimeout:       1 * time.Second,
		ReconnectInterval: 50 * time.Millisecond,
	}
	followerDB, err := engine.Open(followerCfg)
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer followerDB.Close()

	time.Sleep(50 * time.Millisecond)

	// Write keys
	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		mustPut(t, leaderDB, k, "val")
	}

	time.Sleep(100 * time.Millisecond)

	// Fan-out query in reverse
	iter, err := leaderDB.FanOut(types.IteratorOptions{
		Reverse: true,
	})
	if err != nil {
		t.Fatalf("FanOut: %v", err)
	}
	defer func() { _ = iter.Close() }()

	var got []string
	for iter.Valid() {
		got = append(got, string(iter.Key()))
		iter.Next()
	}

	// Should be in reverse order
	for i := 0; i < len(got)-1; i++ {
		if got[i] < got[i+1] {
			t.Fatalf("not reversed: %v", got)
		}
	}

	want := []string{"e", "d", "c", "b", "a"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reverse: got %v, want %v", got, want)
	}
}
