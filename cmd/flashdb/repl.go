package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func runREPL(args []string) error {
	fs := flag.NewFlagSet("repl", flag.ContinueOnError)
	dir := fs.String("dir", "/tmp/flashdb", "Data directory")
	memSize := fs.Int64("memtable-size", 64*1024*1024, "MemTable size in bytes")
	l0Threshold := fs.Int("l0-threshold", 4, "L0 compaction threshold")
	_ = fs.Parse(args)

	cfg := engine.DefaultConfig(*dir)
	cfg.MemTableSize = *memSize
	cfg.L0CompactThreshold = *l0Threshold

	db, err := engine.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	fmt.Printf("flashDB REPL — data dir: %s\n", *dir)
	fmt.Println("Commands: put <key> <value>, get <key>, del <key>, scan [prefix], stats, exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]
		cmdArgs := parts[1:]

		switch cmd {
		case "exit", "quit":
			return nil
		case "put":
			replPut(db, cmdArgs)
		case "get":
			replGet(db, cmdArgs)
		case "del", "delete":
			replDel(db, cmdArgs)
		case "scan":
			replScan(db, cmdArgs)
		case "stats":
			replStats(db)
		default:
			fmt.Printf("unknown command: %s\n", cmd)
			fmt.Println("Commands: put <key> <value>, get <key>, del <key>, scan <prefix>, stats, exit")
		}
	}
	return scanner.Err()
}

func replPut(db *engine.DB, args []string) {
	if len(args) < 2 {
		fmt.Println("usage: put <key> <value>")
		return
	}
	if err := db.Put([]byte(args[0]), []byte(strings.Join(args[1:], " "))); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println("OK")
	}
}

func replGet(db *engine.DB, args []string) {
	if len(args) < 1 {
		fmt.Println("usage: get <key>")
		return
	}
	val, err := db.Get([]byte(args[0]))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Printf("%s\n", val)
	}
}

func replDel(db *engine.DB, args []string) {
	if len(args) < 1 {
		fmt.Println("usage: del <key>")
		return
	}
	if err := db.Delete([]byte(args[0])); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	} else {
		fmt.Println("OK")
	}
}

func replScan(db *engine.DB, args []string) {
	prefix := []byte("")
	if len(args) >= 1 {
		prefix = []byte(args[0])
	}
	if len(prefix) == 0 {
		fmt.Println("scan requires a prefix (use scan <prefix>)")
		return
	}
	iter, err := db.PrefixScan(prefix)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return
	}
	count := 0
	for iter.Valid() {
		fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
		iter.Next()
		count++
	}
	_ = iter.Close()
	fmt.Printf("(%d keys)\n", count)
}

func replStats(db *engine.DB) {
	s := db.Stats()
	fmt.Printf("MemTable: size=%d count=%d\n", s.MemTableSize, s.MemTableCount)
	fmt.Printf("L0 files: %d\n", s.L0FileCount)
	fmt.Printf("SeqNum:   %d\n", s.SeqNum)
}
