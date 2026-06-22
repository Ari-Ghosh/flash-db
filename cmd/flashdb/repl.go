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
			if len(cmdArgs) < 2 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := db.Put([]byte(cmdArgs[0]), []byte(strings.Join(cmdArgs[1:], " "))); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "get":
			if len(cmdArgs) < 1 {
				fmt.Println("usage: get <key>")
				continue
			}
			val, err := db.Get([]byte(cmdArgs[0]))
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			} else {
				fmt.Printf("%s\n", val)
			}
		case "del", "delete":
			if len(cmdArgs) < 1 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := db.Delete([]byte(cmdArgs[0])); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
			} else {
				fmt.Println("OK")
			}
		case "scan":
			prefix := []byte("")
			if len(cmdArgs) >= 1 {
				prefix = []byte(cmdArgs[0])
			}
			if len(prefix) == 0 {
				fmt.Println("scan requires a prefix (use scan <prefix>)")
				continue
			}
			iter, err := db.PrefixScan(prefix)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			count := 0
			for iter.Valid() {
				fmt.Printf("%s = %s\n", iter.Key(), iter.Value())
				iter.Next()
				count++
			}
			_ = iter.Close()
			fmt.Printf("(%d keys)\n", count)
		case "stats":
			s := db.Stats()
			fmt.Printf("MemTable: size=%d count=%d\n", s.MemTableSize, s.MemTableCount)
			fmt.Printf("L0 files: %d\n", s.L0FileCount)
			fmt.Printf("SeqNum:   %d\n", s.SeqNum)
		default:
			fmt.Printf("unknown command: %s\n", cmd)
			fmt.Println("Commands: put <key> <value>, get <key>, del <key>, scan <prefix>, stats, exit")
		}
	}
	return scanner.Err()
}
