package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func runPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 3 {
		return fmt.Errorf("usage: flashdb put <dir> <key> <value>")
	}
	dir, key, value := fs.Arg(0), fs.Arg(1), fs.Arg(2)
	db, err := engine.Open(engine.DefaultConfig(dir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: flashdb get <dir> <key>")
	}
	dir, key := fs.Arg(0), fs.Arg(1)
	db, err := engine.Open(engine.DefaultConfig(dir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	val, err := db.Get([]byte(key))
	if err != nil {
		return err
	}
	fmt.Println(string(val))
	return nil
}

func runDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: flashdb delete <dir> <key>")
	}
	dir, key := fs.Arg(0), fs.Arg(1)
	db, err := engine.Open(engine.DefaultConfig(dir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Delete([]byte(key)); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func runStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: flashdb status <dir>")
	}
	dir := fs.Arg(0)
	db, err := engine.Open(engine.DefaultConfig(dir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	s := db.Stats()
	fmt.Printf("Directory:     %s\n", dir)
	fmt.Printf("MemTable size: %d bytes\n", s.MemTableSize)
	fmt.Printf("MemTable keys: %d\n", s.MemTableCount)
	fmt.Printf("L0 files:      %d\n", s.L0FileCount)
	fmt.Printf("SeqNum:        %d\n", s.SeqNum)
	fmt.Printf("Puts:          %d\n", db.PutCount())
	fmt.Printf("Gets:          %d\n", db.GetCount())
	fmt.Printf("Deletes:       %d\n", db.DeleteCount())

	b := db.BloomStats()
	fmt.Printf("Bloom queries: %d\n", b.TotalQueries)
	fmt.Printf("Bloom FP:      %d (%.4f%%)\n", b.TotalFalsePositives, b.ObservedFPR*100)

	return nil
}

func runCompact(args []string) error {
	fs := flag.NewFlagSet("compact", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: flashdb compact <dir>")
	}
	dir := fs.Arg(0)

	// Force-close the DB and reopen with aggressive compaction settings.
	_ = os.RemoveAll(dir + "_compact_tmp")
	cfg := engine.DefaultConfig(dir)
	cfg.MemTableSize = 64 * 1024 * 1024
	cfg.L0CompactThreshold = 2

	db, err := engine.Open(cfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Flush and trigger compaction.
	if err := db.FlushSync(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	fmt.Println("Compaction triggered. Run 'status' to verify.")
	return nil
}
