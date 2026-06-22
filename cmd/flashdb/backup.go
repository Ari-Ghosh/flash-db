package main

import (
	"flag"
	"fmt"

	"github.com/Ari-Ghosh/flash-db/src/backup"
	"github.com/Ari-Ghosh/flash-db/src/engine"
)

func runBackup(args []string) error {
	fs := flag.NewFlagSet("backup", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: flashdb backup <dir> <dest>")
	}
	dir, dest := fs.Arg(0), fs.Arg(1)

	db, err := engine.Open(engine.DefaultConfig(dir))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	manifest, err := db.Backup(dest)
	if err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	fmt.Printf("Backup complete: %d files, snapSeq=%d\n", len(manifest.Files), manifest.SnapSeq)
	return nil
}

func runRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		return fmt.Errorf("usage: flashdb restore <src> <dir>")
	}
	src, dir := fs.Arg(0), fs.Arg(1)

	if err := backup.Restore(src, dir); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	fmt.Printf("Restore complete: %s -> %s\n", src, dir)
	return nil
}
