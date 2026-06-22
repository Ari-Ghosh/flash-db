// Command flashdb is the CLI entry point for flashDB.
//
// Subcommands:
//
//	serve     Start a flashDB server (with optional replication)
//	repl      Start an interactive REPL
//	put       Insert or update a key
//	get       Retrieve a value by key
//	delete    Remove a key
//	backup    Hot-backup a database directory
//	restore   Restore a backup to a directory
//	status    Print engine statistics
//	compact   Force a compaction cycle
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "serve":
		err = runServe(args)
	case "repl":
		err = runREPL(args)
	case "put":
		err = runPut(args)
	case "get":
		err = runGet(args)
	case "delete", "del":
		err = runDelete(args)
	case "backup":
		err = runBackup(args)
	case "restore":
		err = runRestore(args)
	case "status", "stats":
		err = runStatus(args)
	case "compact":
		err = runCompact(args)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `flashDB – high-performance embedded key-value database

Usage:
  flashdb <command> [options]

Commands:
  serve             Start the database server (default: standalone, add --raft for cluster mode)
  repl              Start an interactive REPL shell
  put               Insert or update a key:  flashdb put <dir> <key> <value>
  get               Retrieve a value:        flashdb get <dir> <key>
  delete            Remove a key:            flashdb delete <dir> <key>
  backup            Hot-backup a database:   flashdb backup <dir> <dest>
  restore           Restore a backup:        flashdb restore <src> <dir>
  status            Show engine statistics:  flashdb status <dir>
  compact           Force compaction:        flashdb compact <dir>

Run 'flashdb help <command>' for more details on a specific command.
`)
}
