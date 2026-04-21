# Development Guide

This guide provides detailed instructions for developing FlashDB locally.

## Prerequisites

- Go 1.22.2 or later
- Git
- Make
- Docker (optional, for containerized development)

## Initial Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/flash-db.git
cd flash-db
```

### 2. Install Development Tools

```bash
make setup
```

This installs:
- `golangci-lint` - Go linter aggregator
- `govulncheck` - Vulnerability scanner

### 3. Setup Git Hooks (Optional but Recommended)

```bash
./scripts/setup-hooks.sh
```

This installs a pre-commit hook that runs:
- `go fmt`
- `go vet`
- `golangci-lint`
- `go test -short`

## Development Workflow

### Building

```bash
# Build the project
make build

# Run the project
make run

# Build and run (shorthand)
make build && ./bin/flashdb
```

### Testing

```bash
# Run all tests
make test

# Run tests with coverage report
make test-coverage

# Run quick tests (no short-running tests only)
make test-short

# Run specific test
go test -v -run TestMemTable_Put ./...

# Run tests with race detector
go test -race ./...
```

### Code Quality

```bash
# Format code
make fmt

# Run linters
make lint

# Run linters with fixes
make lint-fix

# Run static analysis
make vet

# Check for vulnerabilities
make vuln

# Run full quality checks
make all
```

### Dependency Management

```bash
# Tidy dependencies
make deps

# View dependency graph
make deps-graph

# Verify dependencies
go mod verify
```

## Project Structure

```
.
├── src/
│   ├── main.go              # Demo application
│   ├── engine/              # Core database engine
│   ├── memtable/            # In-memory write buffer
│   ├── sstable/             # Sorted string table
│   ├── btree/               # Read-optimized B-tree
│   ├── wal/                 # Write-ahead log
│   ├── compaction/          # LSM compaction
│   ├── replication/         # Leader-follower replication
│   ├── backup/              # Backup and restore
│   ├── txn/                 # Transaction support
│   ├── bloom/               # Bloom filter
│   ├── types/               # Common types
│   └── tests/               # Integration tests
├── scripts/                 # Development scripts
├── .github/
│   ├── workflows/           # CI/CD workflows
│   └── pull_request_template.md
├── Makefile                 # Build automation
├── .golangci.yml            # Linter configuration
├── .editorconfig            # Editor configuration
├── Dockerfile               # Docker image
└── docker-compose.yml       # Docker Compose setup
```

## Package Guide

### Engine (`src/engine/`)
Core database engine that coordinates all components. Key types:
- `DB`: Main database interface
- Methods: `Put()`, `Get()`, `Delete()`, `Begin()`, `NewSnapshot()`

### MemTable (`src/memtable/`)
In-memory write buffer using skip list. Key types:
- `MemTable`: Concurrent skip list buffer
- Methods: `Put()`, `Get()`, `Delete()`, `NewIterator()`

### SSTable (`src/sstable/`)
Immutable sorted files on disk. Key types:
- `SSTable`: Immutable sorted table
- Methods: `Get()`, `NewIterator()`, `Close()`

### B-Tree (`src/btree/`)
Read-optimized tree structure. Key types:
- `BTree`: Multi-level search tree
- Methods: `Get()`, `Seek()`, `Close()`

### WAL (`src/wal/`)
Write-ahead log for durability. Key types:
- `WAL`: Append-only write log
- Methods: `AppendPut()`, `AppendDelete()`, `Recover()`

## Common Tasks

### Adding a New Test

```go
// src/engine/engine_test.go
func TestDB_NewFeature(t *testing.T) {
    db := setupTestDB(t)
    defer db.Close()
    
    // Test logic here
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
}
```

### Running a Single Test

```bash
go test -v -run TestDB_NewFeature ./src/engine
```

### Debugging

Use print statements or use a debugger:

```bash
# With dlv debugger
dlv debug ./src
(dlv) break main.main
(dlv) continue
```

### Profiling

```bash
# CPU profile
go test -cpuprofile=cpu.prof -bench . ./...
go tool pprof cpu.prof

# Memory profile
go test -memprofile=mem.prof ./...
go tool pprof mem.prof
```

## Code Review Checklist

Before submitting a PR:

- [ ] Code compiles without errors: `make build`
- [ ] All tests pass: `make test`
- [ ] Code is formatted: `make fmt`
- [ ] Linters pass: `make lint`
- [ ] No vulnerabilities: `make vuln`
- [ ] Coverage doesn't decrease: `make test-coverage`
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] No unnecessary dependencies added

## Docker Development

### Build Docker Image

```bash
make docker-build
```

### Run with Docker Compose

```bash
make docker-run
```

### Stop and Clean

```bash
make docker-down
make docker-clean
```

## Troubleshooting

### golangci-lint not found

```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

### Tests fail with "too many open files"

```bash
ulimit -n 1024  # Increase file descriptor limit
```

### git hooks not running

```bash
chmod +x .git/hooks/pre-commit
```

## Performance Testing

### Running Benchmarks

```bash
go test -bench=. -benchtime=10s ./src/memtable
```

### Comparing Performance

```bash
go test -bench=. -benchmem ./src/engine > new.txt
# Make changes...
benchstat old.txt new.txt
```

## Resources

- [Go Documentation](https://golang.org/doc/)
- [Effective Go](https://golang.org/doc/effective_go)
- [LSM Tree Concept](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
- [MVCC Concept](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)

## Getting Help

- Check [CONTRIBUTING.md](CONTRIBUTING.md)
- Review [DOCUMENTATION.md](DOCUMENTATION.md)
- Check existing issues and discussions
- Ask in pull request discussions

Happy coding! 🚀
