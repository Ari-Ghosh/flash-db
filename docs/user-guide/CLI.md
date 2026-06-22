# CLI / REPL Reference

FlashDB ships with a full command-line interface (`flashdb`) that supports starting a server, an interactive REPL, and one-shot key-value operations.

## Installation

```bash
go install github.com/Ari-Ghosh/flash-db/cmd/flashdb@latest
```

Or build from source:

```bash
cd flashdb
make build          # produces bin/flashdb
go build -o flashdb ./cmd/flashdb
```

## Usage

```
flashdb <command> [options]

Commands:
  serve             Start the database server
  repl              Start an interactive REPL shell
  put               Insert or update a key:  flashdb put <dir> <key> <value>
  get               Retrieve a value:        flashdb get <dir> <key>
  delete            Remove a key:            flashdb delete <dir> <key>
  backup            Hot-backup a database:   flashdb backup <dir> <dest>
  restore           Restore a backup:        flashdb restore <src> <dir>
  status            Show engine statistics:  flashdb status <dir>
  compact           Force compaction:        flashdb compact <dir>
```

## serve — Server Mode

Starts a flashDB server with optional Prometheus metrics, OpenTelemetry tracing, and Raft consensus.

```
flashdb serve \
  --dir /tmp/flashdb \
  --metrics-addr :9090 \
  --memtable-size 67108864 \
  --l0-threshold 4
```

| Flag | Default | Description |
|---|---|---|
| `--dir` | `/tmp/flashdb` | Data directory |
| `--metrics-addr` | `""` | Prometheus metrics HTTP endpoint (e.g. `:9090`) |
| `--memtable-size` | `67108864` (64 MB) | MemTable flush threshold in bytes |
| `--l0-threshold` | `4` | L0 file count to trigger compaction |
| `--otel-endpoint` | `""` | OpenTelemetry OTLP HTTP endpoint (e.g. `http://localhost:4318`) |
| `--raft-addr` | `""` | Raft cluster bind address (e.g. `:6000`) |
| `--node-id` | `""` | Unique node ID for Raft cluster (required with `--raft-addr`) |
| `--raft-join` | `""` | Existing Raft node address to join |

### Raft Cluster Examples

**First node (bootstraps the cluster):**

```bash
flashdb serve --dir /tmp/node1 --raft-addr :6000 --node-id node1 --metrics-addr :9091
```

**Additional nodes (join existing cluster):**

```bash
flashdb serve --dir /tmp/node2 --raft-addr :6001 --node-id node2 \
  --raft-join 127.0.0.1:6000 --metrics-addr :9092
```

> **Note:** When joining a Raft cluster, the new node starts as a non-voter. The cluster leader must add it as a voter via the programmatic API (`db.AddRaftVoter`). In the future, a `raft join` CLI command will automate this.

## repl — Interactive REPL

Start an interactive shell for ad-hoc operations:

```bash
flashdb repl --dir /tmp/flashdb
```

### REPL Commands

| Command | Syntax | Description |
|---|---|---|
| `put` | `put <key> <value>` | Insert or update a key |
| `get` | `get <key>` | Retrieve a value by key |
| `del` | `del <key>` | Delete a key |
| `scan` | `scan <prefix>` | Scan all keys sharing a prefix |
| `stats` | `stats` | Show engine statistics |
| `exit` | `exit` | Exit the REPL |

### Example Session

```
$ flashdb repl --dir /tmp/demo
flashDB REPL — data dir: /tmp/demo
Commands: put <key> <value>, get <key>, del <key>, scan [prefix], stats, exit

> put user:alice alice@example.com
OK
> put user:bob bob@example.com
OK
> get user:alice
alice@example.com
> scan user:
user:alice = alice@example.com
user:bob = bob@example.com
(2 keys)
> stats
MemTable: size=128 count=2
L0 files: 0
SeqNum:   2
> del user:alice
OK
> get user:alice
error: key not found
> exit
```

## One-Shot Commands

### put

```bash
flashdb put /tmp/flashdb mykey myvalue
```

Inserts `mykey` with value `myvalue`. Opens the database, writes, and closes. Outputs `OK` on success.

### get

```bash
flashdb get /tmp/flashdb mykey
```

Retrieves `mykey`. Prints the value to stdout, or an error if not found.

### delete

```bash
flashdb delete /tmp/flashdb mykey
```

Deletes `mykey`. Outputs `OK` on success.

### status

```bash
flashdb status /tmp/flashdb
```

Prints engine statistics:

```
Directory:     /tmp/flashdb
MemTable size: 128 bytes
MemTable keys: 2
L0 files:      0
SeqNum:        2
Puts:          2
Gets:          1
Deletes:       0
Bloom queries: 0
Bloom FP:      0 (0.0000%)
```

### compact

```bash
flashdb compact /tmp/flashdb
```

Forces a flush of the active MemTable and triggers compaction. Useful for maintenance.

### backup

```bash
flashdb backup /tmp/flashdb /tmp/backup
```

Performs a hot backup of the database to the destination directory. Outputs the file count and snapshot sequence number.

### restore

```bash
flashdb restore /tmp/backup /tmp/restored
```

Restores a backup to a new directory. The destination must be empty.

## Programmatic Use

The CLI commands are thin wrappers around the engine API. For programmatic use, import the `engine` package directly:

```go
import "github.com/Ari-Ghosh/flash-db/src/engine"

db, _ := engine.Open(engine.DefaultConfig("/tmp/demo"))
defer db.Close()
db.Put([]byte("key"), []byte("value"))
```
