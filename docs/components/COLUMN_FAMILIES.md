# Column Families

Column Families provide a way to group related key-value pairs into isolated namespaces within a single FlashDB instance.

## Design
- **Namespacing**: Each Column Family has a unique name.
- **Prefix-Based Isolation**: Keys within a Column Family are internally prefixed to keep them separate from other namespaces and the default keyspace.
- **Shared Storage**: All Column Families share the same underlying WAL, SSTables, and B-trees, but are logically isolated during iteration and lookups.

## Key Features
- **Logical Grouping**: Organizes data similar to "tables" in a relational database.
- **Independent Iteration**: Iterators created for a specific Column Family only see keys within that namespace.
- **Metadata Persistence**: The list of existing Column Families is persisted in a reserved system keyspace (`\x00cf_meta`).

## API
```go
// Create a new Column Family
err := db.CreateColumnFamily("users")

// Get a handle to a Column Family
cf, err := db.GetColumnFamily("users")

// Use the handle for standard operations
cf.Put([]byte("user1"), []byte("..."))
val, err := cf.Get([]byte("user1"))

// List all Column Families
names := db.ListColumnFamilies()

// Drop a Column Family (deletes all its data)
err := db.DropColumnFamily("users")
```

## Internal Representation
A key `k` in Column Family `name` is stored as:
`\x00cf\x00<name>\x00<k>`
This ensures that range scans on one Column Family never leak into another.
