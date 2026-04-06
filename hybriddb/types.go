package hybriddb

import "errors"

// Sentinel errors
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrKeyDeleted  = errors.New("key deleted")
)

// Entry is a versioned key-value record.
// SeqNum increases monotonically with every write.
// Tombstone == true means the key has been deleted.
type Entry struct {
	Key       []byte
	Value     []byte
	SeqNum    uint64
	Tombstone bool
}

// Reader is anything that can look up a key.
type Reader interface {
	Get(key []byte) (*Entry, error)
}

// Writer is anything that can accept a put or delete.
type Writer interface {
	Put(key, value []byte, seq uint64) error
	Delete(key []byte, seq uint64) error
}
