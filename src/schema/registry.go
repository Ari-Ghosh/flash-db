package schema

import (
	"encoding/json"
	"sync"
)

// MapRegistry is an in-memory schema Registry backed by a map.
type MapRegistry struct {
	mu       sync.RWMutex
	schemas  map[string]map[uint32]Codec // name → version → codec
	latest   map[string]uint32           // name → latest version
}

// NewMapRegistry creates an empty in-memory schema registry.
func NewMapRegistry() *MapRegistry {
	return &MapRegistry{
		schemas: make(map[string]map[uint32]Codec),
		latest:  make(map[string]uint32),
	}
}

func (r *MapRegistry) Register(s Schema, c Codec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.schemas[s.Name]; !ok {
		r.schemas[s.Name] = make(map[uint32]Codec)
	}
	if _, exists := r.schemas[s.Name][s.Version]; exists {
		return ErrSchemaExists
	}
	r.schemas[s.Name][s.Version] = c
	if s.Version > r.latest[s.Name] {
		r.latest[s.Name] = s.Version
	}
	return nil
}

func (r *MapRegistry) Resolve(name string, version uint32) (Codec, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	versions, ok := r.schemas[name]
	if !ok {
		return nil, ErrSchemaNotFound
	}
	c, ok := versions[version]
	if !ok {
		return nil, ErrSchemaVersionNotFound
	}
	return c, nil
}

func (r *MapRegistry) Latest(name string) (Codec, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.latest[name]
	if !ok {
		return nil, ErrSchemaNotFound
	}
	return r.schemas[name][v], nil
}

func (r *MapRegistry) List() []Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Schema
	for _, versions := range r.schemas {
		for _, c := range versions {
			out = append(out, c.Schema())
		}
	}
	return out
}

// JSONCodec encodes/decodes values using encoding/json.
type JSONCodec struct {
	schema Schema
}

// NewJSONCodec returns a JSON codec for the given schema.
func NewJSONCodec(s Schema) *JSONCodec {
	return &JSONCodec{schema: s}
}

func (c *JSONCodec) Encode(val any) ([]byte, error) {
	return json.Marshal(val)
}

func (c *JSONCodec) Decode(data []byte, target any) error {
	return json.Unmarshal(data, target)
}

func (c *JSONCodec) Schema() Schema { return c.schema }
