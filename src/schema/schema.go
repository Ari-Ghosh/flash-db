// Package schema provides a type registry and value codec for flashDB.
//
// Schemas describe the structure of values stored in the database,
// enabling type-safe encoding/decoding and schema version evolution.

package schema

import (
	"fmt"
)

// FieldType enumerates supported value field types.
type FieldType uint8

const (
	FieldInt64   FieldType = 1
	FieldFloat64 FieldType = 2
	FieldString  FieldType = 3
	FieldBytes   FieldType = 4
	FieldBool    FieldType = 5
)

// FieldDef describes one field in a schema.
type FieldDef struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Optional bool      `json:"optional,omitempty"`
}

// Schema defines the structure of values under a registered name.
type Schema struct {
	Name    string     `json:"name"`
	Version uint32     `json:"version"`
	Fields  []FieldDef `json:"fields"`
}

// Codec encodes and decodes values for a specific schema version.
type Codec interface {
	Encode(val any) ([]byte, error)
	Decode(data []byte, target any) error
	Schema() Schema
}

// Registry manages the lifecycle of schemas and their codecs.
type Registry interface {
	Register(schema Schema, codec Codec) error
	Resolve(name string, version uint32) (Codec, error)
	Latest(name string) (Codec, error)
	List() []Schema
}

// Errors returned by the schema package.
var (
	ErrSchemaNotFound        = fmt.Errorf("schema: not found")
	ErrSchemaVersionNotFound = fmt.Errorf("schema: version not found")
	ErrSchemaExists          = fmt.Errorf("schema: already registered")
	ErrInvalidType           = fmt.Errorf("schema: invalid field type")
)
