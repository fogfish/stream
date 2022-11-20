package stream

import (
	"context"
	"io"

	"github.com/fogfish/curie"
)

// Thing is the most generic item type used by the library to
// abstract writable/readable streams into storage services.
//
// The interfaces declares anything that have a unique identifier.
// The unique identity is exposed by pair of string: HashKey and SortKey.
type Thing interface {
	HashKey() curie.IRI
	SortKey() curie.IRI
}

//-----------------------------------------------------------------------------
//
// Storage Lazy Sequence
//
//-----------------------------------------------------------------------------

// SeqLazy is an interface to iterate through collection of objects at storage
type SeqLazy[T Thing] interface {
	// Head lifts first element of sequence
	Head() (T, io.ReadCloser, error)
	// Tail evaluates tail of sequence
	Tail() bool
	// Error returns error of stream evaluation
	Error() error
	// Cursor is the global position in the sequence
	Cursor() Thing
}

// SeqConfig configures optional sequence behavior
type SeqConfig[T Thing] interface {
	// Limit sequence size to N elements (pagination)
	Limit(int64) Seq[T]
	// Continue limited sequence from the cursor
	Continue(Thing) Seq[T]
	// Reverse order of sequence
	Reverse() Seq[T]
}

// Seq is an interface to transform collection of objects
//
//	db.Match(...).FMap(func(key Thing, val io.ReadCloser) error { ... })
type Seq[T Thing] interface {
	SeqLazy[T]
	SeqConfig[T]

	// Sequence transformer
	FMap(func(T, io.ReadCloser) error) error
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

// StreamGetter defines read by key notation
type StreamGetter[T Thing] interface {
	Has(context.Context, T) (T, error)
	Get(context.Context, T) (T, io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

// StreamPattern defines simple pattern matching lookup I/O
type StreamPattern[T Thing] interface {
	Match(context.Context, T) Seq[T]
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

// KeyValReader a generic key-value trait to read domain objects
type StreamReader[T Thing] interface {
	StreamGetter[T]
	StreamPattern[T]
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

// StreamWriter defines a generic key-value writer
type StreamWriter[T Thing] interface {
	Put(context.Context, T, io.ReadCloser) error
	Remove(context.Context, T) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

// Stream is a generic key-value trait to access domain objects.
type Streamer[T Thing] interface {
	StreamReader[T]
	StreamWriter[T]
}
