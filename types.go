package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/fogfish/curie"
)

/*

Thing is the most generic item type used by the library to
abstract writable/readable streams into storage services.

The interfaces declares anything that have a unique identifier.
The unique identity is exposed by pair of string: HashKey and SortKey.
*/
type Thing interface {
	HashKey() curie.IRI
	SortKey() curie.IRI
}

//-----------------------------------------------------------------------------
//
// Storage Lazy Sequence
//
//-----------------------------------------------------------------------------

/*

SeqLazy is an interface to iterate through collection of objects at storage
*/
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

/*

SeqConfig configures optional sequence behavior
*/
type SeqConfig[T Thing] interface {
	// Limit sequence size to N elements (pagination)
	Limit(int64) Seq[T]
	// Continue limited sequence from the cursor
	Continue(Thing) Seq[T]
	// Reverse order of sequence
	Reverse() Seq[T]
}

/*

Seq is an interface to transform collection of objects

  db.Match(...).FMap(func(key Thing, val io.ReadCloser) error { ... })
*/
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

/*

StreamGetter defines read by key notation
*/
type StreamGetter[T Thing] interface {
	Has(context.Context, T) (bool, error)
	URL(context.Context, T, time.Duration) (string, error)
	Get(context.Context, T) (T, io.ReadCloser, error)
}

/*

StreamGetterNoContext defines read by key notation
*/
type StreamGetterNoContext[T Thing] interface {
	Has(T) (bool, error)
	URL(T, time.Duration) (string, error)
	Get(T) (T, io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

/*

StreamPattern defines simple pattern matching lookup I/O
*/
type StreamPattern[T Thing] interface {
	Match(context.Context, T) Seq[T]
}

/*

StreamPatternNoContext defines simple pattern matching lookup I/O
*/
type StreamPatternNoContext[T Thing] interface {
	Match(T) Seq[T]
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

/*

KeyValReader a generic key-value trait to read domain objects
*/
type StreamReader[T Thing] interface {
	StreamGetter[T]
	StreamPattern[T]
}

/*

StreamReaderNoContext a generic key-value trait to read domain objects
*/
type StreamReaderNoContext[T Thing] interface {
	StreamGetterNoContext[T]
	StreamPatternNoContext[T]
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

/*

StreamWriter defines a generic key-value writer
*/
type StreamWriter[T Thing] interface {
	Put(context.Context, T, io.ReadCloser) error
	Remove(context.Context, T) error
}

/*

StreamWriterNoContext defines a generic key-value writer
*/
type StreamWriterNoContext[T Thing] interface {
	Put(T, io.ReadCloser) error
	Remove(T) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

/*

Stream is a generic key-value trait to access domain objects.
*/
type Stream[T Thing] interface {
	StreamReader[T]
	StreamWriter[T]
}

/*

StreamNoContext is a generic key-value trait to access domain objects.
*/
type StreamNoContext[T Thing] interface {
	StreamReaderNoContext[T]
	StreamWriterNoContext[T]
}

//-----------------------------------------------------------------------------
//
// Errors
//
//-----------------------------------------------------------------------------

/*

NotFound is an error to handle unknown elements
*/
type NotFound struct {
	Key string
	err error
}

func ErrNotFound(err error, key string) error {
	return &NotFound{Key: key, err: err}
}

func (e *NotFound) Error() string {
	return fmt.Sprintf("Not Found (%s): %s", e.Key, e.err)
}

func (e *NotFound) Unwrap() error { return e.err }

func (e *NotFound) NotFound() bool { return true }

/*

EOS error indicates End Of Stream
*/
func ErrEndOfStream() error {
	return errors.New("end of stream")
}
