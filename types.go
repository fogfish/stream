package stream

import (
	"context"
	"fmt"
	"io"
	"time"
)

/*

Thing is the most generic item type used by the library to
abstract writable/readable streams into storage services.

The interfaces declares anything that have a unique identifier.
The unique identity is exposed by pair of string: HashKey and SortKey.
*/
type Thing interface {
	HashKey() string
	SortKey() string
}

//-----------------------------------------------------------------------------
//
// Storage Lazy Sequence
//
//-----------------------------------------------------------------------------

/*

SeqLazy is an interface to iterate through collection of objects at storage
*/
type SeqLazy interface {
	// Head lifts first element of sequence
	Head() (Thing, error)
	// Body lifts first element of sequence
	Body() (io.ReadCloser, error)
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
type SeqConfig interface {
	// Limit sequence size to N elements (pagination)
	Limit(int64) Seq
	// Continue limited sequence from the cursor
	Continue(Thing) Seq
	// Reverse order of sequence
	Reverse() Seq
}

/*

Seq is an interface to transform collection of objects

  db.Match(...).FMap(func(key Thing, val io.ReadCloser) error { ... })
*/
type Seq interface {
	SeqLazy
	SeqConfig

	// Sequence transformer
	FMap(func(Thing, io.ReadCloser) error) error
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

/*

StreamGetter defines read by key notation
*/
type StreamGetter interface {
	Has(context.Context, Thing) (bool, error)
	URL(context.Context, Thing, time.Duration) (string, error)
	Get(context.Context, Thing) (io.ReadCloser, error)
}

/*

StreamGetterNoContext defines read by key notation
*/
type StreamGetterNoContext interface {
	Has(Thing) (bool, error)
	URL(Thing, time.Duration) (string, error)
	Get(Thing) (io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

/*

StreamPattern defines simple pattern matching lookup I/O
*/
type StreamPattern interface {
	Match(context.Context, Thing) Seq
}

/*

StreamPatternNoContext defines simple pattern matching lookup I/O
*/
type StreamPatternNoContext interface {
	Match(Thing) Seq
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

/*

KeyValReader a generic key-value trait to read domain objects
*/
type StreamReader interface {
	StreamGetter
	StreamPattern
}

/*

StreamReaderNoContext a generic key-value trait to read domain objects
*/
type StreamReaderNoContext interface {
	StreamGetterNoContext
	StreamPatternNoContext
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

/*

StreamWriter defines a generic key-value writer
*/
type StreamWriter interface {
	Put(context.Context, Thing, io.ReadCloser) error
	Remove(context.Context, Thing) error
}

/*

StreamWriterNoContext defines a generic key-value writer
*/
type StreamWriterNoContext interface {
	Put(Thing, io.ReadCloser) error
	Remove(Thing) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

/*

Stream is a generic key-value trait to access domain objects.
*/
type Stream interface {
	StreamReader
	StreamWriter
}

/*

StreamNoContext is a generic key-value trait to access domain objects.
*/
type StreamNoContext interface {
	StreamReaderNoContext
	StreamWriterNoContext
}

//-----------------------------------------------------------------------------
//
// Errors
//
//-----------------------------------------------------------------------------

/*

NotFound is an error to handle unknown elements
*/
type NotFound struct{ Thing }

func (e NotFound) Error() string {
	return fmt.Sprintf("Not Found (%s, %s) ", e.Thing.HashKey(), e.Thing.SortKey())
}

/*

EOS error indicates End Of Stream
*/
type EOS struct{}

func (e EOS) Error() string {
	return "End of Stream"
}
