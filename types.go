package stream

import (
	"context"
	"io"
	"time"

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
// Stream Reader
//
//-----------------------------------------------------------------------------

// Getter defines read by key notation
type Getter[T Thing] interface {
	Has(context.Context, T, ...interface{ GetterOpt(T) }) (T, error)
	Get(context.Context, T, ...interface{ GetterOpt(T) }) (T, io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

// Defines simple pattern matching I/O
type Matcher[T Thing] interface {
	Match(context.Context, T, ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error)
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

// KeyValReader a generic key-value trait to read domain objects
type Reader[T Thing] interface {
	Getter[T]
	Matcher[T]
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

// Generic stream writer methods
type Writer[T Thing] interface {
	Put(context.Context, T, io.Reader, ...interface{ WriterOpt(T) }) error
	Remove(context.Context, T, ...interface{ WriterOpt(T) }) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

// Stream is a generic key-value trait to access domain objects.
type Streamer[T Thing] interface {
	Reader[T]
	Writer[T]
}

//-----------------------------------------------------------------------------
//
// Options
//
//-----------------------------------------------------------------------------

// Limit option for Match
func Limit[T Thing](v int32) interface{ MatcherOpt(T) } { return limit[T](v) }

type limit[T Thing] int32

func (limit[T]) MatcherOpt(T) {}

func (limit limit[T]) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor[T Thing](c Thing) interface{ MatcherOpt(T) } { return cursor[T]{c} }

type cursor[T Thing] struct{ Thing }

func (cursor[T]) MatcherOpt(T) {}

// Duration the stream object is accessible
func AccessExpiredIn[T Thing](t time.Duration) interface {
	WriterOpt(T)
	GetterOpt(T)
	MatcherOpt(T)
} {
	return timeout[T](t)
}

type timeout[T Thing] time.Duration

func (timeout[T]) WriterOpt(T)  {}
func (timeout[T]) GetterOpt(T)  {}
func (timeout[T]) MatcherOpt(T) {}

func (t timeout[T]) Timeout() time.Duration { return time.Duration(t) }
