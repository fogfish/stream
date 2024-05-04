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
type Stream interface{ HashKey() curie.IRI }

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

// Getter defines read by key notation
type Getter[T Stream] interface {
	Has(context.Context, T, ...interface{ GetterOpt(T) }) (T, error)
	Get(context.Context, T, ...interface{ GetterOpt(T) }) (T, io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

// Defines simple pattern matching I/O
type Matcher[T Stream] interface {
	Match(context.Context, T, ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error)
}

// Defines simple pattern matching visitor
type Visitor[T Stream] interface {
	Visit(context.Context, T, func(T) error) error
}

//-----------------------------------------------------------------------------
//
// Stream Reader
//
//-----------------------------------------------------------------------------

// KeyValReader a generic key-value trait to read domain objects
type Reader[T Stream] interface {
	Getter[T]
	Matcher[T]
	Visitor[T]
}

//-----------------------------------------------------------------------------
//
// Stream Writer
//
//-----------------------------------------------------------------------------

// Generic stream writer methods
type Writer[T Stream] interface {
	Put(context.Context, T, io.Reader, ...interface{ WriterOpt(T) }) error
	Remove(context.Context, T, ...interface{ WriterOpt(T) }) error
}

//-----------------------------------------------------------------------------
//
// Storage interface
//
//-----------------------------------------------------------------------------

// Stream is a generic key-value trait to access domain objects.
type Streamer[T Stream] interface {
	Reader[T]
	Writer[T]
}

//-----------------------------------------------------------------------------
//
// Options
//
//-----------------------------------------------------------------------------

// Limit option for Match
func Limit[T Stream](v int32) interface{ MatcherOpt(T) } { return limit[T](v) }

type limit[T Stream] int32

func (limit[T]) MatcherOpt(T) {}

func (limit limit[T]) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor[T Stream](c Stream) interface{ MatcherOpt(T) } { return cursor[T]{c} }

type cursor[T Stream] struct{ Stream }

func (cursor[T]) MatcherOpt(T) {}

// Duration the stream object is accessible
func AccessExpiredIn[T Stream](t time.Duration) interface {
	WriterOpt(T)
	GetterOpt(T)
} {
	return timeout[T](t)
}

type timeout[T Stream] time.Duration

func (timeout[T]) WriterOpt(T)  {}
func (timeout[T]) GetterOpt(T)  {}
func (timeout[T]) MatcherOpt(T) {}

func (t timeout[T]) Timeout() time.Duration { return time.Duration(t) }
