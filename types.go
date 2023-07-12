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

type GetterOpt = interface{ GetterOpt() }

// Getter defines read by key notation
type Getter[T Thing] interface {
	Has(context.Context, T, ...GetterOpt) (T, error)
	Get(context.Context, T, ...GetterOpt) (T, io.ReadCloser, error)
}

//-----------------------------------------------------------------------------
//
// Stream Pattern Matcher
//
//-----------------------------------------------------------------------------

type MatcherOpt = interface{ MatcherOpt() }

// Defines simple pattern matching I/O
type Matcher[T Thing] interface {
	Match(context.Context, T, ...MatcherOpt) ([]T, MatcherOpt, error)
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

type WriterOpt = interface{ WriterOpt() }

// Generic stream writer methods
type Writer[T Thing] interface {
	Put(context.Context, T, io.Reader, ...WriterOpt) error
	Remove(context.Context, T, ...WriterOpt) error
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
func Limit(v int32) MatcherOpt { return limit(v) }

type limit int32

func (limit) MatcherOpt() {}

func (limit limit) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor(c Thing) MatcherOpt { return cursor{c} }

type cursor struct{ Thing }

func (cursor) MatcherOpt() {}

// Duration the stream object is accessible
func AccessExpiredIn(t time.Duration) interface {
	WriterOpt
	GetterOpt
	MatcherOpt
} {
	return timeout(t)
}

type timeout time.Duration

func (timeout) WriterOpt()  {}
func (timeout) GetterOpt()  {}
func (timeout) MatcherOpt() {}

func (t timeout) Timeout() time.Duration { return time.Duration(t) }
