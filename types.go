package stream

import (
	"context"
	"io"
	"net/url"
	"strings"

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
	Match(context.Context, T, ...interface{ MatchOpt() }) ([]T, error)
}

// Limit option for Match
func Limit(v int32) interface{ MatchOpt() } { return limit(v) }

type limit int32

func (limit) MatchOpt() {}

func (limit limit) Limit() int32 { return int32(limit) }

// Cursor option for Match
func Cursor(c Thing) interface{ MatchOpt() } { return cursor{c} }

type cursor struct{ Thing }

func (cursor) MatchOpt() {}

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
	Put(context.Context, T, io.Reader) error
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

//-----------------------------------------------------------------------------
//
// Utility types
//
//-----------------------------------------------------------------------------

// URL custom type with helper functions
type URL url.URL

func (uri *URL) String() string {
	return (*url.URL)(uri).String()
}

// query parameters
func (uri *URL) Query(key, def string) string {
	val := (*url.URL)(uri).Query().Get(key)

	if val == "" {
		return def
	}

	return val
}

// path segments of length
func (uri *URL) Segments() []string {
	return strings.Split((*url.URL)(uri).Path, "/")[1:]
}
