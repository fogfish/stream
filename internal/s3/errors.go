package s3

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/fogfish/stream"
)

func errServiceIO(err error) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[stream.s3.%s] service i/o failed: %w", name, err)
}

func errProcessEntity(err error, thing stream.Thing) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[stream.s3.%s] can't process (%s, %s) : %w", name, thing.HashKey(), thing.SortKey(), err)
}

// NotFound is an error to handle unknown elements
func errNotFound(err error, key string) error {
	return &notFound{err: err, key: key}
}

type notFound struct {
	err error
	key string
}

func (e *notFound) Error() string {
	return fmt.Sprintf("Not Found (%s): %s", e.key, e.err)
}

func (e *notFound) Unwrap() error { return e.err }

func (e *notFound) NotFound() string { return e.key }

//
func errEndOfStream() error {
	return errors.New("end of stream")
}

//
func recoverNoSuchKey(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NoSuchKey"
}

//
func recoverNotFound(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NotFound"
}
