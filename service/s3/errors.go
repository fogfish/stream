package s3

import (
	"errors"
	"fmt"
	"runtime"

	xerrors "github.com/fogfish/errors"
	"github.com/fogfish/stream"
)

const (
	errInvalidConnectorURL = xerrors.Safe1[string]("invalid connector url %s")
	errServiceIO           = xerrors.Type("service i/o failed")
)

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

func recoverNoSuchKey(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NoSuchKey"
}

func recoverNotFound(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NotFound"
}
