package s3

import (
	"errors"
	"fmt"

	"github.com/fogfish/faults"
)

const (
	errUndefinedBucket = faults.Type("undefined S3 bucket")
	errServiceIO       = faults.Safe2[string, string]("service i/o failed (bucket: %s, key: %s)")
)

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
