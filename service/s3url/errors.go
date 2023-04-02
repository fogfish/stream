package s3url

import (
	"errors"
	"fmt"

	"github.com/fogfish/faults"
)

const (
	errInvalidConnectorURL = faults.Safe1[string]("invalid connector url %s")
	errServiceIO           = faults.Type("service i/o failed")
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

// recover from AWS SDK Not Found error
func recoverNotFound(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NotFound"
}
