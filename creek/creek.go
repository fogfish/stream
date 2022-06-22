package creek

import (
	"fmt"

	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/s3"
)

/*

New creates a stream client to access Things at AWS storage service.
The connection URI controls the access parameters.

Supported scheme:
  s3:///my-bucket
*/
func New[T stream.Thing](opts ...stream.Option) (stream.Stream[T], error) {
	cfg, err := stream.NewConfig(opts...)
	if err != nil {
		return nil, err
	}

	creator, err := factory[T](cfg)
	if err != nil {
		return nil, err
	}

	return creator(cfg), nil
}

/*

Must is a helper function to ensure KeyVal interface is valid and there was no
error when calling a New function.

This helper is intended to be used in variable initialization to load the
interface and configuration at startup. Such as:

  var io = dynamo.Must(dynamo.New())
*/
func Must[T stream.Thing](kv stream.Stream[T], err error) stream.Stream[T] {
	if err != nil {
		panic(err)
	}
	return kv
}

/*

creator is a factory function
*/
type creator[T stream.Thing] func(*stream.Config) stream.Stream[T]

/*

parses connector url
*/
func factory[T stream.Thing](cfg *stream.Config) (creator[T], error) {
	switch {
	case cfg.URI == nil:
		return nil, fmt.Errorf("undefined storage endpoint")
	case len(cfg.URI.Path) < 2:
		return nil, fmt.Errorf("invalid storage endpoint, missing storage name: %s", cfg.URI.String())
	case cfg.URI.Scheme == "s3":
		return s3.New[T], nil
	default:
		return nil, fmt.Errorf("unsupported storage schema: %s", cfg.URI.String())
	}
}
