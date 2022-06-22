package creek

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/s3"
)

/*

New creates a stream client to access Things at AWS storage service.
The connection URI controls the access parameters.

Supported scheme:
  s3:///my-bucket
*/
func New[T stream.Thing](
	uri string,
	defSession ...aws.Config,
) (stream.Stream[T], error) {
	awsSession, err := maybeNewSession(defSession)
	if err != nil {
		return nil, err
	}

	creator, spec, err := factory[T](uri, defSession...)
	if err != nil {
		return nil, err
	}

	return creator(awsSession, spec), nil
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
type creator[T stream.Thing] func(
	io aws.Config,
	spec *stream.URL,
) stream.Stream[T]

/*

parses connector url
*/
func factory[T stream.Thing](
	uri string,
	defSession ...aws.Config,
) (creator[T], *stream.URL, error) {
	spec, err := url.Parse(uri)
	if err != nil {
		return nil, nil, err
	}

	switch {
	case spec == nil:
		return nil, nil, fmt.Errorf("Invalid url: %s", uri)
	case len(spec.Path) < 2:
		return nil, nil, fmt.Errorf("Invalid url, path to data storage is not defined: %s", uri)
	case spec.Scheme == "s3":
		return s3.New[T], (*stream.URL)(spec), nil
	default:
		return nil, nil, fmt.Errorf("Unsupported schema: %s", uri)
	}
}

/*

creates default session with AWS API
*/
func maybeNewSession(defSession []aws.Config) (aws.Config, error) {
	if len(defSession) != 0 {
		return defSession[0], nil
	}

	awsSession, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return aws.Config{}, err
	}

	return awsSession, nil
}
