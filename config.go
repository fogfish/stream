//
// Copyright (C) 2019 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/dynamo
//

//
// The file declares configuration options for KeyVal storages
//

package stream

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
)

// Option type to configure the connection
type Option[T any] func(*T)

// Config options for S3-based services
type OptionS3 struct {
	Service  *s3.Client
	Prefixes curie.Prefixes
	Bucket   string
}

// NewConfig creates Config with default options
func NewOptionS3() *OptionS3 {
	return &OptionS3{
		Prefixes: curie.Namespaces{},
	}
}

// Configure AWS Service for broker instance
func WithS3[T OptionS3](service *s3.Client) Option[T] {
	return func(conf *T) {
		switch c := any(conf).(type) {
		case *OptionS3:
			c.Service = service
		}
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes[T OptionS3](prefixes curie.Prefixes) Option[T] {
	return func(conf *T) {
		switch c := any(conf).(type) {
		case *OptionS3:
			c.Prefixes = prefixes
		}
	}
}

// WithBucket defined bucket for I/O
func WithBucket[T OptionS3](bucket string) Option[T] {
	return func(conf *T) {
		switch c := any(conf).(type) {
		case *OptionS3:
			c.Bucket = bucket
		}
	}
}
