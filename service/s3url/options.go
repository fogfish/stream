//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package s3url

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
)

// Option type to configure the connection
type Option func(*Options)

// Generic Config Options
type Options struct {
	prefixes curie.Prefixes
	bucket   string
	service  *s3.Client
}

// NewConfig creates Config with default options
func defaultOptions() *Options {
	return &Options{
		prefixes: curie.Namespaces{},
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(c *Options) {
		c.prefixes = prefixes
	}
}

// WithBucket defined bucket for I/O
func WithBucket(bucket string) Option {
	return func(c *Options) {
		c.bucket = bucket
	}
}

// Configure AWS Service for broker instance
func WithS3(service *s3.Client) Option {
	return func(c *Options) {
		c.service = service
	}
}
