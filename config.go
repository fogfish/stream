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
	"net/url"
	"strings"

	"github.com/fogfish/curie"
)

/*
Config options for the connection
*/
type Config struct {
	Service  any
	Prefixes curie.Prefixes
}

// NewConfig creates Config with default options
func NewConfig() Config {
	return Config{
		Prefixes: curie.Namespaces{},
	}
}

// Option type to configure the connection
type Option func(cfg *Config)

// Configure AWS Service for broker instance
func WithService(service any) Option {
	return func(conf *Config) {
		conf.Service = service
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(conf *Config) {
		conf.Prefixes = prefixes
	}
}

/*
URL custom type with helper functions
*/
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
