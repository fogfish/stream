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
	"github.com/fogfish/curie"
)

/*
Config options for the connection
*/
type Config struct {
	Service  any
	Prefixes curie.Prefixes
}

func (*Config) Config() {}

// NewConfig creates Config with default options
func NewConfig() Config {
	return Config{
		Prefixes: curie.Namespaces{},
	}
}

// Option type to configure the connection
type Option func(cfg interface{ Config() })

// Configure AWS Service for broker instance
func WithService(service any) Option {
	return func(conf interface{ Config() }) {
		switch c := conf.(type) {
		case *Config:
			c.Service = service
		}
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(conf interface{ Config() }) {
		switch c := conf.(type) {
		case *Config:
			c.Prefixes = prefixes
		}
	}
}
