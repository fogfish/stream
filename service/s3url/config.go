package s3url

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
)

// Option type to configure the connection
type Option func(*Config)

// Generic Config Options
type Config struct {
	prefixes curie.Prefixes
	bucket   string
	service  *s3.Client
}

// NewConfig creates Config with default options
func defaultConfig() *Config {
	return &Config{
		prefixes: curie.Namespaces{},
	}
}

// WithPrefixes defines prefixes for CURIEs
func WithPrefixes(prefixes curie.Prefixes) Option {
	return func(c *Config) {
		c.prefixes = prefixes
	}
}

// WithBucket defined bucket for I/O
func WithBucket(bucket string) Option {
	return func(c *Config) {
		c.bucket = bucket
	}
}

// Configure AWS Service for broker instance
func WithS3(service *s3.Client) Option {
	return func(c *Config) {
		c.service = service
	}
}
