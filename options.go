//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stream

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/opts"
)

type Option = opts.Option[Opts]

// File System Configuration Options
type Opts struct {
	api          S3
	upload       S3Upload
	signer       S3Signer
	timeout      time.Duration
	ttlSignedUrl time.Duration
	lslimit      int32
}

func (c *Opts) checkRequired() error {
	return opts.Required(c,
		WithS3(nil),
		WithIOTimeout(0),
	)
}

var (
	// Set S3 client for the file system
	WithS3 = opts.ForType[Opts, S3]()

	// Set S3 upload client for the file system
	WithS3Upload = opts.ForType[Opts, S3Upload]()

	// Set S3 url signer client for the file system
	WithS3Signer = opts.ForType[Opts, S3Signer]()

	// Use aws.Config as base config for S3, S3 upload and S3 url signer clients
	WithConfig = opts.FMap(optsFromConfig)

	// Use region for configuring the service
	WithRegion = opts.FMap(optsFromRegion)

	// Use default aws.Config for all S3 clients
	WithDefaultS3 = opts.From(optsDefaultS3)

	// Sets the I/O timeout for the file stream.
	// This timeout defines how long the stream remains open.
	// After timeout expiration any stream I/O would fail.
	WithIOTimeout = opts.ForName[Opts, time.Duration]("timeout")

	// Sets the time-to-live for the pre-signed urls
	WithPreSignUrlTTL = opts.ForName[Opts, time.Duration]("ttlSignedUrl")

	// Set the number of keys to be read from S3 while walking through "dir"
	WithListingLimit = opts.ForName[Opts, int32]("lslimit")
)

func optsDefault() Opts {
	return Opts{
		timeout:      120 * time.Second,
		ttlSignedUrl: 5 * time.Minute,
		lslimit:      1000,
	}
}

func optsDefaultS3(c *Opts) error {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return err
	}
	return optsFromConfig(c, cfg)
}

func optsFromRegion(c *Opts, region string) error {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}

	return optsFromConfig(c, cfg)
}

func optsFromConfig(c *Opts, cfg aws.Config) error {
	api := s3.NewFromConfig(cfg)

	if c.api == nil {
		c.api = api
	}

	if c.upload == nil {
		c.upload = manager.NewUploader(api)
	}

	if c.signer == nil {
		c.signer = s3.NewPresignClient(api)
	}
	return nil
}
