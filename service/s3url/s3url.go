//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package s3url

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/codec"
	"github.com/fogfish/stream/internal/s3ts"
)

type Storage[T stream.Stream] struct {
	*s3ts.Store[T]

	bucket string
	client *s3.Client
	signer *s3.PresignClient
	codec  codec.Codec[T]
}

// New client instance
func New[T stream.Stream](opts ...Option) (*Storage[T], error) {
	conf := defaultOptions()
	for _, opt := range opts {
		opt(conf)
	}

	client, err := newClient(conf)
	if err != nil {
		return nil, err
	}

	store, err := s3ts.New[T](client, conf.bucket, conf.prefixes)
	if err != nil {
		return nil, err
	}

	signer := s3.NewPresignClient(client)

	return &Storage[T]{
		Store:  store,
		bucket: conf.bucket,
		client: client,
		signer: signer,
		codec:  codec.New[T](conf.prefixes),
	}, nil
}

func newClient(conf *Options) (*s3.Client, error) {
	if conf.service != nil {
		return conf.service, nil
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(aws), nil
}

func (db *Storage[T]) maybeBucket(can string) *string {
	if len(can) != 0 {
		return aws.String(can)
	} else {
		return aws.String(db.bucket)
	}
}

// Put stream into store
func (db *Storage[T]) Put(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) (string, error) {
	expiresIn := time.Duration(20 * time.Minute)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Timeout() time.Duration }:
			expiresIn = v.Timeout()
		}
	}

	req := db.codec.Encode(entity)
	can, key := db.codec.EncodeKey(entity)
	req.Key = aws.String(key)
	req.Bucket = db.maybeBucket(can)

	val, err := db.signer.PresignPutObject(ctx, req, s3.WithPresignExpires(expiresIn))
	if err != nil {
		return "", err
	}

	return val.URL, nil
}

// Get stream from store
func (db *Storage[T]) Get(ctx context.Context, entity T, opts ...interface{ GetterOpt(T) }) (string, error) {
	expiresIn := time.Duration(20 * time.Minute)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Timeout() time.Duration }:
			expiresIn = v.Timeout()
		}
	}

	can, key := db.codec.EncodeKey(entity)
	req := &s3.GetObjectInput{
		Bucket: db.maybeBucket(can),
		Key:    aws.String(key),
	}

	val, err := db.signer.PresignGetObject(ctx, req, s3.WithPresignExpires(expiresIn))
	if err != nil {
		return "", s3ts.ErrServiceIO.New(err, db.bucket, key)
	}

	return val.URL, nil
}
