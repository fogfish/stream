//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/codec"
	"github.com/fogfish/stream/internal/s3ts"
)

type Storage[T stream.Stream] struct {
	*s3ts.Store[T]

	client *s3.Client
	codec  codec.Codec[T]
	bucket string
	upload *manager.Uploader
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

	upload := manager.NewUploader(client)

	return &Storage[T]{
		Store:  store,
		client: client,
		codec:  codec.New[T](conf.prefixes),
		bucket: conf.bucket,
		upload: upload,
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
func (db *Storage[T]) Put(ctx context.Context, entity T, val io.Reader, opts ...interface{ WriterOpt(T) }) error {
	req := db.codec.Encode(entity)

	can, key := db.codec.EncodeKey(entity)
	req.Key = aws.String(key)
	req.Bucket = db.maybeBucket(can)
	req.Body = val

	_, err := db.upload.Upload(ctx, req)
	if err != nil {
		return s3ts.ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Key))
	}
	return nil
}

// Get stream and its metadata from store
func (db *Storage[T]) Get(ctx context.Context, key T, opts ...interface{ GetterOpt(T) }) (T, io.ReadCloser, error) {
	c, k := db.codec.EncodeKey(key)
	return db.get(ctx, c, k)
}

func (db *Storage[T]) get(ctx context.Context, can, key string) (T, io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: db.maybeBucket(can),
		Key:    aws.String(key),
	}
	val, err := db.client.GetObject(ctx, req)
	if err != nil {
		switch {
		case s3ts.RecoverNoSuchKey(err):
			return db.codec.Undefined, nil, s3ts.ErrNotFound(err, key)
		default:
			return db.codec.Undefined, nil, s3ts.ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Key))
		}
	}

	obj := db.codec.DecodeGetObject(val)
	return obj, val.Body, nil
}
