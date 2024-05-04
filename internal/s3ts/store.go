//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package s3ts

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/codec"
)

type Store[T stream.Stream] struct {
	client *s3.Client
	codec  codec.Codec[T]
	bucket string
	waiter *s3.ObjectExistsWaiter
}

// New client instance
func New[T stream.Stream](
	api *s3.Client,
	bucket string,
	prefixes curie.Prefixes,
) (*Store[T], error) {
	return &Store[T]{
		bucket: bucket,
		client: api,
		codec:  codec.New[T](prefixes),
		waiter: s3.NewObjectExistsWaiter(api),
	}, nil
}

func (db *Store[T]) maybeBucket(can string) *string {
	if len(can) != 0 {
		return aws.String(can)
	} else {
		return aws.String(db.bucket)
	}
}
