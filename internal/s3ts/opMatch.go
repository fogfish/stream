//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package s3ts

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
)

// Match
func (db *Store[T]) Match(ctx context.Context, key T, opts ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error) {
	req := db.reqListObjects(key, opts...)
	val, err := db.client.ListObjectsV2(context.Background(), req)
	if err != nil {
		return nil, nil, ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Prefix))
	}

	cnt := int(aws.ToInt32(val.KeyCount))
	seq := make([]T, cnt)
	for i := 0; i < cnt; i++ {
		seq[i] = db.codec.DecodeKey(aws.ToString(val.Contents[i].Key))
	}

	return seq, lastKeyToCursor[T](val), nil
}

// Cursor
type cursor struct{ hashKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }

func lastKeyToCursor[T stream.Stream](val *s3.ListObjectsV2Output) interface{ MatcherOpt(T) } {
	cnt := int(aws.ToInt32(val.KeyCount))
	if cnt == 0 || val.NextContinuationToken == nil {
		return nil
	}

	return stream.Cursor[T](&cursor{hashKey: *val.Contents[cnt-1].Key})
}

func (db *Store[T]) reqListObjects(key T, opts ...interface{ MatcherOpt(T) }) *s3.ListObjectsV2Input {
	var (
		limit  int32   = 1000
		cursor *string = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Limit() int32 }:
			limit = v.Limit()
		case stream.Stream:
			_, c := db.codec.EncodeKey(v)
			cursor = aws.String(c)
		}
	}

	c, k := db.codec.EncodeKey(key)

	return &s3.ListObjectsV2Input{
		Bucket:     db.maybeBucket(c),
		MaxKeys:    aws.Int32(limit),
		Prefix:     aws.String(k),
		StartAfter: cursor,
	}
}
