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
)

// Visit
func (db *Store[T]) Visit(ctx context.Context, key T, f func(T) error) error {
	var reKey interface{ MatchKey(string) bool } = nil

	req := db.reqListObjects(key)

	for {
		val, err := db.client.ListObjectsV2(context.Background(), req)
		if err != nil {
			return ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Prefix))
		}

		for _, el := range val.Contents {
			k := aws.ToString(el.Key)
			if reKey == nil || reKey.MatchKey(k) {
				if err := f(db.codec.DecodeKey(k)); err != nil {
					return err
				}
			}
		}

		cnt := int(aws.ToInt32(val.KeyCount))
		if cnt == 0 || val.NextContinuationToken == nil {
			return nil
		}

		req.StartAfter = val.Contents[cnt-1].Key
	}
}
