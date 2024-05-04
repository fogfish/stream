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
)

func (db *Store[T]) Copy(ctx context.Context, source T, target T) error {
	tcan, tkey := db.codec.EncodeKey(target)
	scan, skey := db.codec.EncodeKey(source)
	bckt := aws.ToString(db.maybeBucket(scan))

	req := &s3.CopyObjectInput{
		Bucket:     db.maybeBucket(tcan),
		Key:        aws.String(tkey),
		CopySource: aws.String(bckt + "/" + skey),
	}

	_, err := db.client.CopyObject(ctx, req)
	if err != nil {
		return ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Key))
	}

	return nil
}
