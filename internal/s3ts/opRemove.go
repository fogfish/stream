package s3ts

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Remove stream from store
func (db *Store[T]) Remove(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) error {
	can, key := db.codec.EncodeKey(entity)
	req := &s3.DeleteObjectInput{
		Bucket: db.maybeBucket(can),
		Key:    aws.String(key),
	}

	_, err := db.client.DeleteObject(ctx, req)
	if err != nil {
		return ErrServiceIO.New(err, db.bucket, key)
	}

	return nil
}
