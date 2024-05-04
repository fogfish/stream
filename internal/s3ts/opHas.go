package s3ts

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Check existence of stream in the store, returning its metadata
func (db *Store[T]) Has(ctx context.Context, key T, opts ...interface{ GetterOpt(T) }) (T, error) {
	c, k := db.codec.EncodeKey(key)
	return db.has(ctx, c, k)
}

func (db *Store[T]) has(ctx context.Context, can, key string) (T, error) {
	req := &s3.HeadObjectInput{
		Bucket: db.maybeBucket(can),
		Key:    aws.String(key),
	}
	val, err := db.client.HeadObject(ctx, req)
	if err != nil {
		switch {
		case RecoverNotFound(err):
			return db.codec.Undefined, ErrNotFound(err, key)
		default:
			return db.codec.Undefined, ErrServiceIO.New(err, db.bucket, key)
		}
	}

	obj := db.codec.DecodeHasObject(val)
	return obj, nil
}
