package s3ts

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (db *Store[T]) Wait(ctx context.Context, key T, timeout time.Duration) error {
	c, k := db.codec.EncodeKey(key)
	req := &s3.HeadObjectInput{
		Bucket: db.maybeBucket(c),
		Key:    aws.String(k),
	}

	err := db.waiter.Wait(ctx, req, timeout)
	if err != nil {
		return ErrServiceIO.New(err, aws.ToString(req.Bucket), aws.ToString(req.Key))
	}

	return nil
}
