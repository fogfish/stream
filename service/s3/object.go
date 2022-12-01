package s3

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/stream"
)

type Object[T stream.Thing] struct {
	storage *Storage[T]
	entity  T
}

func newObject[T stream.Thing](storage *Storage[T], entity T) Object[T] {
	return Object[T]{
		storage: storage,
		entity:  entity,
	}
}

func (obj Object[T]) Wait(ctx context.Context, timeout time.Duration) error {
	err := obj.storage.waiter.Wait(ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(obj.storage.bucket),
			Key:    aws.String(obj.storage.codec.EncodeKey(obj.entity)),
		},
		timeout,
	)
	if err != nil {
		return errServiceIO.New(err)
	}

	return nil
}

func (obj Object[T]) CopyTo(ctx context.Context, target T) (Object[T], error) {
	_, err := obj.storage.client.CopyObject(ctx,
		&s3.CopyObjectInput{
			Bucket:     aws.String(obj.storage.bucket),
			Key:        aws.String(obj.storage.codec.EncodeKey(target)),
			CopySource: aws.String(obj.storage.bucket + "/" + obj.storage.codec.EncodeKey(obj.entity)),
		},
	)
	if err != nil {
		return Object[T]{}, errServiceIO.New(err)
	}

	return Object[T]{
		storage: obj.storage,
		entity:  target,
	}, nil
}
