package s3url

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
	err     error
}

func newObject[T stream.Thing](storage *Storage[T], entity T) Object[T] {
	return Object[T]{
		storage: storage,
		entity:  entity,
	}
}

func (obj Object[T]) Wait(timeout time.Duration) error {
	if obj.err != nil {
		return errServiceIO(obj.err)
	}

	err := obj.storage.waiter.Wait(
		context.Background(),
		&s3.HeadObjectInput{
			Bucket: aws.String(obj.storage.bucket),
			Key:    aws.String(obj.storage.codec.EncodeKey(obj.entity)),
		},
		timeout,
	)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

func (obj Object[T]) CopyTo(target T) Object[T] {
	_, err := obj.storage.client.CopyObject(
		context.Background(),
		&s3.CopyObjectInput{
			Bucket:     aws.String(obj.storage.bucket),
			Key:        aws.String(obj.storage.codec.EncodeKey(target)),
			CopySource: aws.String(obj.storage.bucket + "/" + obj.storage.codec.EncodeKey(obj.entity)),
		},
	)

	return Object[T]{
		storage: obj.storage,
		entity:  target,
		err:     err,
	}
}
