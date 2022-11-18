package s3url

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/seq"
)

type Seq[T stream.Thing] struct {
	*seq.Seq
	storage *Storage[T]
}

func newSeq[T stream.Thing](storage *Storage[T], q *s3.ListObjectsV2Input) *Seq[T] {
	return &Seq[T]{
		Seq:     seq.New(storage.client, q, nil),
		storage: storage,
	}
}

func (seq *Seq[T]) Head() (string, error) {
	key, err := seq.Seq.Head()
	if err != nil {
		return "", err
	}

	req := &s3.GetObjectInput{
		Bucket: aws.String(seq.storage.bucket),
		Key:    aws.String(key),
	}

	val, err := seq.storage.signer.PresignGetObject(context.Background(), req)
	if err != nil {
		return "", err
	}

	return val.URL, nil
}

func (seq *Seq[T]) FMap(f func(string) error) error {
	for seq.Tail() {
		key, err := seq.Head()
		if err != nil {
			return err
		}

		if err := f(key); err != nil {
			return errProcessEntity(err, key)
		}
	}

	return seq.Seq.Error()
}
