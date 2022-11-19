package s3

import (
	"context"
	"io"

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

func (seq *Seq[T]) Head() (T, io.ReadCloser, error) {
	key, err := seq.Seq.Head()
	if err != nil {
		return seq.storage.codec.Undefined, nil, err
	}

	val, vio, err := seq.storage.get(context.Background(), key)
	if err != nil {
		return seq.storage.codec.Undefined, nil, errServiceIO(err)
	}

	return val, vio, nil
}

func (seq *Seq[T]) FMap(f func(T, io.ReadCloser) error) error {
	for seq.Tail() {
		key, val, err := seq.Head()
		if err != nil {
			return err
		}

		if err := f(key, val); err != nil {
			return errProcessEntity(err, key)
		}
	}

	return seq.Seq.Error()
}
