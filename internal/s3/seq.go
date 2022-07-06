package s3

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
)

//
type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

// seq is an iterator over matched results
type seq[T stream.Thing] struct {
	ctx    context.Context
	db     *s3fs[T]
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func newSeq[T stream.Thing](
	ctx context.Context,
	db *s3fs[T],
	q *s3.ListObjectsV2Input,
	err error,
) *seq[T] {
	return &seq[T]{
		ctx:    ctx,
		db:     db,
		q:      q,
		at:     0,
		items:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *seq[T]) maybeSeed() error {
	if !seq.stream {
		return errEndOfStream()
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.items != nil && seq.q.StartAfter == nil {
		return errEndOfStream()
	}

	val, err := seq.db.s3api.ListObjectsV2(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return errServiceIO(err)
	}

	if val.KeyCount == 0 {
		return errEndOfStream()
	}

	items := make([]*string, 0)
	for _, x := range val.Contents {
		items = append(items, x.Key)
	}

	seq.at = 0
	seq.items = items
	if len(items) > 0 && val.NextContinuationToken != nil {
		seq.q.StartAfter = items[len(items)-1]
	}

	if val.NextContinuationToken == nil {
		seq.q.StartAfter = nil
	}

	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(T, io.ReadCloser) error) error {
	for seq.Tail() {
		key, val, err := seq.Head()
		if err != nil {
			return err
		}

		if err := f(key, val); err != nil {
			return errProcessEntity(err, key)
		}
	}
	return seq.err
}

// Head selects the first element of matched collection.
func (seq *seq[T]) Head() (T, io.ReadCloser, error) {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return seq.db.undefined, nil,
				fmt.Errorf("can't seed head of stream: %w", err)
		}
	}

	val, vio, err := seq.db.get(seq.ctx, *seq.items[seq.at])
	if err != nil {
		return seq.db.undefined, nil, errServiceIO(err)
	}

	return val, vio, nil
}

// Tail selects the all elements except the first one
func (seq *seq[T]) Tail() bool {
	seq.at++

	switch {
	case seq.err != nil:
		return false
	case seq.items == nil:
		err := seq.seed()
		return err == nil
	case seq.err == nil && seq.at >= len(seq.items):
		err := seq.maybeSeed()
		return err == nil
	default:
		return true
	}
}

// Cursor is the global position in the sequence
func (seq *seq[T]) Cursor() stream.Thing {
	if seq.q.StartAfter != nil {
		return &cursor{hashKey: *seq.q.StartAfter}
	}
	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *seq[T]) Limit(n int64) stream.Seq[T] {
	seq.q.MaxKeys = int32(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key stream.Thing) stream.Seq[T] {
	// Note: s3 cursor supports only HashKey
	prefix := key.HashKey()

	if prefix != "" {
		seq.q.StartAfter = aws.String(string(prefix))
	}

	return seq
}

// Reverse order of sequence
func (seq *seq[T]) Reverse() stream.Seq[T] {
	return seq
}
