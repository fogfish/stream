package s3

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fogfish/stream"
)

//
type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() string { return c.hashKey }
func (c cursor) SortKey() string { return c.sortKey }

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
		return stream.EOS{}
	}

	return seq.seed()
}

func (seq *seq[T]) seed() error {
	if seq.items != nil && seq.q.StartAfter == nil {
		return stream.EOS{}
	}

	val, err := seq.db.s3.ListObjectsV2WithContext(seq.ctx, seq.q)
	if err != nil {
		seq.err = err
		return err
	}

	if *val.KeyCount == 0 {
		return stream.EOS{}
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
	return nil
}

// FMap transforms sequence
func (seq *seq[T]) FMap(f func(*T, io.ReadCloser) error) error {
	for seq.Tail() {
		key, val, err := seq.Head()
		if err != nil {
			return err
		}

		if err := f(key, val); err != nil {
			return err
		}
	}
	return seq.err
}

// Head selects the first element of matched collection.
func (seq *seq[T]) Head() (*T, io.ReadCloser, error) {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return nil, nil, err
		}
	}

	return seq.db.get(seq.ctx, *seq.items[seq.at])

	// key := strings.Split(*seq.items[seq.at], "/_/")
	// if len(key) == 1 {
	// 	return &cursor{hashKey: key[0]}, nil
	// }
	// return &cursor{hashKey: key[0], sortKey: key[1]}, nil
}

// // Body
// func (seq *seq) Body() (io.ReadCloser, error) {
// 	if seq.items == nil {
// 		if err := seq.seed(); err != nil {
// 			return nil, err
// 		}
// 	}

// 	url, err := seq.db.url(seq.ctx, seq.items[seq.at], 20*time.Minute)
// 	if err != nil {
// 		return nil, err
// 	}

// }

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
		seq := strings.Split(*seq.q.StartAfter, "/_/")
		if len(seq) == 1 {
			return &cursor{hashKey: seq[0]}
		}
		return &cursor{hashKey: seq[0], sortKey: seq[1]}
	}
	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *seq[T]) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *seq[T]) Limit(n int64) stream.Seq[T] {
	seq.q.MaxKeys = aws.Int64(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *seq[T]) Continue(key stream.Thing) stream.Seq[T] {
	prefix := key.HashKey()
	suffix := key.SortKey()

	if prefix != "" {
		if suffix == "" {
			seq.q.StartAfter = aws.String(prefix)
		} else {
			seq.q.StartAfter = aws.String(prefix + "/_/" + suffix)
		}
	}
	return seq
}

// Reverse order of sequence
func (seq *seq[T]) Reverse() stream.Seq[T] {
	return seq
}
