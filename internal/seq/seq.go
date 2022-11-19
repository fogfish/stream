package seq

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
)

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

// seq is an iterator over matched results
type Seq struct {
	client *s3.Client
	q      *s3.ListObjectsV2Input
	at     int
	items  []*string
	stream bool
	err    error
}

func New(client *s3.Client, q *s3.ListObjectsV2Input, err error) *Seq {
	return &Seq{
		client: client,
		q:      q,
		at:     0,
		items:  nil,
		stream: true,
		err:    err,
	}
}

func (seq *Seq) maybeSeed() error {
	if !seq.stream {
		return errEndOfStream()
	}

	return seq.seed()
}

func (seq *Seq) seed() error {
	if seq.items != nil && seq.q.StartAfter == nil {
		return errEndOfStream()
	}

	val, err := seq.client.ListObjectsV2(context.Background(), seq.q)
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

// Head selects the first element of matched collection.
func (seq *Seq) Head() (string, error) {
	if seq.items == nil {
		if err := seq.seed(); err != nil {
			return "",
				fmt.Errorf("can't seed head of stream: %w", err)
		}
	}

	key := *seq.items[seq.at]
	return key, nil
}

// Tail selects the all elements except the first one
func (seq *Seq) Tail() bool {
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
func (seq *Seq) Cursor() stream.Thing {
	if seq.q.StartAfter != nil {
		return &cursor{hashKey: *seq.q.StartAfter}
	}
	return &cursor{}
}

// Error indicates if any error appears during I/O
func (seq *Seq) Error() error {
	return seq.err
}

// Limit sequence to N elements
func (seq *Seq) Limit(n int64) *Seq {
	seq.q.MaxKeys = int32(n)
	seq.stream = false
	return seq
}

// Continue limited sequence from the cursor
func (seq *Seq) Continue(key stream.Thing) *Seq {
	// Note: s3 cursor supports only HashKey
	prefix := key.HashKey()

	if prefix != "" {
		seq.q.StartAfter = aws.String(string(prefix))
	}

	return seq
}

func errEndOfStream() error {
	return errors.New("end of stream")
}

func errServiceIO(err error) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[stream.seq.%s] service i/o failed: %w", name, err)
}
