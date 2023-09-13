package s3

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/codec"
)

type Storage[T stream.Thing] struct {
	bucket string
	client *s3.Client
	upload *manager.Uploader
	waiter *s3.ObjectExistsWaiter
	codec  codec.Codec[T]
}

func New[T stream.Thing](opts ...Option) (*Storage[T], error) {
	conf := defaultOptions()
	for _, opt := range opts {
		opt(conf)
	}

	bucket := conf.bucket
	if bucket == "" {
		return nil, errUndefinedBucket.New(nil)
	}

	client, err := newClient(bucket, conf)
	if err != nil {
		return nil, err
	}

	upload := manager.NewUploader(client)
	waiter := s3.NewObjectExistsWaiter(client)

	return &Storage[T]{
		bucket: bucket,
		client: client,
		upload: upload,
		waiter: waiter,
		codec:  codec.New[T](conf.prefixes),
	}, nil
}

func newClient(bucket string, conf *Options) (*s3.Client, error) {
	if conf.service != nil {
		return conf.service, nil
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(aws), nil
}

// Put
func (db *Storage[T]) Put(ctx context.Context, entity T, val io.Reader, opts ...interface{ WriterOpt(T) }) error {
	req := db.codec.Encode(entity)
	req.Bucket = aws.String(db.bucket)
	req.Body = val

	_, err := db.upload.Upload(ctx, req)
	if err != nil {
		return errServiceIO.New(err, db.bucket, db.codec.EncodeKey(entity))
	}
	return nil
}

// Remove
func (db *Storage[T]) Remove(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) error {
	key := db.codec.EncodeKey(entity)
	req := &s3.DeleteObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(key),
	}

	_, err := db.client.DeleteObject(ctx, req)
	if err != nil {
		return errServiceIO.New(err, db.bucket, key)
	}

	return nil
}

// Has
func (db *Storage[T]) Has(ctx context.Context, key T, opts ...interface{ GetterOpt(T) }) (T, error) {
	return db.has(ctx, db.codec.EncodeKey(key))
}

func (db *Storage[T]) has(ctx context.Context, key string) (T, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(key),
	}
	val, err := db.client.HeadObject(ctx, req)
	if err != nil {
		switch {
		case recoverNotFound(err):
			return db.codec.Undefined, errNotFound(err, key)
		default:
			return db.codec.Undefined, errServiceIO.New(err, db.bucket, key)
		}
	}

	obj := db.codec.DecodeHasObject(val)
	return obj, nil
}

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T, opts ...interface{ GetterOpt(T) }) (T, io.ReadCloser, error) {
	return db.get(ctx, db.codec.EncodeKey(key))
}

func (db *Storage[T]) get(ctx context.Context, key string) (T, io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(key),
	}
	val, err := db.client.GetObject(ctx, req)
	if err != nil {
		switch {
		case recoverNoSuchKey(err):
			return db.codec.Undefined, nil, errNotFound(err, key)
		default:
			return db.codec.Undefined, nil, errServiceIO.New(err, db.bucket, key)
		}
	}

	obj := db.codec.DecodeGetObject(val)
	return obj, val.Body, nil
}

// Match
func (db *Storage[T]) Match(ctx context.Context, key T, opts ...interface{ MatcherOpt(T) }) ([]T, interface{ MatcherOpt(T) }, error) {
	req := db.reqListObjects(key, opts...)
	val, err := db.client.ListObjectsV2(context.Background(), req)
	if err != nil {
		return nil, nil, errServiceIO.New(err, db.bucket, db.codec.EncodeKey(key))
	}

	seq := make([]T, val.KeyCount)
	for i := 0; i < int(val.KeyCount); i++ {
		key := *val.Contents[i].Key

		seq[i], err = db.has(ctx, key)
		if err != nil {
			return nil, nil, errServiceIO.New(err, db.bucket, key)
		}
	}

	return seq, lastKeyToCursor[T](val), nil
}

type cursor struct{ hashKey, sortKey string }

func (c cursor) HashKey() curie.IRI { return curie.IRI(c.hashKey) }
func (c cursor) SortKey() curie.IRI { return curie.IRI(c.sortKey) }

func lastKeyToCursor[T stream.Thing](val *s3.ListObjectsV2Output) interface{ MatcherOpt(T) } {
	if val.KeyCount == 0 || val.NextContinuationToken == nil {
		return nil
	}

	return stream.Cursor[T](&cursor{hashKey: *val.Contents[val.KeyCount-1].Key})
}

func (db *Storage[T]) reqListObjects(key T, opts ...interface{ MatcherOpt(T) }) *s3.ListObjectsV2Input {
	var (
		limit  int32   = 1000
		cursor *string = nil
	)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Limit() int32 }:
			limit = v.Limit()
		case stream.Thing:
			cursor = aws.String(db.codec.EncodeKey(v))
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket:     aws.String(db.bucket),
		MaxKeys:    limit,
		Prefix:     aws.String(db.codec.EncodeKey(key)),
		StartAfter: cursor,
	}
}

func (db *Storage[T]) Wait(ctx context.Context, key T, timeout time.Duration) error {
	err := db.waiter.Wait(ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(db.bucket),
			Key:    aws.String(db.codec.EncodeKey(key)),
		},
		timeout,
	)
	if err != nil {
		return errServiceIO.New(err, db.bucket, db.codec.EncodeKey(key))
	}

	return nil
}

func (db *Storage[T]) Copy(ctx context.Context, source T, target T) error {
	_, err := db.client.CopyObject(ctx,
		&s3.CopyObjectInput{
			Bucket:     aws.String(db.bucket),
			Key:        aws.String(db.codec.EncodeKey(target)),
			CopySource: aws.String(db.bucket + "/" + db.codec.EncodeKey(source)),
		},
	)
	if err != nil {
		return errServiceIO.New(err, db.bucket, db.codec.EncodeKey(target))
	}

	return nil
}
