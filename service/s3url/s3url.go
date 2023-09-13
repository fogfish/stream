package s3url

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/codec"
)

type Storage[T stream.Thing] struct {
	bucket string
	client *s3.Client
	signer *s3.PresignClient
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

	signer := s3.NewPresignClient(client)
	waiter := s3.NewObjectExistsWaiter(client)

	return &Storage[T]{
		bucket: bucket,
		client: client,
		signer: signer,
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
func (db *Storage[T]) Put(ctx context.Context, entity T, opts ...interface{ WriterOpt(T) }) (string, error) {
	expiresIn := time.Duration(20 * time.Minute)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Timeout() time.Duration }:
			expiresIn = v.Timeout()
		}
	}

	key := db.codec.EncodeKey(entity)
	req := db.codec.Encode(entity)
	req.Bucket = aws.String(db.bucket)

	_, err := db.client.HeadObject(ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(db.bucket),
			Key:    aws.String(key),
		},
	)
	if err != nil {
		switch {
		case recoverNotFound(err):
			_, err := db.client.PutObject(ctx, req)
			if err != nil {
				return "", errServiceIO.New(err, db.bucket, key)
			}
		default:
			return "", errServiceIO.New(err, db.bucket, key)
		}
	}

	val, err := db.signer.PresignPutObject(ctx, req, s3.WithPresignExpires(expiresIn))
	if err != nil {
		return "", err
	}

	return val.URL, nil
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
func (db *Storage[T]) Has(ctx context.Context, entity T, opts ...interface{ GetterOpt(T) }) (T, error) {
	key := db.codec.EncodeKey(entity)
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

// Get
func (db *Storage[T]) Get(ctx context.Context, entity T, opts ...interface{ GetterOpt(T) }) (string, error) {
	expiresIn := time.Duration(20 * time.Minute)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Timeout() time.Duration }:
			expiresIn = v.Timeout()
		}
	}

	key := db.codec.EncodeKey(entity)
	req := &s3.GetObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(key),
	}

	val, err := db.signer.PresignGetObject(ctx, req, s3.WithPresignExpires(expiresIn))
	if err != nil {
		return "", errServiceIO.New(err, db.bucket, key)
	}

	return val.URL, nil
}

// Match
func (db *Storage[T]) Match(ctx context.Context, key T, opts ...interface{ MatcherOpt(T) }) ([]string, interface{ MatcherOpt(T) }, error) {
	expiresIn := time.Duration(20 * time.Minute)
	for _, opt := range opts {
		switch v := opt.(type) {
		case interface{ Timeout() time.Duration }:
			expiresIn = v.Timeout()
		}
	}

	req := db.reqListObjects(key, opts...)
	val, err := db.client.ListObjectsV2(context.Background(), req)
	if err != nil {
		return nil, nil, errServiceIO.New(err, db.bucket, db.codec.EncodeKey(key))
	}

	seq := make([]string, val.KeyCount)
	for i := 0; i < int(val.KeyCount); i++ {
		key := *val.Contents[i].Key

		req := &s3.GetObjectInput{
			Bucket: aws.String(db.bucket),
			Key:    aws.String(key),
		}

		signed, err := db.signer.PresignGetObject(context.Background(), req, s3.WithPresignExpires(expiresIn))
		if err != nil {
			return nil, nil, errServiceIO.New(err, db.bucket, key)
		}

		seq[i] = signed.URL
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
