package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func New[T stream.Thing](bucket string, opts ...stream.Option) (*Storage[T], error) {
	conf := stream.NewConfig()
	for _, opt := range opts {
		opt(&conf)
	}

	client, err := newClient(bucket, &conf)
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
		codec:  codec.New[T](conf.Prefixes),
	}, nil
}

func newClient(bucket string, conf *stream.Config) (*s3.Client, error) {
	if conf.Service != nil {
		service, ok := conf.Service.(*s3.Client)
		if ok {
			return service, nil
		}
	}

	aws, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(aws), nil
}

// Put
func (db *Storage[T]) Put(ctx context.Context, entity T, val io.ReadCloser) error {
	req := db.codec.Encode(entity)
	req.Bucket = aws.String(db.bucket)
	req.Body = val

	_, err := db.upload.Upload(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}
	return nil
}

// Remove
func (db *Storage[T]) Remove(ctx context.Context, key T) error {
	req := &s3.DeleteObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.client.DeleteObject(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

// Has
func (db *Storage[T]) Has(ctx context.Context, key T) (T, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(db.codec.EncodeKey(key)),
	}
	val, err := db.client.HeadObject(ctx, req)
	if err != nil {
		switch {
		case recoverNotFound(err):
			return db.codec.Undefined, errNotFound(err, db.codec.EncodeKey(key))
		default:
			return db.codec.Undefined, errServiceIO(err)
		}
	}

	obj := db.codec.DecodeHasObject(val)
	return obj, nil
}

// Get item from storage
func (db *Storage[T]) Get(ctx context.Context, key T) (T, io.ReadCloser, error) {
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
			return db.codec.Undefined, nil, errServiceIO(err)
		}
	}

	obj := db.codec.DecodeGetObject(val)
	return obj, val.Body, nil
}

// Match
func (db *Storage[T]) Match(ctx context.Context, key T) *Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  aws.String(db.bucket),
		MaxKeys: int32(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(db, req)
}

// With
func (db *Storage[T]) With(entity T) Object[T] {
	return newObject(db, entity)
}
