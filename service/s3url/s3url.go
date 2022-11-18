package s3url

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
	s3s "github.com/fogfish/stream/internal/s3"
)

type Storage[T stream.Thing] struct {
	bucket string
	client *s3.Client
	signer *s3.PresignClient
	codec  s3s.Codec[T]
}

func New[T stream.Thing](bucket string) (*Storage[T], error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg)
	signer := s3.NewPresignClient(client)

	return &Storage[T]{
		bucket: bucket,
		client: client,
		signer: signer,
		codec:  s3s.NewCodec[T](curie.Namespaces{}),
	}, nil
}

// Put
func (db *Storage[T]) Put(ctx context.Context, entity T, expire time.Duration) (string, error) {
	req := db.codec.Encode(entity)
	req.Bucket = aws.String(db.bucket)

	// TODO: head (check is exists)
	_, err := db.client.PutObject(ctx, req)
	if err != nil {
		return "", err
	}

	val, err := db.signer.PresignPutObject(ctx, req)
	if err != nil {
		return "", err
	}

	return val.URL, nil
}

func (db *Storage[T]) Has(ctx context.Context, key T) (T, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(db.codec.EncodeKey(key)),
	}
	val, err := db.client.HeadObject(ctx, req)
	if err != nil {
		switch {
		case recoverNotFound(err):
			return db.undefined, nil
		default:
			return db.undefined, errServiceIO(err)
		}
	}

	obj := db.codec.DecodeHasObject(val)
	return obj, nil
}

// Get
func (db *Storage[T]) Get(ctx context.Context, key T, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: aws.String(db.bucket),
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	val, err := db.signer.PresignGetObject(ctx, req)
	if err != nil {
		return "", err
	}

	return val.URL, nil
}

// Match
func (db *Storage[T]) Match(ctx context.Context, key T, expire time.Duration) *Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  aws.String(db.bucket),
		MaxKeys: int32(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(db, req)
}
