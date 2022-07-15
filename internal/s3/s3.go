package s3

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/stream"
)

// ds3 is a S3 client
type s3fs[T stream.Thing] struct {
	s3api     *s3.Client
	s3sign    *s3.PresignClient
	upload    *manager.Uploader
	waiter    *s3.ObjectExistsWaiter
	codec     Codec[T]
	bucket    *string
	undefined T
}

func New[T stream.Thing](cfg *stream.Config) stream.Stream[T] {
	s3api := s3.NewFromConfig(cfg.AWS)
	s3sign := s3.NewPresignClient(s3api)
	upload := manager.NewUploader(s3api)
	waiter := s3.NewObjectExistsWaiter(s3api)

	db := &s3fs[T]{
		s3api:  s3api,
		s3sign: s3sign,
		upload: upload,
		waiter: waiter,
	}

	// config bucket name
	seq := cfg.URI.Segments()
	db.bucket = &seq[0]

	//
	db.codec = NewCodec[T](cfg.Prefixes)

	return db
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (db *s3fs[T]) Has(ctx context.Context, key T) (T, error) {
	req := &s3.HeadObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}
	val, err := db.s3api.HeadObject(ctx, req)
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

// fetch direct download url
func (db *s3fs[T]) URL(ctx context.Context, key T, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	val, err := db.s3sign.PresignGetObject(ctx, req)
	if err != nil {
		return "", errServiceIO(err)
	}

	return val.URL, nil
}

// Get item from storage
func (db *s3fs[T]) Get(ctx context.Context, key T) (T, io.ReadCloser, error) {
	return db.get(ctx, db.codec.EncodeKey(key))
}

func (db *s3fs[T]) get(ctx context.Context, key string) (T, io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(key),
	}
	val, err := db.s3api.GetObject(ctx, req)
	if err != nil {
		switch {
		case recoverNoSuchKey(err):
			return db.undefined, nil, errNotFound(err, key)
		default:
			return db.undefined, nil, errServiceIO(err)
		}
	}

	obj := db.codec.DecodeGetObject(val)
	return obj, val.Body, nil
}

// Put writes entity
func (db *s3fs[T]) Put(ctx context.Context, entity T, val io.ReadCloser) error {
	req := db.codec.Encode(entity)
	req.Bucket = db.bucket
	req.Body = val

	_, err := db.upload.Upload(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}
	return nil
}

// Copy entity
func (db *s3fs[T]) Copy(ctx context.Context, source T, target T) error {
	_, err := db.s3api.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     db.bucket,
		Key:        aws.String(db.codec.EncodeKey(target)),
		CopySource: aws.String(*db.bucket + "/" + db.codec.EncodeKey(source)),
	})
	if err != nil {
		return errServiceIO(err)
	}

	err = db.waiter.Wait(ctx, &s3.HeadObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(target)),
	}, 60*time.Second)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

// Remove discards the entity from the table
func (db *s3fs[T]) Remove(ctx context.Context, key T) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3api.DeleteObject(ctx, req)
	if err != nil {
		return errServiceIO(err)
	}

	return nil
}

func (db *s3fs[T]) Match(ctx context.Context, key T) stream.Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.bucket,
		MaxKeys: int32(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
