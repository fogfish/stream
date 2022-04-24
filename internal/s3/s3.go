package s3

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fogfish/stream"
)

// ds3 is a S3 client
type s3fs[T stream.Thing] struct {
	io     *session.Session
	s3     s3iface.S3API
	codec  Codec[T]
	bucket *string
}

func New[T stream.Thing](
	io *session.Session,
	spec *stream.URL,
) stream.Stream[T] {
	db := &s3fs[T]{io: io, s3: s3.New(io)}

	// config bucket name
	seq := spec.Segments(2)
	db.bucket = seq[0]

	//
	db.codec = NewCodec[T]()

	return db
}

// Mock S3 I/O channel
func (db *s3fs[T]) Mock(s3 s3iface.S3API) {
	db.s3 = s3
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (db *s3fs[T]) Has(
	ctx context.Context,
	key T,
) (bool, error) {
	req := &s3.HeadObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}
	_, err := db.s3.HeadObjectWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				return false, nil
			}
			return false, err
		default:
			return false, err
		}
	}

	return true, nil
}

// fetch direct download url
func (db *s3fs[T]) URL(ctx context.Context, key T, expire time.Duration) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	item, _ := db.s3.GetObjectRequest(req)
	item.SetContext(ctx)
	return item.Presign(expire)
}

// Get item from storage
func (db *s3fs[T]) Get(ctx context.Context, key T) (*T, io.ReadCloser, error) {
	return db.get(ctx, db.codec.EncodeKey(key))
}

func (db *s3fs[T]) get(ctx context.Context, key string) (*T, io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(key),
	}
	val, err := db.s3.GetObjectWithContext(ctx, req)
	if err != nil {
		switch v := err.(type) {
		case awserr.Error:
			if v.Code() == s3.ErrCodeNoSuchKey {
				seq := strings.Split(key, "/_/")
				if len(seq) == 1 {
					return nil, nil, stream.NotFound{HashKey: seq[0]}
				}
				return nil, nil, stream.NotFound{HashKey: seq[0], SortKey: seq[1]}
			}
			return nil, nil, err
		default:
			return nil, nil, err
		}
	}

	obj := db.codec.Decode(val)
	return obj, val.Body, err
}

// Put writes entity
func (db *s3fs[T]) Put(ctx context.Context, entity T, val io.ReadCloser) error {
	up := s3manager.NewUploader(db.io)

	req := db.codec.Encode(entity)
	req.Bucket = db.bucket
	req.Body = val

	_, err := up.UploadWithContext(ctx, req)
	return err
}

// Remove discards the entity from the table
func (db *s3fs[T]) Remove(ctx context.Context, key T) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObjectWithContext(ctx, req)

	return err
}

func (db *s3fs[T]) Match(ctx context.Context, key T) stream.Seq[T] {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
