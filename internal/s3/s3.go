package s3

import (
	"context"
	"io"
	"net"
	"net/http"
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
type s3fs struct {
	io     *session.Session
	s3     s3iface.S3API
	http   *http.Client
	codec  Codec
	bucket *string
}

func New(
	io *session.Session,
	spec *stream.URL,
) stream.Stream {
	db := &s3fs{io: io, s3: s3.New(io)}

	// config bucket name
	seq := spec.Segments(2)
	db.bucket = seq[0]

	//
	db.codec = Codec{}

	//
	db.http = &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			ReadBufferSize:    1024 * 1024,
			Dial: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).Dial,
			// TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}

	return db
}

// Mock S3 I/O channel
func (db *s3fs) Mock(s3 s3iface.S3API) {
	db.s3 = s3
}

//-----------------------------------------------------------------------------
//
// Key Value
//
//-----------------------------------------------------------------------------

func (db *s3fs) Has(
	ctx context.Context,
	key stream.Thing,
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
func (db *s3fs) URL(
	ctx context.Context,
	key stream.Thing,
	expire time.Duration,
) (string, error) {
	return db.url(ctx, aws.String(db.codec.EncodeKey(key)), expire)
}

func (db *s3fs) url(
	ctx context.Context,
	key *string,
	expire time.Duration,
) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: db.bucket,
		Key:    key,
	}

	item, _ := db.s3.GetObjectRequest(req)
	item.SetContext(ctx)
	return item.Presign(expire)
}

// Get item from storage
func (db *s3fs) Get(ctx context.Context, key stream.Thing) (io.ReadCloser, error) {
	url, err := db.URL(ctx, key, 20*time.Minute)
	if err != nil {
		return nil, err
	}
	return db.get(ctx, url)
}

func (db *s3fs) get(ctx context.Context, url string) (io.ReadCloser, error) {
	eg, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	eg.Header.Add("Connection", "close")
	eg.Header.Add("Transfer-Encoding", "chunked")

	in, err := db.http.Do(eg)
	if err != nil {
		return nil, err
	}

	return in.Body, nil
}

// Put writes entity
func (db *s3fs) Put(ctx context.Context, key stream.Thing, val io.ReadCloser) error {
	up := s3manager.NewUploader(db.io)

	req := &s3manager.UploadInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
		Body:   val,
	}

	// TODO
	// for _, f := range opts {
	// 	f(req)
	// }
	_, err := up.UploadWithContext(ctx, req)
	return err
}

// Remove discards the entity from the table
func (db *s3fs) Remove(ctx context.Context, key stream.Thing) error {
	req := &s3.DeleteObjectInput{
		Bucket: db.bucket,
		Key:    aws.String(db.codec.EncodeKey(key)),
	}

	_, err := db.s3.DeleteObjectWithContext(ctx, req)

	return err
}

func (db *s3fs) Match(ctx context.Context, key stream.Thing) stream.Seq {
	req := &s3.ListObjectsV2Input{
		Bucket:  db.bucket,
		MaxKeys: aws.Int64(1000),
		Prefix:  aws.String(db.codec.EncodeKey(key)),
	}

	return newSeq(ctx, db, req, nil)
}
