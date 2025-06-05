//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stream

import (
	"context"
	"io"
	"io/fs"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Stat returns a FileInfo describing the file and its metadata from the file system.
type Stat interface {
	Stat() (fs.FileInfo, error)
}

// Cancel effect of file system i/o, before file is closed.
type Canceler interface {
	Cancel() error
}

// File is a writable object
type File = interface {
	Stat
	io.Writer
	io.Closer
	Canceler
}

// File System extension supporting writable files
type CreateFS[T any] interface {
	fs.FS
	Create(path string, attr *T) (File, error)
}

// File System extension supporting file removal
type RemoveFS interface {
	fs.FS
	Remove(path string) error
}

// File System extension supporting file copying
type CopyFS interface {
	fs.FS
	Copy(source, target string) error
	Wait(path string, timeout time.Duration) error
}

// File System extension supporting I/O operations via urls
type CurlFS[T any] interface {
	fs.FS
	PutUrl(path string, attr *T, ttl time.Duration) (string, error)
	GetUrl(path string, ttl time.Duration) (string, error)
}

// well-known attributes controlled by S3 system
type SystemMetadata struct {
	CacheControl    string
	ContentEncoding string
	ContentLanguage string
	ContentType     string
	Expires         *time.Time
	ETag            string
	LastModified    *time.Time
	StorageClass    string
}

// Well-known attribute for reading pre-signed Urls of S3 objects
type PreSignedUrl struct {
	PreSignedUrl string
}

//-----------------------------------------------------------------------------

type S3 interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
}

type S3Upload interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

type S3Signer interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
	PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}
