//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

// Package stream provides a Golang file system abstraction tailored for AWS S3,
// enabling seamless streaming of binary objects along with their
// corresponding metadata. The package implements
// [Golang File System](https://pkg.go.dev/io/fs) and enhances it by adding
// support for writable files and type-safe metadata.
package stream

import (
	"context"
	"errors"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// File System Configuration Options
type Opts struct {
	api          S3
	upload       S3Upload
	signer       S3Signer
	timeout      time.Duration
	ttlSignedUrl time.Duration
	lslimit      int32
}

// File System
type FileSystem[T any] struct {
	Opts
	codec  *codec[T]
	bucket string
}

var (
	_ fs.FS              = (*FileSystem[struct{}])(nil)
	_ fs.StatFS          = (*FileSystem[struct{}])(nil)
	_ fs.ReadDirFS       = (*FileSystem[struct{}])(nil)
	_ fs.GlobFS          = (*FileSystem[struct{}])(nil)
	_ CreateFS[struct{}] = (*FileSystem[struct{}])(nil)
	_ RemoveFS           = (*FileSystem[struct{}])(nil)
	_ CopyFS             = (*FileSystem[struct{}])(nil)
)

// Create a file system instance, mounting S3 Bucket. Use Option type to
// configure file system.
func New[T any](bucket string, opts ...Option) (*FileSystem[T], error) {
	fsys := &FileSystem[T]{
		Opts: Opts{
			timeout:      120 * time.Second,
			ttlSignedUrl: 5 * time.Minute,
			lslimit:      1000,
		},
		bucket: bucket,
		codec:  newCodec[T](),
	}

	for _, opt := range opts {
		opt(&fsys.Opts)
	}

	if fsys.api == nil {
		aws, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		api := s3.NewFromConfig(aws)

		fsys.api = api
		fsys.upload = manager.NewUploader(api)

		if fsys.codec.s != nil && fsys.signer == nil {
			fsys.signer = s3.NewPresignClient(api)
		}
	}

	return fsys, nil
}

// Create a file system instance, mounting S3 Bucket. Use Option type to
// configure file system.
func NewFS(bucket string, opts ...Option) (*FileSystem[struct{}], error) {
	return New[struct{}](bucket, opts...)
}

// To open the file for writing use `Create` function giving the absolute path
// starting with `/`, the returned file descriptor is a composite of
// `io.Writer`, `io.Closer` and `stream.Stat`. Utilize Golang's convenient
// streaming methods to update S3 object seamlessly. Once all bytes are written,
// it's crucial to close the stream. Failure to do so would cause data loss.
// The object is considered successfully created on S3 only if all `Write`
// operations and subsequent `Close` actions are successful.
func (fsys *FileSystem[T]) Create(path string, attr *T) (File, error) {
	if err := RequireValidFile("create", path); err != nil {
		return nil, err
	}

	return newWriter(fsys, path, attr), nil
}

// To open the file for reading use `Open` function giving the absolute path
// starting with `/`, the returned file descriptor is a composite of
// `io.Reader`, `io.Closer` and `stream.Stat`. Utilize Golang's convenient
// streaming methods to consume S3 object seamlessly.
func (fsys *FileSystem[T]) Open(path string) (fs.File, error) {
	if err := RequireValidPath("open", path); err != nil {
		return nil, err
	}

	if IsValidDir(path) {
		return openDir(fsys, path), nil
	}

	return newReader(fsys, path), nil
}

// Stat returns a FileInfo describing the file.
// File system executes HeadObject S3 API call to obtain metadata.
func (fsys *FileSystem[T]) Stat(path string) (fs.FileInfo, error) {
	if err := RequireValidPath("stat", path); err != nil {
		return nil, err
	}

	info := info[T]{path: path}

	if IsValidDir(path) {
		info.mode = fs.ModeDir
		return info, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), fsys.timeout)
	defer cancel()

	req := &s3.HeadObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    info.s3Key(),
	}

	val, err := fsys.api.HeadObject(ctx, req)
	if err != nil {
		switch {
		case recoverNotFound(err):
			return nil, fs.ErrNotExist
		default:
			return nil, &fs.PathError{
				Op:   "stat",
				Path: path,
				Err:  err,
			}
		}
	}

	info.size = aws.ToInt64(val.ContentLength)
	info.time = aws.ToTime(val.LastModified)
	info.attr = new(T)
	fsys.codec.DecodeHeadOutput(val, info.attr)

	if fsys.signer != nil && fsys.codec.s != nil {
		if url, err := fsys.preSignGetUrl(info.s3Key()); err == nil {
			fsys.codec.s.Put(info.attr, url)
		}
	}

	return info, nil
}

// Returns file metadata of type T embedded into a FileInfo.
func (fsys *FileSystem[T]) StatSys(stat fs.FileInfo) *T {
	info, ok := stat.(info[T])
	if !ok {
		return nil
	}

	return info.attr
}

func (fsys *FileSystem[T]) preSignGetUrl(s3key *string) (string, error) {
	req := &s3.GetObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    s3key,
	}

	ctx, cancel := context.WithTimeout(context.Background(), fsys.timeout)
	defer cancel()

	val, err := fsys.signer.PresignGetObject(ctx, req, s3.WithPresignExpires(fsys.ttlSignedUrl))
	if err != nil {
		return "", &fs.PathError{
			Op:   "presign",
			Path: "/" + aws.ToString(s3key),
			Err:  err,
		}
	}

	return val.URL, nil
}

// Reads the named directory or path prefix.
// The classical file system organize data hierarchically into directories as
// opposed to the flat storage structure of general purpose AWS S3.
//
// It assumes a directory if the path ends with `/`.
//
// It return path relative to pattern for all found object.
func (fsys *FileSystem[T]) ReadDir(path string) ([]fs.DirEntry, error) {
	if err := RequireValidDir("readdir", path); err != nil {
		return nil, err
	}

	dd := openDir(fsys, path)

	return dd.ReadDir(-1)
}

// Glob returns the names of all files matching pattern.
// The classical file system organize data hierarchically into directories as
// opposed to the flat storage structure of general purpose AWS S3.
//
// It assumes a directory if the path ends with `/`.
//
// It return path relative to pattern for all found object.
//
// The pattern consists of S3 key prefix Golang regex. Its are split by `|`.
func (fsys *FileSystem[T]) Glob(pattern string) ([]string, error) {
	var reg *regexp.Regexp
	var err error

	pat := strings.SplitN(pattern, "|", 2)
	if len(pat) == 2 {
		reg, err = regexp.Compile(pat[1])
		if err != nil {
			return nil, &fs.PathError{
				Op:   "glob",
				Path: pattern,
				Err:  err,
			}
		}
	}

	dir, err := fsys.ReadDir(pat[0])
	if err != nil {
		return nil, err
	}

	seq := make([]string, 0)
	for _, x := range dir {
		if reg == nil || reg.MatchString(x.Name()) {
			seq = append(seq, x.Name())
		}
	}
	return seq, nil
}

// Remove object
func (fsys *FileSystem[T]) Remove(path string) error {
	if err := RequireValidFile("remove", path); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), fsys.timeout)
	defer cancel()

	req := &s3.DeleteObjectInput{
		Bucket: &fsys.bucket,
		Key:    s3Key(path),
	}

	_, err := fsys.api.DeleteObject(ctx, req)
	if err != nil {
		return &fs.PathError{
			Op:   "remove",
			Path: path,
			Err:  err,
		}
	}

	return nil
}

// Copy object from source location to the target.
// The target shall be absolute s3://bucket/key url.
func (fsys *FileSystem[T]) Copy(source, target string) error {
	if err := RequireValidPath("copy", source); err != nil {
		return err
	}

	if !strings.HasPrefix(target, "s3://") {
		return &fs.PathError{
			Op:   "copy",
			Path: target,
			Err:  errors.New("s3:// prefix is required"),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), fsys.timeout)
	defer cancel()

	req := &s3.CopyObjectInput{
		Bucket:     &fsys.bucket,
		Key:        s3Key(source),
		CopySource: aws.String(target[5:]),
	}

	_, err := fsys.api.CopyObject(ctx, req)
	if err != nil {
		return &fs.PathError{
			Op:   "copy",
			Path: target,
			Err:  err,
		}
	}

	return nil
}

// Wait for timeout until path exists
func (fsys *FileSystem[T]) Wait(path string, timeout time.Duration) error {
	if err := RequireValidFile("wait", path); err != nil {
		return err
	}

	waiter := s3.NewObjectExistsWaiter(fsys.api)

	req := &s3.HeadObjectInput{
		Bucket: aws.String(fsys.bucket),
		Key:    s3Key(path),
	}

	err := waiter.Wait(context.Background(), req, timeout)
	if err != nil {
		return &fs.PathError{
			Op:   "wait",
			Path: path,
			Err:  err,
		}
	}

	return nil
}

//------------------------------------------------------------------------------

func recoverNoSuchKey(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NoSuchKey"
}

func recoverNotFound(err error) bool {
	var e interface{ ErrorCode() string }

	ok := errors.As(err, &e)
	return ok && e.ErrorCode() == "NotFound"
}
