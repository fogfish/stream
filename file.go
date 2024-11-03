//
// Copyright (C) 2024 Dmitry Kolesnikov
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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//------------------------------------------------------------------------------

// object's metadata descriptor
type info[T any] struct {
	path string
	mode fs.FileMode
	size int64
	time time.Time
	attr *T
}

var (
	_ fs.FileInfo = info[any]{}
	_ fs.DirEntry = info[any]{}
)

func (f info[T]) Name() string               { return f.path }
func (f info[T]) Size() int64                { return f.size }
func (f info[T]) Mode() fs.FileMode          { return f.mode }
func (f info[T]) ModTime() time.Time         { return f.time }
func (f info[T]) IsDir() bool                { return f.mode.IsDir() }
func (f info[T]) Sys() any                   { return f.attr }
func (f info[T]) Type() fs.FileMode          { return f.mode.Type() }
func (f info[T]) Info() (fs.FileInfo, error) { return f, nil }

func (f info[T]) s3Key() *string { return s3Key(f.path) }

func s3Key(path string) *string {
	if path[0] == '/' {
		return aws.String(path[1:])
	}

	return aws.String(path)
}

//------------------------------------------------------------------------------

// reader file descriptor
type reader[T any] struct {
	info[T]
	fs  *FileSystem[T]
	r   io.ReadCloser
	can context.CancelFunc
}

var (
	_ fs.File = (*reader[any])(nil)
)

// open read only descriptor to file
func newReader[T any](fsys *FileSystem[T], name string) *reader[T] {
	return &reader[T]{
		info: info[T]{
			path: name,
		},
		fs: fsys,
	}
}

// check file's metadata
func (fd *reader[T]) Stat() (fs.FileInfo, error) {
	if fd.r == nil {
		if err := fd.lazyOpen(); err != nil {
			return nil, err
		}
	}

	// if fd.size == 0 {
	// 	stat, err := fd.fs.Stat(fd.path)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	info, ok := stat.(info[T])
	// 	if !ok {
	// 		return nil, fs.ErrInvalid
	// 	}

	// 	fd.info.size = info.size
	// 	fd.info.time = info.time
	// 	fd.info.attr = info.attr
	// }

	return fd.info, nil
}

func (fd *reader[T]) lazyOpen() error {
	req := &s3.GetObjectInput{
		Bucket: aws.String(fd.fs.bucket),
		Key:    fd.s3Key(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), fd.fs.timeout)

	val, err := fd.fs.api.GetObject(ctx, req)
	if err != nil {
		cancel()

		switch {
		case recoverNoSuchKey(err):
			return fs.ErrNotExist
		default:
			return &fs.PathError{
				Op:   "open",
				Path: fd.path,
				Err:  err,
			}
		}
	}

	fd.r = val.Body
	fd.can = cancel
	fd.info.size = aws.ToInt64(val.ContentLength)
	fd.info.time = aws.ToTime(val.LastModified)
	fd.info.attr = new(T)

	fd.fs.codec.DecodeGetOutput(val, fd.info.attr)
	if fd.fs.signer != nil && fd.fs.codec.s != nil {
		if url, err := fd.fs.preSignGetUrl(fd.s3Key()); err == nil {
			fd.fs.codec.s.Put(fd.info.attr, url)
		}
	}

	return nil
}

func (fd *reader[T]) Read(b []byte) (int, error) {
	if fd.r == nil {
		if err := fd.lazyOpen(); err != nil {
			return 0, err
		}
	}

	return fd.r.Read(b)
}

func (fd *reader[T]) Close() error {
	if fd.r == nil {
		return nil
	}

	defer func() {
		fd.r = nil
		fd.can = nil
	}()

	if fd.can != nil {
		fd.can()
	}

	if err := fd.r.Close(); err != nil {
		return err
	}

	return nil
}

//------------------------------------------------------------------------------

type writer[T any] struct {
	info[T]
	fs     *FileSystem[T]
	w      *io.PipeWriter
	r      *io.PipeReader
	wg     sync.WaitGroup
	upload string
	err    error
}

var (
	_ io.Writer = (*writer[any])(nil)
	_ io.Closer = (*writer[any])(nil)
)

func newWriter[T any](fsys *FileSystem[T], path string, attr *T) *writer[T] {
	return &writer[T]{
		info: info[T]{
			path: path,
			attr: attr,
		},
		fs: fsys,
	}
}

func (fd *writer[T]) lazyOpen() {
	fd.r, fd.w = io.Pipe()
	fd.wg = sync.WaitGroup{}
	fd.wg.Add(1)

	go func() {
		defer fd.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), fd.fs.timeout)
		defer cancel()

		req := &s3.PutObjectInput{
			Bucket:   aws.String(fd.fs.bucket),
			Key:      fd.s3Key(),
			Body:     fd.r,
			Metadata: make(map[string]string),
		}
		fd.fs.codec.EncodePutInput(fd.attr, req)

		if val, err := fd.fs.upload.Upload(ctx, req); err != nil {
			fd.err = &fs.PathError{
				Op:   "write",
				Path: fd.path,
				Err:  err,
			}
			fd.r.Close()
		} else {
			fd.upload = val.UploadID
		}
	}()
}

func (fd *writer[T]) preSignPutUrl() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), fd.fs.timeout)
	defer cancel()

	req := &s3.PutObjectInput{
		Bucket:   aws.String(fd.fs.bucket),
		Key:      fd.s3Key(),
		Metadata: make(map[string]string),
	}
	fd.fs.codec.EncodePutInput(fd.attr, req)

	val, err := fd.fs.signer.PresignPutObject(ctx, req, s3.WithPresignExpires(fd.fs.ttlSignedUrl))
	if err != nil {
		return "", &fs.PathError{
			Op:   "presign",
			Path: fd.path,
			Err:  err,
		}
	}

	return val.URL, nil
}

func (fd *writer[T]) Write(p []byte) (int, error) {
	if fd.r == nil && fd.w == nil {
		fd.lazyOpen()
	}

	if fd.err != nil {
		return 0, fd.err
	}

	// Note: IO fails if pipe is closed.
	n, err := fd.w.Write(p)
	if fd.err != nil {
		return 0, fd.err
	}

	return n, err
}

func (fd *writer[T]) Close() error {
	if fd.err != nil {
		return fd.err
	}

	if fd.w != nil && fd.r != nil {
		err := fd.w.Close()
		fd.wg.Wait()

		if fd.err != nil {
			return fd.err
		}

		return err
	}

	return nil
}

func (fd *writer[T]) Stat() (fs.FileInfo, error) {
	if fd.fs.signer != nil && fd.fs.codec.s != nil {
		if fd.info.attr == nil {
			fd.info.attr = new(T)
		}

		if url, err := fd.preSignPutUrl(); err == nil {
			fd.fs.codec.s.Put(fd.info.attr, url)
		}
	}

	return fd.info, nil
}

// Cancel effect of file i/o
func (fd *writer[T]) Cancel() error {
	_, err := fd.fs.api.AbortMultipartUpload(context.Background(),
		&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(fd.fs.bucket),
			Key:      fd.s3Key(),
			UploadId: aws.String(fd.upload),
		},
	)
	if err != nil {
		return err
	}

	return nil
}
