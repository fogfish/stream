//
// Copyright (C) 2020 - 2025 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package spool

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"

	"github.com/fogfish/opts"
	"github.com/fogfish/stream"
)

// A FileSystem provides access to a hierarchical file system.
// The abstraction support I/O to local file system or AWS S3.
// Use it with https://github.com/fogfish/stream
type FileSystem interface {
	fs.FS
	Create(path string, attr *struct{}) (File, error)
	Remove(path string) error
}

// File provides I/O access to individual object on the file system.
type File = interface {
	io.Writer
	io.Closer
	Stat() (fs.FileInfo, error)
	Cancel() error
}

const (
	immutable = iota
	mutable
)

const (
	strict = iota
	skiperror
)

var (
	withMutability = opts.ForName[Spool, int]("mutable")

	// spool is immutable, consumed tasks remains in spool.
	IsImmutable = withMutability(immutable)
	// spool is mutable, consumed tasks are removed
	IsMutable = withMutability(mutable)

	withStrict = opts.ForName[Spool, int]("strict")

	// spool fails on error
	WithStrict = withStrict(strict)
	// spool skips error, continue as warning
	WithSkipError = withStrict(skiperror)
)

// Spool handler function
type Spoolf = func(context.Context, string, io.Reader) (io.ReadCloser, error)

type Spool struct {
	reader  FileSystem
	writer  FileSystem
	mutable int
	strict  int
}

func New(reader, writer FileSystem, opt ...opts.Option[Spool]) *Spool {
	s := &Spool{
		reader: reader,
		writer: writer,
	}

	opts.Apply(s, opt)

	return s
}

// apply spool function over the file
func (spool *Spool) apply(ctx context.Context, path string, f Spoolf) error {
	fd, err := spool.reader.Open(path)
	if err != nil {
		return spool.iserr(err)
	}
	defer fd.Close()

	dd, err := f(ctx, path, fd)
	if err != nil {
		return spool.iserr(err)
	}
	if dd == nil {
		return nil
	}
	defer dd.Close()

	err = spool.write(spool.writer, path, dd)
	if err != nil {
		return spool.iserr(err)
	}

	if spool.mutable == mutable {
		err = spool.reader.Remove(path)
		if err != nil {
			return spool.iserr(err)
		}
	}

	return nil
}

func (spool *Spool) iserr(err error) error {
	if spool.strict == skiperror {
		slog.Warn(err.Error())
		return nil
	}

	return err
}

// Write new file to spool
func (spool *Spool) Write(path string, r io.Reader) error {
	return spool.write(spool.reader, path, r)
}

func (spool *Spool) WriteFile(path string, b []byte) error {
	return spool.Write(path, bytes.NewBuffer(b))
}

// Apply the spool function over each file in the reader filesystem, producing
// results to writer file system.
func (spool *Spool) ForEach(ctx context.Context, dir string, f Spoolf) error {
	return fs.WalkDir(spool.reader, dir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			if err := spool.apply(ctx, path, f); err != nil {
				return err
			}

			return nil
		},
	)
}

// Apply the spool function over all file in the reader filesystem, producing
// results to writer file system.
func (spool *Spool) ForEachPath(ctx context.Context, paths []string, f Spoolf) error {
	for _, path := range paths {
		if err := spool.apply(ctx, path, f); err != nil {
			return err
		}
	}

	return nil
}

// Apply the spool function over each file in the reader filesystem, producing
// results to writer file system. It is a variant of [ForEach] that used bytes slices.
func (spool *Spool) ForEachFile(
	ctx context.Context,
	dir string,
	f func(context.Context, string, []byte) ([]byte, error),
) error {
	return spool.ForEach(ctx, dir,
		func(ctx context.Context, path string, r io.Reader) (io.ReadCloser, error) {
			in, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}

			b, err := f(ctx, path, in)
			if err != nil {
				return nil, err
			}
			if len(b) == 0 {
				return nil, nil
			}

			return io.NopCloser(bytes.NewBuffer(b)), nil
		},
	)
}

// Apply the parition function over each file in the reader filesystem, producing
// results to writer file system.
func (spool *Spool) Partition(
	ctx context.Context,
	dir string,
	f func(context.Context, string, io.Reader) (string, error),
) error {
	return fs.WalkDir(spool.reader, dir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			fd, err := spool.reader.Open(path)
			if err != nil {
				return spool.iserr(err)
			}
			defer fd.Close()

			shard, err := f(ctx, path, fd)
			if err != nil {
				return spool.iserr(err)
			}
			if len(shard) == 0 {
				return nil
			}

			cp, err := spool.reader.Open(path)
			if err != nil {
				return spool.iserr(err)
			}
			defer cp.Close()

			err = spool.write(spool.writer, filepath.Join("/", shard, path), cp)
			if err != nil {
				return spool.iserr(err)
			}

			if spool.mutable == mutable {
				err = spool.reader.Remove(path)
				if err != nil {
					return spool.iserr(err)
				}
			}

			return nil
		},
	)
}

func (spool *Spool) PartitionFile(
	ctx context.Context,
	dir string,
	f func(context.Context, string, []byte) (string, error),
) error {
	return spool.Partition(ctx, dir,
		func(ctx context.Context, path string, r io.Reader) (string, error) {
			in, err := io.ReadAll(r)
			if err != nil {
				return "", err
			}

			shard, err := f(ctx, path, in)
			if err != nil {
				return "", err
			}

			return shard, nil
		},
	)
}

func (spool *Spool) write(fs stream.CreateFS[struct{}], path string, r io.Reader) error {
	fd, err := fs.Create(path, nil)
	if err != nil {
		return err
	}

	_, err = io.Copy(fd, r)
	if err != nil {
		fd.Cancel()
		return err
	}

	err = fd.Close()
	if err != nil {
		fd.Cancel()
		return err
	}

	return nil
}
