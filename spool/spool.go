//
// Copyright (C) 2020 - 2026 Dmitry Kolesnikov
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
	"regexp"
	"strings"

	"github.com/fogfish/opts"
	"github.com/fogfish/stream"
)

// A FileSystem abstraction provides access to a hierarchical file system or sequence of files
//
// Use https://github.com/fogfish/stream to implement FileSystem for local file system or AWS S3.
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

// Walker provides Walk method to traverse the files.
type Walker interface {
	Walk(fs.FS, fs.WalkDirFunc) error
}

// Spooler is a function applied to each file.
// It is given the file path, a reader for the input file and a writer for the output file.
type Spooler = func(context.Context, string, io.Reader, io.Writer) error

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

	// spool files matching the pattern
	WithPattern = opts.ForName[Spool, string]("pattern")

	// output files with new extension
	WithFileExt = opts.ForName[Spool, string]("ext")
)

type Spool struct {
	reader  FileSystem
	writer  FileSystem
	mutable int
	strict  int
	pattern string
	ext     string
}

func New(reader, writer FileSystem, opt ...opts.Option[Spool]) *Spool {
	s := &Spool{
		reader: reader,
		writer: writer,
	}

	opts.Apply(s, opt)

	return s
}

func (spool *Spool) iserr(err error) error {
	if spool.strict == skiperror {
		slog.Warn(err.Error())
		return nil
	}

	return err
}

// Write file to spool, consuming all data from the reader.
func (spool *Spool) Write(path string, r io.Reader) error {
	return spool.write(spool.reader, path, r)
}

// Write file to spool
func (spool *Spool) WriteFile(path string, b []byte) error {
	return spool.Write(path, bytes.NewBuffer(b))
}

// ForEach iterates over the files in the spool's reader using the provided walker.
// It applies the Spooler function f to each file that matches the spool's pattern (if set).
// Directories are skipped, and files not matching the pattern are ignored.
// The function returns an error if the regex compilation fails, walking encounters an error,
// or if applying the Spooler function fails for any file.
func (spool *Spool) ForEach(ctx context.Context, walker Walker, f Spooler) error {
	var re *regexp.Regexp
	if len(spool.pattern) > 0 {
		ex, err := regexp.Compile(spool.pattern)
		if err != nil {
			return err
		}
		re = ex
	}

	return walker.Walk(spool.reader,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			if re != nil && !re.MatchString(path) {
				return nil
			}

			if err := spool.apply(ctx, path, f); err != nil {
				return err
			}

			return nil
		},
	)
}

// apply spool function over the file
func (spool *Spool) apply(ctx context.Context, path string, f Spooler) (rerr error) {
	rfd, err := spool.reader.Open(path)
	if err != nil {
		return spool.iserr(err)
	}
	defer rfd.Close()

	target := path
	if len(spool.ext) > 0 {
		target = strings.TrimSuffix(target, filepath.Ext(target)) + spool.ext
	}

	wfd, err := spool.writer.Create(target, nil)
	if err != nil {
		return err
	}
	defer func() {
		err := wfd.Close()
		if err != nil {
			wfd.Cancel()
			rerr = err
		}
	}()

	err = f(ctx, path, rfd, wfd)
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

// Apply the shard function over each file in the reader filesystem.
// The shard function is given the file path and a reader for the input file,
// and returns the shard name for the output file. The output file is written
// to the writer filesystem with the same path, but prefixed with the shard name.
//
// If the shard function returns an empty string, the file is skipped.
// After processing, if the spool is mutable, the original file is removed
// from the reader filesystem.
func (spool *Spool) Shard(
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
