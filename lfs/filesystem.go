//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package lfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/fogfish/stream"
)

type FileSystem struct {
	fs   fs.StatFS
	Root string
}

var (
	_ fs.FS                     = (*FileSystem)(nil)
	_ fs.StatFS                 = (*FileSystem)(nil)
	_ fs.ReadDirFS              = (*FileSystem)(nil)
	_ fs.GlobFS                 = (*FileSystem)(nil)
	_ stream.CreateFS[struct{}] = (*FileSystem)(nil)
	_ stream.RemoveFS           = (*FileSystem)(nil)
	_ stream.CopyFS             = (*FileSystem)(nil)
)

// Create local file system instance, mounting dir.
// It uses os.DirFS under the hood, making it compatible with streams extensions
func New(root string) (*FileSystem, error) {
	_, err := os.Stat(root)
	if err != nil {
		return nil, err
	}

	f := os.DirFS(root)
	if root == "/" {
		root = ""
	}

	return &FileSystem{
		fs:   f.(fs.StatFS),
		Root: root,
	}, nil
}

// Create temp file system
func NewTempFS(root string, pattern string) (*FileSystem, error) {
	dir, err := os.MkdirTemp(root, pattern)
	if err != nil {
		return nil, err
	}

	return New(dir)
}

// To open the file for writing use `Create` function giving the absolute path
// starting with `/`, the returned file descriptor is a composite of
// `io.Writer`, `io.Closer` and `stream.Stat`.
func (fsys *FileSystem) Create(path string, attr *struct{}) (stream.File, error) {
	if err := stream.RequireValidFile("create", path); err != nil {
		return nil, err
	}

	file := filepath.Join(fsys.Root, path)
	return fsys.osCreate("create", file)
}

func (fsys *FileSystem) osCreate(ctx, path string) (stream.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, &fs.PathError{
			Op:   ctx,
			Path: path,
			Err:  err,
		}
	}

	fd, err := os.Create(path)
	if err != nil {
		return nil, &fs.PathError{
			Op:   ctx,
			Path: path,
			Err:  err,
		}
	}

	return nopCanceler{File: fd}, nil
}

type nopCanceler struct{ *os.File }

// Cancel effect of file i/o
func (f nopCanceler) Cancel() error { return f.Close() }

// To open the file for reading use `Open` function giving the absolute path
// starting with `/`, the returned file descriptor is a composite of
// `io.Reader`, `io.Closer` and `stream.Stat`.
func (fsys *FileSystem) Open(path string) (fs.File, error) {
	if err := stream.RequireValidPath("open", path); err != nil {
		return nil, err
	}
	return fsys.fs.Open(trim(path))
}

// Stat returns a FileInfo describing the file.
func (fsys *FileSystem) Stat(path string) (fs.FileInfo, error) {
	if err := stream.RequireValidPath("stat", path); err != nil {
		return nil, err
	}
	return fsys.fs.Stat(trim(path))
}

// Reads the named directory or path prefix.
//
// It assumes a directory if the path ends with `/`.
//
// It return path relative to pattern for all found object.
func (fsys *FileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	if err := stream.RequireValidDir("readdir", path); err != nil {
		return nil, err
	}

	if f, ok := fsys.fs.(fs.ReadDirFS); ok {
		return f.ReadDir(trim(path))
	}

	return nil, fmt.Errorf("invalid os.DirFS configuration")
}

// Glob returns the names of all files matching pattern.
//
// It assumes a directory if the path ends with `/`.
//
// It return path relative to pattern for all found object.
//
// The pattern consists of path prefix Golang regex. Its are split by `|`.
func (fsys *FileSystem) Glob(pattern string) ([]string, error) {
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
func (fsys *FileSystem) Remove(path string) error {
	if err := stream.RequireValidFile("remove", path); err != nil {
		return err
	}

	file := filepath.Join(fsys.Root, path)

	return os.Remove(file)
}

// Copy object from source location to the target.
func (fsys *FileSystem) Copy(source, target string) (err error) {
	if err := stream.RequireValidFile("copy", source); err != nil {
		return err
	}

	if err := stream.RequireValidFile("copy", target); err != nil {
		return err
	}

	r, err := fsys.Open(source)
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := fsys.osCreate("copy", target)
	if err != nil {
		return err
	}
	defer func() { err = w.Close() }()

	if _, err := io.Copy(w, r); err != nil {
		return &fs.PathError{Op: "copy", Path: target, Err: err}
	}

	return nil
}

// Wait for timeout until path exists
func (fsys *FileSystem) Wait(path string, timeout time.Duration) error {
	if err := stream.RequireValidFile("wait", path); err != nil {
		return err
	}

	t := time.Now()
	for {
		if time.Since(t) >= timeout {
			return &fs.PathError{
				Op:   "wait",
				Path: path,
				Err:  fmt.Errorf("timeout"),
			}
		}

		_, err := fsys.Stat(path)
		switch {
		case err == nil:
			return nil
		case !os.IsNotExist(err):
			return err
		}

		time.Sleep(2 * time.Second)
	}
}

func trim(path string) string {
	if path == "/" {
		return "."
	}
	return strings.Trim(path, "/")
}
