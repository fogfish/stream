//
// Copyright (C) 2026 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stdio

import (
	"io"
	"io/fs"
	"time"

	"github.com/fogfish/stream"
)

// FileSystem is a no-op implementation of the FileSystem interface for standard input/output.
type FileSystem struct {
	r reader
	w writer
}

type writer struct {
	io.Writer
}

func (w *writer) Stat() (fs.FileInfo, error) {
	return nil, nil
}

func (w *writer) Close() error {
	return nil
}

func (w *writer) Cancel() error {
	return nil
}

type reader struct {
	io.Reader
}

func (r *reader) Stat() (fs.FileInfo, error) {
	return nil, nil
}

func (r *reader) Close() error {
	return nil
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

// Create a new FileSystem instance with the given reader and writer.
func New(r io.Reader, w io.Writer) (*FileSystem, error) {
	fsys := &FileSystem{}

	if r != nil {
		fsys.r = reader{r}
	}

	if w != nil {
		fsys.w = writer{w}
	}

	return fsys, nil
}

func (fsys *FileSystem) Create(path string, attr *struct{}) (stream.File, error) {
	if fsys.w.Writer == nil {
		return nil, &fs.PathError{
			Op:   "Create",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	return &fsys.w, nil
}

func (fsys *FileSystem) Open(path string) (fs.File, error) {
	if fsys.r.Reader == nil {
		return nil, &fs.PathError{
			Op:   "Open",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	return &fsys.r, nil
}

func (fsys *FileSystem) Stat(path string) (fs.FileInfo, error) {
	return nil, nil
}

func (fsys *FileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	if fsys.r.Reader != nil {
		return []fs.DirEntry{direntry("stdin")}, nil
	}

	return nil, nil
}

func (fsys *FileSystem) Glob(pattern string) ([]string, error) {
	if fsys.r.Reader != nil {
		return []string{"stdin"}, nil
	}

	return nil, nil
}

func (fsys *FileSystem) Remove(path string) error {
	return nil
}

func (fsys *FileSystem) Copy(source, target string) (err error) {
	return nil
}

func (fsys *FileSystem) Wait(path string, timeout time.Duration) error {
	return nil
}

type direntry string

func (d direntry) Name() string {
	return string(d)
}

func (d direntry) IsDir() bool {
	return false
}

func (d direntry) Type() fs.FileMode {
	return 0
}

func (d direntry) Info() (fs.FileInfo, error) {
	return nil, nil
}

/*

type DirEntry interface {
	// Name returns the name of the file (or subdirectory) described by the entry.
	// This name is only the final element of the path (the base name), not the entire path.
	// For example, Name would return "hello.go" not "home/gopher/hello.go".
	Name() string

	// IsDir reports whether the entry describes a directory.
	IsDir() bool

	// Type returns the type bits for the entry.
	// The type bits are a subset of the usual FileMode bits, those returned by the FileMode.Type method.
	Type() FileMode

	// Info returns the FileInfo for the file or subdirectory described by the entry.
	// The returned FileInfo may be from the time of the original directory read
	// or from the time of the call to Info. If the file has been removed or renamed
	// since the directory read, Info may return an error satisfying errors.Is(err, ErrNotExist).
	// If the entry denotes a symbolic link, Info reports the information about the link itself,
	// not the link's target.
	Info() (FileInfo, error)
}

*/
