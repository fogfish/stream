//
// Copyright (C) 2026 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stdio

import (
	"io/fs"
	"os"
	"time"

	"github.com/fogfish/stream"
)

// FileSystem is a no-op implementation of the FileSystem interface for standard input/output.
type FileSystem struct {
	r reader
	w writer
}

type writer struct {
	*os.File
}

func (w *writer) Cancel() error {
	return nil
}

type reader struct {
	*os.File
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
func New(r *os.File, w *os.File) (*FileSystem, error) {
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
	if fsys.w.File == nil {
		return nil, &fs.PathError{
			Op:   "Create",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	return &fsys.w, nil
}

func (fsys *FileSystem) Open(path string) (fs.File, error) {
	if fsys.r.File == nil {
		return nil, &fs.PathError{
			Op:   "Open",
			Path: path,
			Err:  fs.ErrInvalid,
		}
	}

	return &fsys.r, nil
}

func (fsys *FileSystem) Stat(path string) (fs.FileInfo, error) {
	if fsys.r.File != nil {
		info, err := fsys.r.File.Stat()
		if err != nil {
			return nil, err
		}
		return info, nil
	}
	return nil, nil
}

func (fsys *FileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	if fsys.r.File != nil {
		info, err := fsys.r.File.Stat()
		if err != nil {
			return nil, err
		}
		return []fs.DirEntry{fs.FileInfoToDirEntry(info)}, nil
	}

	return nil, nil
}

func (fsys *FileSystem) Glob(pattern string) ([]string, error) {
	if fsys.r.File != nil {
		return []string{"STDIN"}, nil
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
