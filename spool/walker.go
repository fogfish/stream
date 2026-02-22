//
// Copyright (C) 2020 - 2026 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package spool

import "io/fs"

// WalkDir returns a Walker that traverses the file system rooted at the given directory.
func WalkDir(dir string) Walker {
	return walkdir(dir)
}

type walkdir string

func (dir walkdir) Walk(fsys fs.FS, fn fs.WalkDirFunc) error {
	return fs.WalkDir(fsys, string(dir), fn)
}

// WalkFiles returns a Walker that traverses the given list of files. Each file is processed as if it were a separate root directory.
func WalkFiles(files []string) Walker {
	return walkfiles(files)
}

type walkfiles []string

func (files walkfiles) Walk(fsys fs.FS, fn fs.WalkDirFunc) error {
	for _, file := range files {
		info, err := fs.Stat(fsys, file)
		if err != nil {
			return err
		}

		err = fn(file, direntity{info}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type direntity struct {
	fs.FileInfo
}

func (d direntity) Type() fs.FileMode {
	return d.FileInfo.Mode().Type()
}

func (d direntity) Info() (fs.FileInfo, error) {
	return d.FileInfo, nil
}
