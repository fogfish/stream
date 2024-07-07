//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package lfs_test

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream/lfs"
)

var (
	file    = "/the/example/key"
	dir     = file + "/"
	content = "Hello World!"
)

func TestNew(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	it.Then(t).Should(it.Nil(err))

	s3fs, err := lfs.New(root)
	it.Then(t).Should(it.Nil(err)).ShouldNot(it.Nil(s3fs))
}

func TestReadWrite(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	it.Then(t).Should(it.Nil(err))

	t.Run("File/Write", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Create(file, nil)
		it.Then(t).Must(it.Nil(err))

		n, err := io.WriteString(fd, content)
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(n, len(content)),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("File/Read", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		buf, err := io.ReadAll(fd)
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(string(buf), content),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Dir/Read", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(dir)
		it.Then(t).Should(
			it.Nil(err),
		).ShouldNot(
			it.Nil(fd),
		)
	})

	t.Run("File/Read/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Open("invalid..key/")),
		)
	})

	// t.Run("File/Read/Error", func(t *testing.T) {
	// 	s3fs, err := lfs.New(root)
	// 	it.Then(t).Should(it.Nil(err))

	// 	fd, err := s3fs.Open(file)
	// 	it.Then(t).Must(it.Nil(err))

	// 	it.Then(t).Should(
	// 		it.Error(io.ReadAll(fd)),
	// 	)
	// })

	t.Run("File/Read/Error/NotFound", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		_, err = s3fs.Open("/not.found")
		it.Then(t).Should(
			it.True(errors.Is(err, fs.ErrNotExist)),
		)

	})

	// t.Run("File/Write/Error", func(t *testing.T) {
	// 	s3fs, err := lfs.New(root)
	// 	it.Then(t).Should(it.Nil(err))

	// 	fd, err := s3fs.Create(file, nil)
	// 	it.Then(t).Must(it.Nil(err))

	// 	it.Then(t).Should(
	// 		it.Error(io.WriteString(fd, content)),
	// 	)

	// 	it.Then(t).Should(
	// 		it.Fail(fd.Close),
	// 	)
	// })

	t.Run("File/Write/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Create("invalid..key/", nil)),
		)
	})

	t.Run("File/Write/Error/Directory", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Create(dir, nil)),
		)
	})
}

func TestWalk(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	it.Then(t).Should(it.Nil(err))
	it.Then(t).ShouldNot(
		it.Error(0, os.MkdirAll(filepath.Join(root, dir), 0755)),
		it.Error(os.Create(filepath.Join(root, dir, "1"))),
		it.Error(os.Create(filepath.Join(root, dir, "2"))),
		it.Error(os.Create(filepath.Join(root, dir, "3"))),
	)

	t.Run("ReadDir", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		seq, err := s3fs.ReadDir(dir)
		it.Then(t).Must(
			it.Nil(err),
			it.Equal(len(seq), 3),
		)
		it.Then(t).Should(
			it.Equal(seq[0].Name(), "1"),
			it.Equal(seq[1].Name(), "2"),
			it.Equal(seq[2].Name(), "3"),
		)
	})

	t.Run("ReadDir/Error", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.ReadDir("/not.found")),
		)
	})

	t.Run("ReadDir/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.ReadDir("invalid..key/")),
		)
	})

	t.Run("Glob", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		seq, err := s3fs.Glob(dir)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("1", "2", "3"),
		)
	})

	t.Run("GlobWithPattern", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		seq, err := s3fs.Glob(dir + "|2")
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("2"),
		)
	})

	t.Run("GlobWithPattern/Error", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Glob(dir + "|\\")),
		)
	})

	t.Run("WalkDir", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		seq := make([]string, 0)
		err = fs.WalkDir(s3fs, dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			it.Then(t).
				ShouldNot(
					it.Error(d.Info()),
				).
				Should(
					it.Nil(err),
					it.Equal(d.Type(), 0),
				)

			seq = append(seq, path)
			return nil
		})
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal(file+"/1", file+"/2", file+"/3"),
		)
	})
}

func TestRemove(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	it.Then(t).Should(it.Nil(err))

	t.Run("Remove", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		fd, err := s3fs.Create(file, nil)
		it.Then(t).Must(it.Nil(err))
		fd.Write([]byte(content))
		fd.Close()

		err = s3fs.Remove(file)
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Remove/Error", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Remove(file)
			}),
		)
	})

	t.Run("Remove/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Remove("invalid..key/")
			}),
		)
	})
}

func TestCopy(t *testing.T) {
	root, err := os.MkdirTemp("", "")
	it.Then(t).Should(it.Nil(err))

	t.Run("Copy", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Must(it.Nil(err))

		fd, err := s3fs.Create(file, nil)
		it.Then(t).Must(it.Nil(err))
		fd.Write([]byte(content))
		fd.Close()

		err = s3fs.Copy(file, filepath.Join(root, "test/file"))
		it.Then(t).Must(it.Nil(err))
	})

	// t.Run("Copy/Error", func(t *testing.T) {
	// 	s3fs, err := lfs.New(root)
	// 	it.Then(t).Should(it.Nil(err))

	// 	it.Then(t).Should(
	// 		it.Fail(func() error {
	// 			return s3fs.Copy(file, "s3://test/file")
	// 		}),
	// 	)
	// })

	t.Run("Copy/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Copy("invalid..key/", "/test/file")
			}),
		)
	})

	t.Run("Copy/Error/InvalidSchema", func(t *testing.T) {
		s3fs, err := lfs.New(root)
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Copy(file, file)
			}),
		)
	})
}
