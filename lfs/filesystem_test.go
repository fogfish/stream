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
	"time"

	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream/lfs"
)

var (
	file    = "/the/example/key"
	dir     = "/the/example/dir/"
	content = "Hello World!"
	size    = int64(len(content))
)

func TestNew(t *testing.T) {
	s3fs, err := lfs.NewTempFS("", "lfs")
	it.Then(t).Should(it.Nil(err)).ShouldNot(it.Nil(s3fs))

	s3fs, err = lfs.New("/")
	it.Then(t).Should(it.Nil(err)).ShouldNot(it.Nil(s3fs))

	_, err = lfs.New("not.found")
	it.Then(t).ShouldNot(it.Nil(err))

	_, err = lfs.NewTempFS("not.found", "lfs")
	it.Then(t).ShouldNot(it.Nil(err))

}

func TestReadWrite(t *testing.T) {
	t.Run("File/Read", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

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
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, dir), 0755)),
		)

		fd, err := s3fs.Open(dir)
		it.Then(t).Should(
			it.Nil(err),
		).ShouldNot(
			it.Nil(fd),
		)
	})

	t.Run("File/Read/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
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
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		_, err = s3fs.Open("/not.found")
		it.Then(t).Should(
			it.True(errors.Is(err, fs.ErrNotExist)),
		)

	})

	t.Run("File/Write", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
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

	t.Run("File/Write/Error/Dir", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, "the"), 0555)),
		)

		_, err = s3fs.Create("/the/file", nil)
		it.Then(t).ShouldNot(it.Nil(err))
	})

	t.Run("File/Write/Error/File", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, "the/file"), 0755)),
		)

		_, err = s3fs.Create("/the/file", nil)
		it.Then(t).ShouldNot(it.Nil(err))
	})

	t.Run("File/Write/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Create("invalid..key/", nil)),
		)
	})

	t.Run("File/Write/Error/Directory", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Create(dir, nil)),
		)
	})
}

func TestWalk(t *testing.T) {

	t.Run("ReadDir", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, dir), 0755)),
		)
		it.Then(t).ShouldNot(
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "1"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "2"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "3"))),
		)

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
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.ReadDir("/not.found")),
		)
	})

	t.Run("ReadDir/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.ReadDir("invalid..key/")),
		)
	})

	t.Run("Glob", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, dir), 0755)),
		)
		it.Then(t).ShouldNot(
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "1"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "2"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "3"))),
		)

		seq, err := s3fs.Glob(dir)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("1", "2", "3"),
		)
	})

	t.Run("GlobWithPattern", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, dir), 0755)),
		)
		it.Then(t).ShouldNot(
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "1"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "2"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "3"))),
		)

		seq, err := s3fs.Glob(dir + "|2")
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("2"),
		)
	})

	t.Run("GlobWithPattern/Error", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Glob(dir + "|\\")),
		)
	})

	t.Run("WalkDir", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(os.MkdirAll(filepath.Join(s3fs.Root, dir), 0755)),
		)
		it.Then(t).ShouldNot(
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "1"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "2"))),
			it.Error(os.Create(filepath.Join(s3fs.Root, dir, "3"))),
		)

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
			it.Seq(seq).Equal(dir+"1", dir+"2", dir+"3"),
		)
	})
}

func TestRemove(t *testing.T) {
	t.Run("Remove", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		err = s3fs.Remove(file)
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Remove/Error", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Remove(file)
			}),
		)
	})

	t.Run("Remove/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Remove("invalid..key/")
			}),
		)
	})
}

func TestCopy(t *testing.T) {
	t.Run("Copy", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		err = s3fs.Copy(file, filepath.Join(s3fs.Root, "test/file"))
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
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Copy("invalid..key/", "/test/file")
			}),
		)
	})

	t.Run("Copy/Error/InvalidSchema", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Copy(file, "some/invalid/file")
			}),
		)
	})
}

func TestWait(t *testing.T) {

	t.Run("Wait", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Must(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		err = s3fs.Wait(file, 5*time.Second)
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Wait/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Wait("invalid..key/", 5*time.Second)
			}),
		)
	})

	t.Run("Wait/Error/Timeout", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Fail(func() error {
				return s3fs.Wait(file, 0*time.Second)
			}),
		)
	})
}

func TestStat(t *testing.T) {
	t.Run("Stat", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		fi, err := s3fs.Stat(file)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), filepath.Base(file)),
			it.Equal(fi.Size(), size),
			it.Equal(fi.IsDir(), false),
			it.Equal(fi.Mode(), 0644),
		)
	})

	t.Run("Stat/Error", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Stat(file)),
		)
	})

	t.Run("Stat/Error/InvalidPath", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		it.Then(t).Should(
			it.Error(s3fs.Stat("invalid..key/")),
		)
	})

	t.Run("Stat/Error/NotFound", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(it.Nil(err))

		_, err = s3fs.Stat(file)
		it.Then(t).Should(
			it.True(errors.Is(err, fs.ErrNotExist)),
		)
	})

	t.Run("File/Stat", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), filepath.Base(file)),
			it.Equal(fi.Size(), size),
			it.Equal(fi.IsDir(), false),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("File/Stat.Read", func(t *testing.T) {
		s3fs, err := lfs.NewTempFS("", "lfs")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(createFile(s3fs)),
		)

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		_, err = io.ReadAll(fd)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), filepath.Base(file)),
			it.Equal(fi.Size(), size),
			it.Equal(fi.IsDir(), false),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})
}

func createFile(fsys *lfs.FileSystem) error {
	fd, err := fsys.Create(file, nil)
	if err != nil {
		return err
	}

	_, err = fd.Write([]byte(content))
	if err != nil {
		return err
	}

	err = fd.Close()
	if err != nil {
		return err
	}

	return nil
}
