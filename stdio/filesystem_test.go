//
// Copyright (C) 2026 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stdio_test

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream/stdio"
)

var content = "Hello World!"

func TestNew(t *testing.T) {
	t.Run("Both", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, os.Stdout)
		it.Then(t).
			Should(it.Nil(err)).
			ShouldNot(it.Nil(fsys))
	})

	t.Run("ReaderOnly", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, nil)
		it.Then(t).
			Should(it.Nil(err)).
			ShouldNot(it.Nil(fsys))
	})

	t.Run("WriterOnly", func(t *testing.T) {
		fsys, err := stdio.New(nil, os.Stdout)
		it.Then(t).
			Should(it.Nil(err)).
			ShouldNot(it.Nil(fsys))
	})

	t.Run("Neither", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).
			Should(it.Nil(err)).
			ShouldNot(it.Nil(fsys))
	})
}

func TestCreate(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		r, w, err := os.Pipe()
		it.Then(t).Must(it.Nil(err))
		defer r.Close()

		fsys, err := stdio.New(nil, w)
		it.Then(t).Must(it.Nil(err))

		fd, err := fsys.Create("stdout", nil)
		it.Then(t).Must(it.Nil(err))

		n, err := io.WriteString(fd, content)
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(n, len(content)),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))

		buf, err := io.ReadAll(r)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(it.Equal(string(buf), content))
	})

	t.Run("Write/Stat", func(t *testing.T) {
		fsys, err := stdio.New(nil, os.Stdout)
		it.Then(t).Must(it.Nil(err))

		fd, err := fsys.Create("stdout", nil)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Should(it.Nil(err))
		it.Then(t).ShouldNot(it.Nil(fi))
	})

	t.Run("Write/Cancel", func(t *testing.T) {
		fsys, err := stdio.New(nil, os.Stdout)
		it.Then(t).Must(it.Nil(err))

		fd, err := fsys.Create("stdout", nil)
		it.Then(t).Must(it.Nil(err))

		err = fd.Cancel()
		it.Then(t).Should(it.Nil(err))
	})

	t.Run("Write/Error/NoWriter", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(fsys.Create("stdout", nil)),
		)
	})
}

func TestOpen(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		r, w, err := os.Pipe()
		it.Then(t).Must(it.Nil(err))
		_, err = io.WriteString(w, content)
		it.Then(t).Must(it.Nil(err))
		w.Close()

		fsys, err := stdio.New(r, nil)
		it.Then(t).Must(it.Nil(err))

		fd, err := fsys.Open("stdin")
		it.Then(t).Must(it.Nil(err))

		buf, err := io.ReadAll(fd)
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(string(buf), content),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Read/Stat", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, nil)
		it.Then(t).Must(it.Nil(err))

		fd, err := fsys.Open("stdin")
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Should(it.Nil(err))
		it.Then(t).ShouldNot(it.Nil(fi))
	})

	t.Run("Read/Error/NoReader", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(fsys.Open("stdin")),
		)
	})
}

func TestStat(t *testing.T) {
	t.Run("Stat", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		fi, err := fsys.Stat("any")
		it.Then(t).Should(
			it.Nil(err),
			it.Nil(fi),
		)
	})
}

func TestReadDir(t *testing.T) {
	t.Run("WithReader", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, nil)
		it.Then(t).Must(it.Nil(err))

		entries, err := fsys.ReadDir("/")
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(it.Equal(len(entries), 1))

		it.Then(t).Should(
			it.Equal(entries[0].IsDir(), false),
		)
	})

	t.Run("WithReader/DirEntryInfo", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, nil)
		it.Then(t).Must(it.Nil(err))

		entries, err := fsys.ReadDir("/")
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Must(it.Equal(len(entries), 1))

		fi, err := entries[0].Info()
		it.Then(t).Should(it.Nil(err))
		it.Then(t).ShouldNot(it.Nil(fi))
	})

	t.Run("NoReader", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		entries, err := fsys.ReadDir("/")
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(len(entries), 0),
		)
	})
}

func TestGlob(t *testing.T) {
	t.Run("Glob", func(t *testing.T) {
		fsys, err := stdio.New(os.Stdin, os.Stdout)
		it.Then(t).Must(it.Nil(err))

		matches, err := fsys.Glob("*")
		it.Then(t).Should(
			it.Nil(err),
			it.Equal(len(matches), 1),
		)
	})
}

func TestRemove(t *testing.T) {
	t.Run("Remove", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		err = fsys.Remove("any")
		it.Then(t).Should(it.Nil(err))
	})
}

func TestCopy(t *testing.T) {
	t.Run("Copy", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		err = fsys.Copy("source", "target")
		it.Then(t).Should(it.Nil(err))
	})
}

func TestWait(t *testing.T) {
	t.Run("Wait", func(t *testing.T) {
		fsys, err := stdio.New(nil, nil)
		it.Then(t).Must(it.Nil(err))

		err = fsys.Wait("any", 5*time.Second)
		it.Then(t).Should(it.Nil(err))
	})
}
