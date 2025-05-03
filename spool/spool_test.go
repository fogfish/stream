//
// Copyright (C) 2020 - 2025 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package spool_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream/lfs"
	"github.com/fogfish/stream/spool"
)

func TestSpoolForEach(t *testing.T) {
	in, err := lfs.NewTempFS(os.TempDir(), "in")
	it.Then(t).Must(it.Nil(err))

	to, err := lfs.NewTempFS(os.TempDir(), "to")
	it.Then(t).Must(it.Nil(err))

	qq := spool.New(in, to)

	seq := []string{"/a", "/b", "/c", "/d", "/e", "/f"}
	for _, txt := range seq {
		err := qq.WriteFile(txt, []byte(txt))
		it.Then(t).Must(it.Nil(err))
	}

	dat := []string{}
	qq.ForEach(context.Background(), "/",
		func(ctx context.Context, path string, r io.Reader, w io.Writer) error {
			dat = append(dat, path)
			_, err := io.Copy(w, r)
			return err
		},
	)

	it.Then(t).Should(
		it.Seq(seq).Equal(dat...),
	)
}

func TestSpoolForEachPath(t *testing.T) {
	in, err := lfs.NewTempFS(os.TempDir(), "in")
	it.Then(t).Must(it.Nil(err))

	to, err := lfs.NewTempFS(os.TempDir(), "to")
	it.Then(t).Must(it.Nil(err))

	qq := spool.New(in, to)

	seq := []string{"/a", "/b", "/c", "/d", "/e", "/f"}
	for _, txt := range seq {
		err := qq.WriteFile(txt, []byte(txt))
		it.Then(t).Must(it.Nil(err))
	}

	dat := []string{}
	qq.ForEachPath(context.Background(), seq,
		func(ctx context.Context, path string, r io.Reader, w io.Writer) error {
			dat = append(dat, path)
			_, err := io.Copy(w, r)
			return err
		},
	)

	it.Then(t).Should(
		it.Seq(seq).Equal(dat...),
	)
}

func TestSpoolPartition(t *testing.T) {
	in, err := lfs.NewTempFS(os.TempDir(), "in")
	it.Then(t).Must(it.Nil(err))

	to, err := lfs.NewTempFS(os.TempDir(), "to")
	it.Then(t).Must(it.Nil(err))

	qq := spool.New(in, to)

	seq := []string{"/a", "/b", "/c", "/d", "/e", "/f"}
	for _, txt := range seq {
		err := qq.WriteFile(txt, []byte(txt))
		it.Then(t).Must(it.Nil(err))
	}

	dat := []string{}
	qq.Partition(context.Background(), "/",
		func(ctx context.Context, path string, r io.Reader) (string, error) {
			dat = append(dat, path)
			return path, nil
		},
	)

	it.Then(t).Should(
		it.Seq(seq).Equal(dat...),
	)
}
