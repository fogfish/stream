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
	"os"
	"testing"

	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream/lfs"
	"github.com/fogfish/stream/spool"
)

func TestSpool(t *testing.T) {
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
	qq.ForEachFile(context.Background(), "/",
		func(ctx context.Context, path string, b []byte) ([]byte, error) {
			dat = append(dat, path)
			return b, nil
		},
	)

	it.Then(t).Should(
		it.Seq(seq).Equal(dat...),
	)
}
