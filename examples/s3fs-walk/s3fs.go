//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package main

import (
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/fogfish/stream"
)

func main() {
	if err := walk(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}
}

func walk() error {
	// mount s3 bucket as file system
	s3fs, err := stream.NewFS(os.Args[1])
	if err != nil {
		return err
	}

	return fs.WalkDir(s3fs, "/the/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		fmt.Printf("--> %v | %v\n", path, d.Name())

		if d.IsDir() {
			os.Stdout.WriteString(fmt.Sprintf("==> %s\n", path))
			return nil
		}

		if err := read(s3fs, path); err != nil {
			return err
		}

		return nil
	})
}

func read(s3fs *stream.FileSystem[struct{}], path string) error {
	fd, err := s3fs.Open(path)
	if err != nil {
		return err
	}
	defer fd.Close()

	buf, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	fi, err := fd.Stat()
	if err != nil {
		return err
	}

	output(fi, buf)

	return nil
}

func output(fi fs.FileInfo, buf []byte) {
	os.Stdout.WriteString("---\n")
	os.Stdout.WriteString(fmt.Sprintf("Name:\t%s\n", fi.Name()))
	os.Stdout.WriteString(fmt.Sprintf("Size:\t%d\n", fi.Size()))
	os.Stdout.WriteString(fmt.Sprintf("Mode:\t%v\n", fi.Mode()))
	os.Stdout.WriteString(fmt.Sprintf("Time:\t%v\n", fi.ModTime()))
	os.Stdout.WriteString(string(buf))
	os.Stdout.WriteString("\n")
}
