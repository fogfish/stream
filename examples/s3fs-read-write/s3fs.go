//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package main

import (
	"io"
	"os"

	"github.com/fogfish/stream"
)

func main() {
	if err := write(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}

	if err := read(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}
}

func write() error {
	// mount s3 bucket as file system
	s3fs, err := stream.NewFS(os.Args[1])
	if err != nil {
		return err
	}

	// create new object on S3 and return stream `io.Writer`
	fd, err := s3fs.Create("/the/example/key.txt", nil)
	if err != nil {
		return err
	}

	// stream data using io.Writer interface
	_, err = io.WriteString(fd, "Hello World!\n")
	if err != nil {
		return err
	}

	// S3 would not "materialize" object until stream is closed
	err = fd.Close()
	if err != nil {
		return err
	}

	return nil
}

func read() error {
	// mount s3 bucket as file system
	s3fs, err := stream.NewFS(os.Args[1])
	if err != nil {
		return err
	}

	// open stream `io.Reader` to an object on S3
	fd, err := s3fs.Open("/the/example/key.txt")
	if err != nil {
		return err
	}
	defer fd.Close()

	// stream data using io.Reader interface
	buf, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	os.Stdout.Write(buf)

	return nil
}
