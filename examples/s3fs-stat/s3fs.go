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
	if err := stat(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}

	os.Stdout.WriteString("\n")

	if err := read(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}
}

func output(fi fs.FileInfo) {
	os.Stdout.WriteString(fmt.Sprintf("Name:\t%s\n", fi.Name()))
	os.Stdout.WriteString(fmt.Sprintf("Size:\t%d\n", fi.Size()))
	os.Stdout.WriteString(fmt.Sprintf("Mode:\t%v\n", fi.Mode()))
	os.Stdout.WriteString(fmt.Sprintf("Time:\t%v\n", fi.ModTime()))
}

func stat() error {
	// mount s3 bucket as file system
	s3fs, err := stream.NewFS(os.Args[1])
	if err != nil {
		return err
	}

	// read metadata
	fi, err := s3fs.Stat("/the/example/key.txt")
	if err != nil {
		return err
	}

	output(fi)

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

	// read files metadata
	fi, err := fd.Stat()
	if err != nil {
		return err
	}

	output(fi)

	// stream data using io.Reader interface
	buf, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	os.Stdout.Write(buf)

	return nil
}
