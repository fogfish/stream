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
	"os"

	"github.com/fogfish/stream"
)

// A Golang struct type serves as the metadata container, where each public
// field is transformed into name-value pairs before being written to S3.
type Note struct {
	Author          string
	Chapter         string
	ContentType     string
	ContentLanguage string
}

func main() {
	if err := write(); err != nil {
		os.Stderr.WriteString(err.Error())
		return
	}

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

func write() error {
	// mount s3 bucket as file system using type of metadata container as constraints.
	s3fs, err := stream.New[Note](os.Args[1])
	if err != nil {
		return err
	}

	// define object metadata
	note := &Note{
		Author:          "fogfish",
		Chapter:         "Streaming",
		ContentType:     "text/plain",
		ContentLanguage: "en",
	}

	// create new object on S3 and return stream `io.Writer`
	fd, err := s3fs.Create("/the/example/key.txt", note)
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

func stat() error {
	// mount s3 bucket as file system
	s3fs, err := stream.New[Note](os.Args[1])
	if err != nil {
		return err
	}

	// read metadata
	fi, err := s3fs.Stat("/the/example/key.txt")
	if err != nil {
		return err
	}

	if note := s3fs.StatSys(fi); note != nil {
		os.Stdout.WriteString(fmt.Sprintf("Metadata:\t%+v\n", note))
	}

	return nil
}

func read() error {
	// mount s3 bucket as file system using type of metadata container as constraints.
	s3fs, err := stream.New[Note](os.Args[1])
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

	if note := s3fs.StatSys(fi); note != nil {
		os.Stdout.WriteString(fmt.Sprintf("Metadata:\t%+v\n", note))
	}

	// stream data using io.Reader interface
	buf, err := io.ReadAll(fd)
	if err != nil {
		return err
	}

	os.Stdout.Write(buf)

	return nil
}
