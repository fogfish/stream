package main

import (
	"fmt"
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
	s3fs, err := stream.New[stream.PreSignedUrl](os.Args[1])
	if err != nil {
		return err
	}

	fd, err := s3fs.Create("/the/example/key.txt", nil)
	if err != nil {
		return err
	}
	defer fd.Close()

	// read metadata
	fi, err := fd.Stat()
	if err != nil {
		return err
	}

	if meta := s3fs.StatSys(fi); meta != nil {
		os.Stdout.WriteString(fmt.Sprintf("Write Url:\t%s\n", meta.PreSignedUrl))
	}

	return nil
}

func read() error {
	// mount s3 bucket as file system
	s3fs, err := stream.New[stream.PreSignedUrl](os.Args[1])
	if err != nil {
		return err
	}

	// read metadata
	fi, err := s3fs.Stat("/the/example/key.txt")
	if err != nil {
		return err
	}

	if meta := s3fs.StatSys(fi); meta != nil {
		os.Stdout.WriteString(fmt.Sprintf("Read Url:\t%s\n", meta.PreSignedUrl))
	}

	return nil
}
