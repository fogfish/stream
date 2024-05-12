//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stream_test

import (
	"bytes"
	"io"
	"io/fs"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fogfish/it/v2"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/internal/mocks"
)

var (
	file         = "/the/example/key"
	dir          = file + "/"
	presignedUrl = "https://example.com" + file
	content      = "Hello World!"
	size         = int64(len(content))
	modified     = time.Date(2024, 05, 11, 18, 04, 30, 0, time.UTC)
	expires      = time.Date(2025, 05, 11, 18, 04, 30, 0, time.UTC)
	note         = Note{
		SystemMetadata: stream.SystemMetadata{
			CacheControl:    "no-cache",
			ContentEncoding: "identity",
			ContentLanguage: "en",
			ContentType:     "text/plain",
			Expires:         &expires,
			ETag:            "cafe",
			LastModified:    &modified,
			StorageClass:    "GLACIER",
		},
		Author:  "fogfish",
		Chapter: "streaming",
	}

	s3HeadObject = mocks.HeadObject{
		Mock: mocks.Mock[s3.HeadObjectOutput]{
			ExpectKey: file[1:],
			ReturnVal: &s3.HeadObjectOutput{
				ContentLength:   aws.Int64(size),
				ContentType:     aws.String("text/plain"),
				LastModified:    aws.Time(modified),
				CacheControl:    aws.String("no-cache"),
				ContentEncoding: aws.String("identity"),
				ContentLanguage: aws.String("en"),
				Expires:         aws.Time(expires),
				ETag:            aws.String("cafe"),
				StorageClass:    types.StorageClassGlacier,
				Metadata: map[string]string{
					"author":  "fogfish",
					"chapter": "streaming",
				},
			},
		},
	}

	s3GetObject = mocks.GetObject{
		Mock: mocks.Mock[s3.GetObjectOutput]{
			ExpectKey: file[1:],
			ReturnVal: &s3.GetObjectOutput{
				Body:            io.NopCloser(bytes.NewBuffer([]byte(content))),
				ContentLength:   aws.Int64(size),
				ContentType:     aws.String("text/plain"),
				LastModified:    aws.Time(modified),
				CacheControl:    aws.String("no-cache"),
				ContentEncoding: aws.String("identity"),
				ContentLanguage: aws.String("en"),
				Expires:         aws.Time(expires),
				ETag:            aws.String("cafe"),
				StorageClass:    types.StorageClassGlacier,
				Metadata: map[string]string{
					"author":  "fogfish",
					"chapter": "streaming",
				},
			},
		},
	}

	s3PutObject = mocks.PutObject{
		Mock: mocks.Mock[manager.UploadOutput]{
			ExpectKey: file[1:],
			ExpectVal: content,
		},
	}

	s3ListObject = mocks.ListObject{
		Mock: mocks.Mock[s3.ListObjectsV2Output]{
			ExpectKey: dir[1:],
			ReturnVal: &s3.ListObjectsV2Output{
				KeyCount: aws.Int32(3),
				Contents: []types.Object{
					{Key: aws.String(file[1:] + "/1"), Size: aws.Int64(100), LastModified: aws.Time(modified)},
					{Key: aws.String(file[1:] + "/2"), Size: aws.Int64(200), LastModified: aws.Time(modified)},
					{Key: aws.String(file[1:] + "/3"), Size: aws.Int64(300), LastModified: aws.Time(modified)},
				},
			},
		},
	}

	s3DeleteObject = mocks.DeleteObject{
		Mock: mocks.Mock[s3.DeleteObjectOutput]{
			ExpectKey: file[1:],
		},
	}

	s3CopyObject = mocks.CopyObject{
		Mock: mocks.Mock[s3.CopyObjectOutput]{
			ExpectKey: file[1:],
		},
	}

	s3PresignPutObject = mocks.PresignPutObject{
		Mock: mocks.Mock[v4.PresignedHTTPRequest]{
			ExpectKey: file[1:],
			ReturnVal: &v4.PresignedHTTPRequest{
				URL: presignedUrl,
			},
		},
	}

	s3PresignGetObject = mocks.PresignGetObject{
		Mock: mocks.Mock[v4.PresignedHTTPRequest]{
			ExpectKey: file[1:],
			ReturnVal: &v4.PresignedHTTPRequest{
				URL: presignedUrl,
			},
		},
	}
)

func TestNew(t *testing.T) {
	s3fs, err := stream.NewFS("test",
		stream.WithS3(s3GetObject),
		stream.WithS3Upload(s3PutObject),
		stream.WithS3Signer(s3PresignGetObject),
		stream.WithIOTimeout(5*time.Second),
		stream.WithPreSignUrlTTL(20*time.Second),
		stream.WithListingLimit(1000),
	)
	it.Then(t).Should(it.Nil(err)).ShouldNot(it.Nil(s3fs))
}

func TestReadWrite(t *testing.T) {
	t.Run("File/Read", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3GetObject),
		)
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
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(dir)
		it.Then(t).Must(it.Nil(err))

		it.Then(t).Should(
			it.Error(io.ReadAll(fd)),
		)
	})

	t.Run("File/Write", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3Upload(s3PutObject),
		)
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
}

func TestWalk(t *testing.T) {
	t.Run("ReadDir", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3ListObject),
		)
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

	t.Run("Glob", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3ListObject),
		)
		it.Then(t).Must(it.Nil(err))

		seq, err := s3fs.Glob(dir)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("1", "2", "3"),
		)
	})

	t.Run("GlobWithPattern", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3ListObject),
		)
		it.Then(t).Must(it.Nil(err))

		seq, err := s3fs.Glob(dir + "|2")
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Seq(seq).Equal("2"),
		)
	})

	t.Run("WalkDir", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3ListObject),
		)
		it.Then(t).Must(it.Nil(err))

		seq := make([]string, 0)
		err = fs.WalkDir(s3fs, dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

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
	s3fs, err := stream.NewFS("test",
		stream.WithS3(s3DeleteObject),
	)
	it.Then(t).Must(it.Nil(err))

	t.Run("Remove", func(t *testing.T) {
		err := s3fs.Remove(file)
		it.Then(t).Must(it.Nil(err))
	})
}

func TestCopy(t *testing.T) {
	t.Run("Copy", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3CopyObject),
		)
		it.Then(t).Must(it.Nil(err))

		err = s3fs.Copy(file, "s3://test/file")
		it.Then(t).Must(it.Nil(err))
	})
}

func TestWait(t *testing.T) {
	t.Run("Copy", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3HeadObject),
		)
		it.Then(t).Must(it.Nil(err))

		err = s3fs.Wait(file, 5*time.Second)
		it.Then(t).Must(it.Nil(err))
	})
}

func TestStat(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3HeadObject),
		)
		it.Then(t).Should(it.Nil(err))

		fi, err := s3fs.Stat(file)
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), file),
			it.Equal(fi.Size(), size),
			it.Equiv(fi.ModTime(), modified),
			it.Equal(fi.IsDir(), false),
		)
	})

	t.Run("File/Stat", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), file),
			it.Equal(fi.Size(), size),
			it.Equiv(fi.ModTime(), modified),
			it.Equal(fi.IsDir(), false),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("File/Stat.Read", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		_, err = io.ReadAll(fd)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), file),
			it.Equal(fi.Size(), size),
			it.Equiv(fi.ModTime(), modified),
			it.Equal(fi.IsDir(), false),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("Dir/Stat", func(t *testing.T) {
		s3fs, err := stream.NewFS("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(dir)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))
		it.Then(t).Should(
			it.Equal(fi.Name(), dir),
			it.Equal(fi.Size(), 0),
			it.Equiv(fi.ModTime(), time.Time{}),
			it.Equal(fi.IsDir(), true),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

}

type Note struct {
	stream.SystemMetadata
	Author  string
	Chapter string
}

func TestMetadata(t *testing.T) {
	t.Run("Stat", func(t *testing.T) {
		s3fs, err := stream.New[Note]("test",
			stream.WithS3(s3HeadObject),
		)
		it.Then(t).Should(it.Nil(err))

		fi, err := s3fs.Stat(file)
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equiv(meta, &note),
		)
	})

	t.Run("File/Stat", func(t *testing.T) {
		s3fs, err := stream.New[Note]("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equiv(meta, &note),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

	t.Run("File/Stat.Read", func(t *testing.T) {
		s3fs, err := stream.New[Note]("test",
			stream.WithS3(s3GetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		_, err = io.ReadAll(fd)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equiv(meta, &note),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})
}

func TestPreSign(t *testing.T) {
	t.Run("PreSignUrl", func(t *testing.T) {
		s3fs, err := stream.New[stream.PreSignedUrl]("test",
			stream.WithS3(s3HeadObject),
			stream.WithS3Signer(s3PresignGetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fi, err := s3fs.Stat(file)
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equal(meta.PreSignedUrl, presignedUrl),
		)
	})

	t.Run("File/Read/PreSignUrl", func(t *testing.T) {
		s3fs, err := stream.New[stream.PreSignedUrl]("test",
			stream.WithS3(s3GetObject),
			stream.WithS3Signer(s3PresignGetObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Open(file)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equal(meta.PreSignedUrl, presignedUrl),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))

	})

	t.Run("File/Write/PreSignUrl", func(t *testing.T) {
		s3fs, err := stream.New[stream.PreSignedUrl]("test",
			stream.WithS3Signer(s3PresignPutObject),
		)
		it.Then(t).Should(it.Nil(err))

		fd, err := s3fs.Create(file, nil)
		it.Then(t).Must(it.Nil(err))

		fi, err := fd.Stat()
		it.Then(t).Must(it.Nil(err))

		meta := s3fs.StatSys(fi)
		it.Then(t).Should(
			it.Equal(meta.PreSignedUrl, presignedUrl),
		)

		err = fd.Close()
		it.Then(t).Must(it.Nil(err))
	})

}
