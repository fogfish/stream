//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stream

import (
	"context"
	"errors"

	"io/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

//------------------------------------------------------------------------------

// directory descriptor
type dd[T any] struct {
	info[T]
	fs *FileSystem[T]
}

var (
	_ fs.ReadDirFile = (*dd[any])(nil)
)

// open directory descriptor
func openDir[T any](fsys *FileSystem[T], path string) (*dd[T], error) {
	return &dd[T]{
		info: info[T]{
			path: path,
			mode: fs.ModeDir,
		},
		fs: fsys,
	}, nil
}

func (dd *dd[T]) Stat() (fs.FileInfo, error) { return dd.info, nil }

func (dd *dd[T]) Read([]byte) (int, error) {
	return 0, &fs.PathError{
		Op:   "read",
		Path: dd.path,
		Err:  errors.New("is a directory"),
	}
}

func (dd *dd[T]) Close() error { return nil }

func (dd *dd[T]) ReadDir(n int) ([]fs.DirEntry, error) {
	return dd.readAll()
}

func (dd *dd[T]) readAll() ([]fs.DirEntry, error) {
	seq := make([]fs.DirEntry, 0)
	req := &s3.ListObjectsV2Input{
		Bucket:  aws.String(dd.fs.bucket),
		MaxKeys: aws.Int32(dd.fs.lslimit),
		Prefix:  dd.s3Key(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), dd.fs.timeout)
	defer cancel()

	for {
		val, err := dd.fs.api.ListObjectsV2(ctx, req)
		if err != nil {
			return nil, &fs.PathError{
				Op:   "readdir",
				Path: dd.path,
				Err:  err,
			}
		}

		for _, el := range val.Contents {
			seq = append(seq, dd.objectToDirEntry(el))
		}

		cnt := int(aws.ToInt32(val.KeyCount))
		if cnt == 0 || val.NextContinuationToken == nil {
			return seq, nil
		}

		req.StartAfter = val.Contents[cnt-1].Key
	}
}

func (dd *dd[T]) objectToDirEntry(t types.Object) fs.DirEntry {
	// Note: file system requires a strict hierarchical division on files and dirs.
	//       It is assumed by fs.FS implementations (e.g. WalkDir) and also requires
	//       Name to be basename. It is not convenient for S3 where file system is flat.
	path := aws.ToString(t.Key)
	path = path[len(dd.path)-1:]

	// ETag
	// ObjectStorageClass
	return info[T]{
		path: path,
		size: aws.ToInt64(t.Size),
		time: aws.ToTime(t.LastModified),
	}
}
