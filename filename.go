//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package stream

import (
	"io/fs"
)

// The file system requires absolute path starting from "/"
// The file should not end with "/"
func IsValidFile(path string) bool {
	return len(path) > 0 && path[0] == '/' && fs.ValidPath(path[1:])
}

// Validate file path
func RequireValidFile(ctx, path string) error {
	if IsValidFile(path) {
		return nil
	}

	return &fs.PathError{
		Op:   ctx,
		Path: path,
		Err:  fs.ErrInvalid,
	}
}

// The file system requires absolute path starting from "/"
func IsValidPath(path string) bool {
	if path == "/" {
		return true
	}

	if len(path) != 0 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}

	if len(path) == 0 || path[0] != '/' || !fs.ValidPath(path[1:]) {
		return false
	}

	return true
}

// Validate Path
func RequireValidPath(ctx, path string) error {
	if IsValidPath(path) {
		return nil
	}

	return &fs.PathError{
		Op:   ctx,
		Path: path,
		Err:  fs.ErrInvalid,
	}
}

// The file system emulates "dirs" as any valid path ending with "/"
func IsValidDir(path string) bool {
	if path == "/" {
		return true
	}

	if len(path) == 0 || path[0] != '/' || path[len(path)-1] != '/' {
		return false
	}

	return fs.ValidPath(path[1 : len(path)-1])
}

// Validate Dir
func RequireValidDir(ctx, path string) error {
	if IsValidDir(path) {
		return nil
	}

	return &fs.PathError{
		Op:   ctx,
		Path: path,
		Err:  fs.ErrInvalid,
	}
}
