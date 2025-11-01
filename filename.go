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

// The file system accepts paths with or without leading "/"
// Both are treated as paths relative to the mount point.
// The file should not end with "/"
func IsValidFile(path string) bool {
	if len(path) == 0 {
		return false
	}

	// Files must not end with /
	if path[len(path)-1] == '/' {
		return false
	}

	// Accept paths with leading slash - strip it for validation
	if path[0] == '/' {
		return fs.ValidPath(path[1:])
	}

	// Accept paths without leading slash
	return fs.ValidPath(path)
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

// The file system accepts paths with or without leading "/"
// Both are treated as paths relative to the mount point.
func IsValidPath(path string) bool {
	if path == "/" {
		return true
	}

	// Handle trailing slash
	p := path
	if len(p) != 0 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}

	if len(p) == 0 {
		return false
	}

	// Accept paths with leading slash - strip it for validation
	if p[0] == '/' {
		return fs.ValidPath(p[1:])
	}

	// Accept paths without leading slash
	return fs.ValidPath(p)
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
// Accepts paths with or without leading "/"
func IsValidDir(path string) bool {
	if path == "/" {
		return true
	}

	if len(path) == 0 || path[len(path)-1] != '/' {
		return false
	}

	// Strip trailing slash
	p := path[:len(path)-1]

	// Accept paths with leading slash - strip it for validation
	if len(p) > 0 && p[0] == '/' {
		return fs.ValidPath(p[1:])
	}

	// Accept paths without leading slash
	return fs.ValidPath(p)
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
