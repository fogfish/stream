<p align="center">
  <img src="./doc/stream.png" height="240" />
  <h3 align="center">stream</h3>
  <p align="center"><strong>Golang file system abstraction tailored for AWS S3</strong></p>

  <p align="center">
    <!-- Version -->
    <a href="https://github.com/fogfish/stream/releases">
      <img src="https://img.shields.io/github/v/tag/fogfish/stream?label=version" />
    </a>
    <!-- Documentation -->
    <a href="https://pkg.go.dev/github.com/fogfish/stream">
      <img src="https://pkg.go.dev/badge/github.com/fogfish/stream" />
    </a>
    <!-- Build Status  -->
    <a href="https://github.com/fogfish/stream/actions/">
      <img src="https://github.com/fogfish/stream/workflows/build/badge.svg" />
    </a>
    <!-- GitHub -->
    <a href="http://github.com/fogfish/stream">
      <img src="https://img.shields.io/github/last-commit/fogfish/stream.svg" />
    </a>
    <!-- Coverage -->
    <a href="https://coveralls.io/github/fogfish/stream?branch=main">
      <img src="https://coveralls.io/repos/github/fogfish/stream/badge.svg?branch=main" />
    </a>
    <!-- Go Card -->
    <a href="https://goreportcard.com/report/github.com/fogfish/stream">
      <img src="https://goreportcard.com/badge/github.com/fogfish/stream" />
    </a>
  </p>
</p>

--- 

The library provides a Golang file system abstraction tailored for AWS S3, enabling seamless streaming of binary objects along with their corresponding metadata.

## Inspiration

The streaming is convenient paradigm for handling large binary objects like images, videos, and more. Applications can effectively manage data consumption by leveraging `io.Reader` and `io.Writer` for seamless abstraction. This library employs the [AWS Golang SDK v2](https://github.com/aws/aws-sdk-go-v2) under the hood to facilitate access to AWS S3 through streams.

On the other hand, a file system is a method used by computers to organize and store data on storage devices. It provides a structured way to access and manage binary objects. File systems handle tasks such as creating, reading, writing, and deleting binary objects, as well as managing permissions and metadata associated with each file or directory.

The library implements [Golang File System](https://pkg.go.dev/io/fs) and enhances it by adding support for writable files and type-safe metadata. The file system api is following:

```go
type FileSystem interface {
  Create(path string) (File, error)
  Open(path string) (fs.File, error)
  Stat(path string) (fs.FileInfo, error)
  ReadDir(path string) ([]fs.DirEntry, error)
  Glob(pattern string) ([]string, error)
}
```

Notably, the interface supports reading and writing [metadata associated with AWS objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html) using `fs.FileInfo`. 

The filesystem abstraction operates based on two fundamental principles:
1. Files are always accessed using an absolute path starting with `/`. This absolute path is interpreted within the context of the "mounted" file system.
2. Directories always end with a `/` to distinguish them from files.  


## Getting started

The library requires Go **1.18** or later due to usage of [generics](https://go.dev/blog/intro-generics).

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the `main` branch using forking and pull requests as described in contribution guidelines. The stable version is available via Golang modules.

Use `go get` to retrieve the library and add it as dependency to your application.

```bash
go get -u github.com/fogfish/stream
```

- [Inspiration](#inspiration)
- [Getting started](#getting-started)
  - [Quick Start](#quick-start)
  - [Mounting S3](#mounting-s3)
  - [Reading objects](#reading-objects)
  - [Writing objects](#writing-objects)
  - [Walking through objects](#walking-through-objects)
  - [Supported File System Operations](#supported-file-system-operations)
  - [Objects metadata](#objects-metadata)
  - [Type-safe objects metadata](#type-safe-objects-metadata)
  - [Presigned Urls](#presigned-urls)
  - [Error handling](#error-handling)
  - [Local file system](#local-file-system)
  - [Spool - File System Queue](#spool---file-system-queue)
- [How To Contribute](#how-to-contribute)
  - [commit message](#commit-message)
  - [bugs](#bugs)
- [License](#license)


### Quick Start

Check out the [examples](./examples/). They cover all fundamental use cases with runnable code snippets. Below is a simplest "Hello World"-like application for reading the object from AWS S3.  

```go
import (
  "io"
  "os"

  "github.com/fogfish/stream"
)

// mount s3 bucket as file system
s3fs, err := stream.NewFS(/* name of S3 bucket */)
if err != nil {
  return err
}

// open stream `io.Reader` to an object on S3
fd, err := s3fs.Open("/the/example/key.txt")
if err != nil {
  return err
}

// stream data using io.Reader interface
buf, err := io.ReadAll(fd)
if err != nil {
  return err
}

// close stream
err = fd.Close()
if err != nil {
  return err
}
```

[See and try examples](./examples/). Its cover all basic use-cases of the library.


### Mounting S3

The library serves as a user-side implementation of Golang's file system abstractions defined by [io/fs](https://pkg.go.dev/io/fs). It implements `fs.FS`, `fs.StatFS`, `fs.ReadDirFS` and `fs.GlobFS`. Additionally, it offers [extensions](./types.go) for file writing: `stream.CreateFS`, `stream.RemoveFS` and `stream.CopyFS`. 

To create a file system instance, utilize `stream.NewFS` or `stream.New`. The file system is configurable using [options pattern](/options.go).

```go
s3fs, err := stream.NewFS(
  /* name of S3 bucket */,
  stream.WithIOTimeout(5 * time.Second),
  stream.WithListingLimit(25),
)
```


### Reading objects

To open the file for reading use `Open` function giving the absolute path starting with `/`, the returned file descriptor is a composite of `io.Reader`, `io.Closer` and `stream.Stat`. Utilize Golang's convenient streaming methods to consume S3 object seamlessly.

```go
r, err := s3fs.Open("/the/example/key")
if err != nil {
  return err
}
defer r.Close() 

// utilize Golang's convenient streaming methods
io.ReadAll(r)
```


### Writing objects

To open the file for writing use `Create` function giving the absolute path starting with `/`, the returned file descriptor is a composite of `io.Writer`, `io.Closer` and `stream.Stat`. Utilize Golang's convenient streaming methods to update S3 object seamlessly. Once all bytes are written, it's crucial to close the stream. Failure to do so would cause data loss. The object is considered successfully created on S3 only if all `Write` operations and subsequent `Close` actions are successful.

```go
w, err := s3fs.Create("/the/example/key", nil)
if err != nil {
  return err
}

// utilize Golang's convenient streaming methods
io.WriteString(w, "Hello World!\n")

// close stream and handle error to prevent data loss. 
err = w.Close()
if err != nil {
  return err
}
```


### Walking through objects

The file system implements interfaces `fs.ReadDirFS` and `fs.GlobFS` for traversal through objects. The classical file system organize data hierarchically into directories as opposed to the flat storage structure of general purpose AWS S3 ([the directory bucket is not supported yet](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html)). The flat structure implies a limitations into the implementation
1. it assumes a directory if the path ends with `/` (e.g. `/the/example/key` points to the object, `/the/example/key/` points to the directory).
2. it return path relative to pattern for all found object.


```go
err := fs.WalkDir(s3fs, dir, func(path string, d fs.DirEntry, err error) error {
  if err != nil {
    return err
  }

  if d.IsDir() {
    return nil
  }

  // do something with file
  // path is absolute path to the file but entry is relative path
  // path == dir + d.Name()

  return nil
})
```


### Supported File System Operations 

For added convenience, the file system is enhanced with `stream.RemoveFS` and `stream.CopyFS`, enabling the removal of S3 objects and the copying of objects across buckets, respectively.


### Objects metadata

`fs.FileInfo` is a primary container for S3 objects metadata. The file system provides access to metadata either from open streams (file descriptors) or for any key.

```go
fi, err := s3fs.Stat("/the/example/key")
if err != nil {
  return err
}
```

### Type-safe objects metadata

AWS S3 support [object metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html) as a set of name-value pairs and allows to define the metadata at the time you upload the object and read it late. This library support both system and user-controlled metadata attributes.

What sets this library apart is its encouragement for developers to utilize the Golang type system in defining object metadata. Rather than working with loosely typed name-value pairs, metadata is structured as Golang structs, promoting correctness and maintainability. This approach is facilitated through generic programming style within the library. 

A Golang struct type serves as the metadata container, where each public field is transformed into name-value pairs before being written to S3. Example below defines the container build with two user controlled attributes `Author` and `Chapter` and two system attributes `ContentType` and `ContentLanguage`.

```go
type Note struct {
  Author          string
  Chapter         string
  ContentType     string
  ContentLanguage string
}
```

The file system interface has been expanded to handle user-defined metadata in a type-safe manner. Firstly, `stream.New()` create type annotated client. Secondly, the `Create()` function accepts a pointer to the metadata container, which is then written alongside the data. Lastly, the `fs.FileInfo` container retains an instance of associated metadata, which is accessible through either a `Sys()` call or the `StatSys()` helper.

```go
// create client and define metadata type
s3fs, err := stream.New[Note](/* name of S3 bucket */)

// create stream and annotate it with metadata
fd, err := s3fs.Create("/the/example/key",
  &Note{/* defined metadata values */},
)

// fs.FileInfo carries previously written metadata, use Sys() function to access.
fi, err := s3fs.Stat("/the/example/key")

note := s3fs.StatSys(fi)
```

AWS S3 defined collection of well-known system attributes. This library supports only subset of those: `Cache-Control`, `Content-Encoding`, `Content-Language`, `Content-Type`, `Expires`, `ETag`, `Last-Modified` and `Storage-Class`. Open Pull Request or raise an issue if subset needs to be enhanced.  

The library define type `stream.SystemMetadata` that incorporates all supported attributes. You might annotate your own types.

```go
type Note struct {
  stream.SystemMetadata
  Author          string
  Chapter         string
}
```


### Presigned Urls

Usage of `io.Reader` and `io.Writer` interfaces is sufficient for majority cloud applications. However, there are instances where delegating read/write responsibilities to a mobile client becomes necessary. For example, directly uploading images or video files from a mobile client to an S3 bucket is both scalable and considerably more efficient than routing through a backend system. The library accommodates this scenario with a special case for streaming binary objects using pre-signed URLs. The file system return pre-signed URL for the stream within the metadata. It only requires definition of attribute `PreSignedUrl` of `string` type.

```go
type PreSignedUrl struct {
	PreSignedUrl string
}
```

Use `fs.FileInfo` container and metadata api depicted above to obtain pre-signed URLs.

```go
// Mount the S3 bucket with metadata containing the `PreSignedUrl` attribute
s3fs, err := stream.New[stream.PreSignedUrl](/* name of S3 bucket */)

// Open file for read or write
fd, err := s3fs.Create("/the/example/key", nil)
if err != nil {
  return err
}
defer fd.Close()

// read files metadata
fi, err := fd.Stat()
if err != nil {
return err
}

if meta := s3fs.StatSys(fi); meta != nil {
  // Use meta.PreSignedUrl
}
```

Note: Utilizing a pre-signed URL necessitates passing all headers that were provided to the Create function.

```go
fd, err := s3fs.Create("/the/example/key",
  &Note{
    Author:          "fogfish",
    ContentType:     "text/plain",
    ContentLanguage: "en",
  }
)
```

```bash
curl -XPUT https://pre-signed-url-goes-here \
  -H 'Content-Type: text/plain' \
  -H 'Content-Language: en' \
  -H 'X-Amz-Meta-Author: fogfish' \
  -d 'some content'
```


### Error handling

The library consistently returns `fs.PathError`, except in cases where the object is not found, in which `fs.ErrNotExist` is returned. Additionally, it refrains from wrapping stream I/O errors.


### Local file system

The library implements compatible wrapper of `os.DirFS` to enhance functionality and provide a more user-friendly interface for filesystem operations, allowing clients to seamlessly mount both S3 and local file systems.

```go
import "github.com/fogfish/stream/lfs"

fs, err := lfs.New("/path/to/root")
```

### Spool - File System Queue

The library implements a convinient utility to implement Linux-like `spool` over the mounted file systems. The spooling is transparently done either over local file system or AWS S3. 

```go

```

## How To Contribute

The library is [MIT](LICENSE) licensed and accepts contributions via GitHub pull requests:

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request


The build and testing process requires [Go](https://golang.org) version 1.13 or later.

**build** and **test** library.

```bash
git clone https://github.com/fogfish/stream
cd stream
go test
```

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

### bugs

If you experience any issues with the library, please let us know via [GitHub issues](https://github.com/fogfish/dynamo/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 


## License

[![See LICENSE](https://img.shields.io/github/license/fogfish/stream.svg?style=for-the-badge)](LICENSE)
