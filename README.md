# stream

The library implements a simple streaming abstraction to store linked-data, binary objects and its associated metadata at AWS storage services. The library support AWS S3, AWS S3 PreSigned URLs.


## Inspiration

The library encourages developers to use Golang type system to define domain models, write correct, maintainable code. This paradigm is also extended on operating with binary objects (e.g. images, videos, etc). Using this library, the application can abstract binary objects using streams (`io.Reader`) for the content and Golang structs for objects metadata.  The library uses generic programming style to implement actual storage I/O, while expose metadata object as `[T stream.Thing]` with implicit conversion back and forth between a concrete struct(s). The library uses [AWS Golang SDK v2](https://github.com/aws/aws-sdk-go-v2) under the hood.

Essentially, the library implement a following generic key-value trait to access domain objects. 

```go
type Streamer[T stream.Thing] interface {
  Put(T, io.Reader) error
  Get(T) (T, io.Reader, error)
  Has(T) (T, error)
  Remove(T) error
  Match(T) Seq[T]
}
```

## Getting started

The library requires Go **1.18** or later due to usage of [generics](https://go.dev/blog/intro-generics).

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the `main` branch using forking and pull requests as described in contribution guidelines. The stable version is available via Golang modules.

Use `go get` to retrieve the library and add it as dependency to your application.

```bash
go get -u github.com/fogfish/stream
```

- [Getting Started](#getting-started)
  - [Data types definition](#data-types-definition)
  - [Stream I/O](#stream-io)
  - [Working with streams metadata](#working-with-streams-metadata)
  - [Error Handling](#error-handling)
  - [Streaming presigned url](#streaming-presigned-url)

### Data types definition

Data types definition is an essential part of development with `stream` library. Golang structs declares metadata of your binary objects. Public fields are serialized into S3 metadata attributes, the field tag `metadata` controls marshal/unmarshal process. 

The library demands from each structure implementation of `Thing` interface. This type acts as struct annotation -- Golang compiler raises an error at compile time if other data type is supplied for Stream I/O. Secondly, each structure defines unique "composite primary key". The library encourages definition of both partition and sort keys, which facilitates linked-data, hierarchical structures and cheap relations between data elements.

```go
type Note struct {
	Author          string    `metadata:"Author"`
	ID              string    `metadata:"Id"`
  ContentType     string    `metadata:"Content-Type"`
	ContentLanguage string    `metadata:"Content-Language"`
}

//
// Identity implements thing interface
func (n Note) HashKey() string { return n.Author }
func (n Note) SortKey() string { return n.ID }

//
// this data type is a normal Golang struct
// just create an instance, fill required fields
// The struct define the path to the object on the bucket as
// composition of Has & Sort keys together with object's metadata
var note := Note{
  Author:          "haskell",
  ID:              "8980789222",
  ContentType:     "text/plain",
  ContentLanguage: "en",
}
```

This is it! Your application is ready to stream data to/form AWS S3 Buckets.

### Stream I/O

Please [see and try examples](./examples/). Its cover all basic use-cases with runnable code snippets.

```bash
go run examples/stream/stream.go s3:///my-bucket
```

The following code snippet shows a typical I/O patterns

```go
import (
  "github.com/fogfish/stream"
  "github.com/fogfish/stream/service/s3"
)

//
// Create client and bind it with the bucket
// Use URI notation to specify the diver (s3://) and the bucket (/my-bucket) 
db, err := s3.New[Note](s3.WithBucket("my-bucket"))

//
// Write the stream with Put
stream := io.NopCloser(strings.NewReader("..."))
if err := db.Put(context.TODO(), note, stream); err != nil {
}

//
// Lookup the stream using Get. This function takes input structure as key
// and return a new copy upon the completion. The only requirement - ID has to
// be defined.
note, stream, err := db.Get(context.TODO(),
  Note{
    Author:  "haskell",
    ID:      "8980789222",
  },
)

switch {
case nil:
  // success
case recoverNotFound(err):
  // not found
default:
  // other i/o error
}

//
// Remove the stream using Remove
err := db.Remove(
  Note{
    Author:  "haskell",
    ID:      "8980789222",
  },
)

if err != nil { /* ... */ }
```

### Working with streams metadata

Please see the original AWS post about [Working with object metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html). The library support both system and user-defined metadata. System define metadata is few well-known attributes: `Cache-Control`, `Content-Encoding`, `Content-Language`, `Content-Type` and `Expires`.

```go
type Note struct {
  // User-defined metadata
  Author    string `metadata:"Author"`
  ID        string `metadata:"Id"`
  Custom    string `metadata:"Custom"`
  Attribute string `metadata:"Attribute"`
  // System metadata
  CacheControl    *string    `metadata:"Cache-Control"`
  ContentEncoding *string    `metadata:"Content-Encoding"`
  ContentLanguage *string    `metadata:"Content-Language"`
  ContentType     *string    `metadata:"Content-Type"`
  Expires         *time.Time `metadata:"Expires"`
  LastModified    *time.Time `metadata:"Last-Modified"`
}
```

### Error Handling

The library enforces for "assert errors for behavior, not type" as the error handling strategy, see [the post](https://tech.fog.fish/2022/07/05/assert-golang-errors-for-behavior.html) for details. 

Use following behaviors to recover from errors

```go
type ErrorCode interface{ ErrorCode() string }

type NotFound interface { NotFound() string }
```

### Streaming presigned url

Usage of `io.Reader` interface is sufficient for majority cloud applications. However, sometime is required to delegate read/write responsibility to mobile client. For example, uploading images or video files from mobile client to S3 bucket directly is scalable and way more efficient than doing this thought backend system. The library supports a special case for streaming binary objects using pre-signed urls, where `Put` & `Get` methods returns pre-signed URL instead of actual stream: 

```go
type Streamer[T stream.Thing] interface {
  Put(T) (string, error)
  Get(T) (string, error)
  Has(T) (T, error)
  Remove(T) error
  Match(T) Seq[T]
}
```

Write object using the library:

```go
import (
  "github.com/fogfish/stream"
  "github.com/fogfish/stream/service/s3url"
)

db, err := s3url.New[Note](s3url.WithBucket("my-bucket"))

url, err := db.Put(context.TODO(),
  Note{
    Author:          "haskell",
    ID:              "8980789222",
    ContentType:     "text/plain",
    ContentLanguage: "en",
  },
  5*time.Minute
)
```

Note: the usage of pre-signed url requires passing of all headers that has been passed to `Put` function

```bash
curl -XPUT https://pre-signed-url-goes-here \
  -H 'Content-Type: text/plain' \
  -H 'Content-Language: en' \
  -H 'X-Amz-Meta-Author: haskell' \
  -H 'X-Amz-Meta-Id: 8980789222' \
  -d 'some content'
```

Read object using the library:

```go
url, err := db.Get(context.TODO(),
  Note{
    Author:          "haskell",
    ID:              "8980789222",
  },
  5*time.Minute
)
```

```bash
curl -XGET https://pre-signed-url-goes-here
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
git clone https://github.com/fogfish/dynamo
cd dynamo
go test
```

### commit message

The commit message helps us to write a good release note, speed-up review process. The message should address two question what changed and why. The project follows the template defined by chapter [Contributing to a Project](http://git-scm.com/book/ch5-2.html) of Git book.

### bugs

If you experience any issues with the library, please let us know via [GitHub issues](https://github.com/fogfish/dynamo/issue). We appreciate detailed and accurate reports that help us to identity and replicate the issue. 

## License

[![See LICENSE](https://img.shields.io/github/license/fogfish/stream.svg?style=for-the-badge)](LICENSE)
