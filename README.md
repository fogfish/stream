# stream

The library implements a simple key-value abstraction to store binary data at AWS storage services: AWS S3.


## Inspiration

The library encourages developers to use Golang `io.ReadCloser` to define manipulation with binary data, write correct, maintainable code.The library uses interfaces to abstract and expose domain objects as streams and their identity. The library uses [AWS Golang SDK](https://aws.amazon.com/sdk-for-go/) under the hood.

Essentially, the library implement a following generic key-value trait to access domain objects. 

```go
type Stream interface {
  Put(Thing, io.ReadCloser) error
  Get(Thing) (io.ReadCloser, error)
  Remove(Thing) error
  Match(Thing) Seq
}
```

## Getting started

The latest version of the library is available at its `main` branch. All development, including new features and bug fixes, take place on the `main` branch using forking and pull requests as described in contribution guidelines. The stable version is available via Golang modules.

1. Use `go get` to retrieve the library and add it as dependency to your application.

```bash
go get -u github.com/fogfish/stream
```

2. Import required package in your code

```go
import (
  "github.com/fogfish/stream"
  "github.com/fogfish/stream/creek"
)
```

### Data types definition

Data types definition is an essential part of development with `stream` library. The library demands implementation of `Thing` interface from application as identity from each stream. Secondly, each key is unique "composite primary key". The library encourages definition of both partition and sort keys, which facilitates linked-data, hierarchical structures and cheap relations between data elements.

```go
import "github.com/fogfish/stream"

type Note struct {
	Author string
	ID     string
}

//
// Identity implements thing interface
func (n Note) HashKey() string { return n.Author }
func (n Note) SortKey() string { return n.ID }

//
// this data type is a normal Golang struct
// just create an instance, fill required fields
var note := Note{
  Author:  "haskell",
  ID:      "8980789222",
}
```

### Stream I/O

Please [see and try examples](examples). Its cover all basic use-cases with runnable code snippets.

```bash
go run examples/stream/stream.go s3:///my-bucket
```

The following code snippet shows a typical I/O patterns

```go
import (
  "github.com/fogfish/stream"
  "github.com/fogfish/stream/creek"
)

//
// Create client and bind it with the bucket
// Use URI notation to specify the diver (s3://) and the bucket (/my-bucket) 
db := creek.Must(creek.New("s3:///my-bucket"))

//
// Write the stream with Put
stream := io.NopCloser(strings.NewReader("..."))
if err := db.Put(note, stream); err != nil {
}

//
// Lookup the stream using Get. This function takes input structure as key
// and return a new copy upon the completion. The only requirement - ID has to
// be defined.
stream, err := db.Get(
  Note{
    Author:  "haskell",
    ID:      "8980789222",
  },
)

switch v := err.(type) {
case nil:
  // success
case stream.NotFound:
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
