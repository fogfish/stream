package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fogfish/curie"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/creek"
)

type Note struct {
	Author curie.IRI `metadata:"author"`
	ID     curie.IRI `metadata:"id"`
}

func (n Note) HashKey() curie.IRI { return n.Author }
func (n Note) SortKey() curie.IRI { return n.ID }

type Stream stream.Stream[*Note]

//
//
func main() {
	db := creek.Must(
		creek.New[*Note](
			stream.WithURI(os.Args[1]),
			stream.WithPrefixes(
				curie.Namespaces{
					"person": "t/person/",
					"note":   "note/",
				},
			),
		),
	)

	examplePut(db)
	exampleGet(db)
	exampleURL(db)
	exampleMatch(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db Stream) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}
		val := io.NopCloser(
			strings.NewReader(
				fmt.Sprintf("This is example note %d.", i),
			),
		)
		err := db.Put(context.TODO(), &key, val)

		fmt.Println("=[ put ]=> ", err)
	}
}

func exampleGet(db Stream) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		val, sio, err := db.Get(context.TODO(), &key)
		defer sio.Close()

		switch v := err.(type) {
		case nil:
			b, _ := io.ReadAll(sio)
			fmt.Printf("=[ get ]=> %+v %s\n", val, b)
		case stream.NotFound:
			fmt.Printf("=[ get ]=> Not found: (%v, %v)\n", key.Author, key.ID)
		default:
			fmt.Printf("=[ get ]=> Fail: %v\n", v)
		}
	}
}

func exampleURL(db Stream) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		val, err := db.URL(context.TODO(), &key, 20*time.Minute)
		switch v := err.(type) {
		case nil:
			fmt.Printf("=[ url ]=> %s\n", val)
		case stream.NotFound:
			fmt.Printf("=[ url ]=> Not found: (%v, %v)\n", key.Author, key.ID)
		default:
			fmt.Printf("=[ url ]=> Fail: %v\n", v)
		}
	}
}

func exampleMatch(db Stream) {
	key := Note{Author: curie.New("person:")}
	err := db.Match(context.TODO(), &key).
		FMap(func(key *Note, val io.ReadCloser) error {
			defer val.Close()
			b, _ := io.ReadAll(val)
			fmt.Printf("=[ match ]=> %+v %s\n", key, b)
			return nil
		})
	if err != nil {
		fmt.Printf("=[ match ]=> %v\n", err)
	}
}

func exampleRemove(db Stream) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}
		err := db.Remove(context.TODO(), &key)

		fmt.Println("=[ remove ]=> ", err)
	}
}
