package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fogfish/stream"
	"github.com/fogfish/stream/creek"
)

type Note struct {
	Author string `metadata:"Author"`
	ID     string `metadata:"Id"`
}

func (n Note) HashKey() string { return n.Author }
func (n Note) SortKey() string { return n.ID }

//
//
func main() {
	db := creek.Must(creek.New[Note](os.Args[1]))

	examplePut(db)
	exampleGet(db)
	exampleURL(db)
	exampleMatch(db)
	exampleRemove(db)
}

const n = 5

func examplePut(db stream.Stream[Note]) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: fmt.Sprintf("person:%d", i),
			ID:     fmt.Sprintf("note:%d", i),
		}
		val := io.NopCloser(
			strings.NewReader(
				fmt.Sprintf("This is example note %d.", i),
			),
		)
		err := db.Put(context.TODO(), key, val)

		fmt.Println("=[ put ]=> ", err)
	}
}

func exampleGet(db stream.Stream[Note]) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: fmt.Sprintf("person:%d", i),
			ID:     fmt.Sprintf("note:%d", i),
		}

		val, sio, err := db.Get(context.TODO(), key)
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

func exampleURL(db stream.Stream[Note]) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: fmt.Sprintf("person:%d", i),
			ID:     fmt.Sprintf("note:%d", i),
		}

		val, err := db.URL(context.TODO(), key, 20*time.Minute)
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

func exampleMatch(db stream.Stream[Note]) {
	db.Match(context.TODO(), Note{Author: fmt.Sprintf("person")}).
		FMap(func(key *Note, val io.ReadCloser) error {
			defer val.Close()
			b, _ := io.ReadAll(val)
			fmt.Printf("=[ match ]=> %+v %s\n", key, b)
			return nil
		})
}

func exampleRemove(db stream.Stream[Note]) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: fmt.Sprintf("person:%d", i),
			ID:     fmt.Sprintf("note:%d", i),
		}
		err := db.Remove(context.TODO(), key)

		fmt.Println("=[ remove ]=> ", err)
	}
}
