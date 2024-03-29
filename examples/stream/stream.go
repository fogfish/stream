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
	"github.com/fogfish/stream/service/s3"
)

// Number of object to create
const n = 5

// Note is an object manipulated at storage
type Note struct {
	Author          curie.IRI  `metadata:"author"`
	ID              curie.IRI  `metadata:"id"`
	ContentType     string     `metadata:"Content-Type"`
	ContentLanguage string     `metadata:"Content-Language"`
	LastModified    *time.Time `metadata:"Last-Modified"`
}

func (n Note) HashKey() curie.IRI { return n.Author }
func (n Note) SortKey() curie.IRI { return n.ID }

type Storage = *s3.Storage[Note]

func main() {
	db, err := s3.New[Note](
		s3.WithBucket(os.Args[1]),
		s3.WithPrefixes(curie.Namespaces{
			"person": "t/person/",
			"note":   "note/",
			"backup": "backup/note/",
		}),
	)
	if err != nil {
		panic(err)
	}

	examplePut(db)
	exampleGet(db)
	exampleHas(db)
	exampleMatch(db)
	exampleCopy(db)
	exampleRemove(db)
}

func examplePut(db Storage) {
	for i := 0; i < n; i++ {
		note := Note{
			Author:          curie.New("person:%d", i),
			ID:              curie.New("note:%d", i),
			ContentType:     "text/plain",
			ContentLanguage: "en",
		}
		data := io.NopCloser(
			strings.NewReader(
				fmt.Sprintf("This is example note %d.", i),
			),
		)
		err := db.Put(context.Background(), note, data)
		if err != nil {
			fmt.Printf("=[ put ]=> failed: %s", err)
			continue
		}

		fmt.Printf("=[ put ]=> %+v\n", note)
	}
}

func exampleGet(db Storage) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		note, sin, err := db.Get(context.Background(), key)
		if err != nil {
			fmt.Printf("=[ get ]=> failed: %s\n", err)
			continue
		}

		data, err := io.ReadAll(sin)
		if err != nil {
			fmt.Printf("=[ get ]=> failed: %s\n", err)
			continue
		}

		fmt.Printf("=[ get ]=> %+v\n%s\n", note, data)
	}
}

func exampleHas(db Storage) {
	for i := 0; i < n; i++ {
		key := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		note, err := db.Has(context.Background(), key)
		if err != nil {
			fmt.Printf("=[ has ]=> failed: %s\n", err)
			continue
		}

		fmt.Printf("=[ has ]=> %+v\n", note)
	}
}

func exampleMatch(db Storage) {
	key := Note{Author: curie.New("person:")}
	seq, cur, err := db.Match(context.Background(), key, stream.Limit[Note](2))
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
		return
	}

	fmt.Println("=[ match 1st ]=> ")
	for _, note := range seq {
		fmt.Printf("=[ match ]=> %+v\n", note)
	}

	seq, _, err = db.Match(context.Background(), key, cur)
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
		return
	}

	fmt.Println("=[ match 2nd ]=> ")
	for _, note := range seq {
		fmt.Printf("=[ match ]=> %+v\n", note)
	}
}

func exampleCopy(db Storage) {
	for i := 0; i < n; i++ {
		note := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		backup := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("backup:%d", i),
		}

		if err := db.Copy(context.TODO(), note, backup); err != nil {
			fmt.Printf("=[ copy ]=> failed: %v\n", err)
			continue
		}

		if err := db.Wait(context.TODO(), backup, 1*time.Minute); err != nil {
			fmt.Printf("=[ copy ]=> failed: %v\n", err)
			continue
		}

		fmt.Printf("=[ copy ]=> %+v to %+v\n", note, backup)
	}
}

func exampleRemove(db Storage) {
	for i := 0; i < n; i++ {
		for _, key := range []Note{
			{Author: curie.New("person:%d", i), ID: curie.New("note:%d", i)},
			{Author: curie.New("person:%d", i), ID: curie.New("backup:%d", i)},
		} {
			err := db.Remove(context.TODO(), key)
			if err != nil {
				fmt.Printf("=[ remove ]=> failed: %v\n", err)
				continue
			}

			fmt.Println("=[ remove ]=> ", key)
		}
	}
}
