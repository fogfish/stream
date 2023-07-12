package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fogfish/curie"
	"github.com/fogfish/gurl/v2/http"
	ƒ "github.com/fogfish/gurl/v2/http/recv"
	ø "github.com/fogfish/gurl/v2/http/send"
	"github.com/fogfish/stream"
	"github.com/fogfish/stream/service/s3url"
)

// Number of object to create
const n = 5

// Note is an object manipulated at storage
type Note struct {
	Author          curie.IRI `metadata:"author"`
	ID              curie.IRI `metadata:"id"`
	ContentType     string    `metadata:"Content-Type"`
	ContentLanguage string    `metadata:"Content-Language"`
	Content         string
}

func (n Note) HashKey() curie.IRI { return n.Author }
func (n Note) SortKey() curie.IRI { return n.ID }

type Storage = *s3url.Storage[Note]

func main() {
	db, err := s3url.New[Note](
		stream.WithBucket(os.Args[1]),
		stream.WithPrefixes(curie.Namespaces{
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
	http := http.New()

	for i := 0; i < n; i++ {
		note := Note{
			Author:          curie.New("person:%d", i),
			ID:              curie.New("note:%d", i),
			ContentType:     "text/plain",
			ContentLanguage: "en",
			Content:         fmt.Sprintf("This is example note %d.", i),
		}

		uri, err := db.Put(context.Background(), note, stream.AccessExpiredIn(1*time.Minute))
		if err != nil {
			fmt.Printf("=[ put ]=> failed: %s\n", err)
			continue
		}

		err = examplePutByURL(http, uri, note)
		if err != nil {
			fmt.Printf("=[ put ]=> failed: %s\n", err)
			continue
		}

		fmt.Printf("=[ put ]=> %+v\n", note)
	}
}

func examplePutByURL(stack http.Stack, uri string, note Note) error {
	return stack.IO(context.Background(),
		http.PUT(
			ø.URI(uri),
			ø.ContentType.Text,
			ø.Header("X-Amz-Meta-Author", note.Author.Safe()),
			ø.Header("X-Amz-Meta-Id", note.ID.Safe()),
			ø.Header("Content-Language", note.ContentLanguage),
			ø.Send(note.Content),

			ƒ.Status.OK,
		),
	)
}

func exampleGet(db Storage) {
	http := http.New()

	for i := 0; i < n; i++ {
		note := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		uri, err := db.Get(context.Background(), note, stream.AccessExpiredIn(1*time.Minute))
		if err != nil {
			fmt.Printf("=[ get ]=> failed: %s", err)
			continue
		}

		note, err = exampleGetByURL(http, uri)
		if err != nil {
			fmt.Printf("=[ get ]=> failed: %s", err)
			continue
		}

		fmt.Printf("=[ get ]=> %+v\n", note)
	}
}

func exampleGetByURL(stack http.Stack, uri string) (Note, error) {
	var (
		note Note
		data []byte
	)
	err := stack.IO(context.Background(),
		http.GET(
			ø.URI(uri),
			ø.Accept.TextPlain,

			ƒ.Status.OK,
			ƒ.Header("X-Amz-Meta-Author", (*string)(&note.Author)),
			ƒ.Header("X-Amz-Meta-Id", (*string)(&note.ID)),
			ƒ.Header("Content-Type", &note.ContentType),
			ƒ.Header("Content-Language", &note.ContentLanguage),
			ƒ.Bytes(&data),
		),
	)

	note.Content = string(data)
	return note, err
}

func exampleHas(db Storage) {
	for i := 0; i < n; i++ {
		note := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		val, err := db.Has(context.Background(), note)
		if err != nil {
			fmt.Printf("=[ has ]=> failed: %v\n", err)
			continue
		}

		fmt.Printf("=[ has ]=> %+v\n", val)
	}
}

func exampleMatch(db Storage) {
	http := http.New()

	key := Note{Author: curie.New("person:")}
	seq, cur, err := db.Match(context.Background(), key, stream.AccessExpiredIn(1*time.Minute), stream.Limit(2))
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
	}

	fmt.Println("=[ match 1st ]=> ")
	for _, url := range seq {
		note, err := exampleGetByURL(http, url)
		if err != nil {
			return
		}

		fmt.Printf("=[ match ]=> %+v\n", note)
	}

	seq, _, err = db.Match(context.Background(), key, stream.AccessExpiredIn(1*time.Minute), cur)
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
	}

	fmt.Println("=[ match 2nd ]=> ")
	for _, url := range seq {
		note, err := exampleGetByURL(http, url)
		if err != nil {
			return
		}

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
