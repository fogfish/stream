package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fogfish/curie"
	"github.com/fogfish/gurl/http"
	ƒ "github.com/fogfish/gurl/http/recv"
	ø "github.com/fogfish/gurl/http/send"
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
	db, err := s3url.New[Note](os.Args[1],
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

		uri, err := db.Put(context.Background(), note, 1*time.Minute)
		if err != nil {
			fmt.Printf("=[ put ]=> failed: %s", err)
			continue
		}

		err = examplePutByURL(http, uri, note)
		if err != nil {
			fmt.Printf("=[ put ]=> failed: %s", err)
			continue
		}

		fmt.Printf("=[ put ]=> %+v\n", note)
	}
}

func examplePutByURL(http http.Stack, uri string, note Note) error {
	return http.IO(context.Background(),
		ø.PUT.URI(uri),
		ø.ContentType.Text,
		ø.Header("X-Amz-Meta-Author").Is(string(note.Author)),
		ø.Header("X-Amz-Meta-Id").Is(string(note.ID)),
		ø.Header("Content-Language").Is(note.ContentLanguage),
		ø.Send(note.Content),

		ƒ.Status.OK,
	)
}

func exampleGet(db Storage) {
	http := http.New()

	for i := 0; i < n; i++ {
		note := Note{
			Author: curie.New("person:%d", i),
			ID:     curie.New("note:%d", i),
		}

		uri, err := db.Get(context.Background(), note, 1*time.Minute)
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

func exampleGetByURL(http http.Stack, uri string) (Note, error) {
	var (
		note Note
		data []byte
	)
	err := http.IO(context.Background(),
		ø.GET.URI(uri),
		ø.Accept.TextPlain,

		ƒ.Status.OK,
		ƒ.Header("X-Amz-Meta-Author").To((*string)(&note.Author)),
		ƒ.Header("X-Amz-Meta-Id").To((*string)(&note.ID)),
		ƒ.Header("Content-Type").To(&note.ContentType),
		ƒ.Header("Content-Language").To(&note.ContentLanguage),
		ƒ.Bytes(&data),
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
	seq, err := db.Match(context.Background(), key, 1*time.Minute)
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
	}

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
			val, err := db.Remove(context.TODO(), key)
			if err != nil {
				fmt.Printf("=[ remove ]=> failed: %v\n", err)
				continue
			}

			fmt.Println("=[ remove ]=> ", val)
		}
	}
}
