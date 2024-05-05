//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

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
	ID              curie.IRI  `metadata:"id"`
	ContentType     string     `metadata:"Content-Type"`
	ContentLanguage string     `metadata:"Content-Language"`
	LastModified    *time.Time `metadata:"Last-Modified"`
}

func (n Note) HashKey() curie.IRI { return n.ID }

type Storage = *s3.Storage[Note]

func main() {
	db, err := s3.New[Note](
		s3.WithBucket(os.Args[1]),
		s3.WithPrefixes(curie.Namespaces{
			"person": "t/person/",
			"backup": "t/backup/",
		}),
	)
	if err != nil {
		panic(err)
	}

	examplePut(db)
	exampleGet(db)
	exampleHas(db)
	exampleMatch(db)
	exampleVisit(db)
	exampleCopy(db)
	exampleRemove(db)
}

func examplePut(db Storage) {
	for i := 0; i < n; i++ {
		note := Note{
			ID:              curie.New("person:%d/note/%d", i, i),
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
			ID: curie.New("person:%d/note/%d", i, i),
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
			ID: curie.New("person:%d/note/%d", i, i),
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
	re, _ := stream.RegExp[Note](`person/[0-9]/note/.*`)

	key := Note{ID: curie.New("person:")}
	seq, cur, err := db.Match(context.Background(), key, stream.Limit[Note](2), re)
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
		return
	}

	fmt.Println("=[ match 1st ]=> ")
	for _, note := range seq {
		note, err = db.Has(context.Background(), note)
		if err != nil {
			fmt.Printf("=[ has ]=> failed: %s\n", err)
			return
		}

		fmt.Printf("=[ match ]=> %+v\n", note)
	}

	seq, _, err = db.Match(context.Background(), key, cur, re)
	if err != nil {
		fmt.Printf("=[ match ]=> failed: %v\n", err)
		return
	}

	fmt.Println("=[ match 2nd ]=> ")
	for _, note := range seq {
		note, err = db.Has(context.Background(), note)
		if err != nil {
			fmt.Printf("=[ has ]=> failed: %s\n", err)
			return
		}

		fmt.Printf("=[ match ]=> %+v\n", note)
	}
}

func exampleVisit(db Storage) {
	key := Note{ID: curie.New("person:")}
	err := db.Visit(context.Background(), key,
		func(n Note) (err error) {
			n, err = db.Has(context.Background(), n)
			if err != nil {
				fmt.Printf("=[ has ]=> failed: %s\n", err)
				return
			}

			fmt.Printf("=[ visit ]=> %+v\n", n)
			return nil
		},
	)

	if err != nil {
		fmt.Printf("=[ visit ]=> failed: %v\n", err)
		return
	}
}

func exampleCopy(db Storage) {
	for i := 0; i < n; i++ {
		person := Note{ID: curie.New("person:%d/note/%d", i, i)}
		backup := Note{ID: curie.New("backup:%d/note/%d", i, i)}

		if err := db.Copy(context.TODO(), person, backup); err != nil {
			fmt.Printf("=[ copy ]=> failed: %v\n", err)
			continue
		}

		if err := db.Wait(context.TODO(), backup, 1*time.Minute); err != nil {
			fmt.Printf("=[ copy ]=> failed: %v\n", err)
			continue
		}

		fmt.Printf("=[ copy ]=> %+v to %+v\n", person, backup)
	}
}

func exampleRemove(db Storage) {
	for i := 0; i < n; i++ {
		for _, key := range []Note{
			{ID: curie.New("person:%d/note/%d", i, i)},
			{ID: curie.New("backup:%d/note/%d", i, i)},
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
