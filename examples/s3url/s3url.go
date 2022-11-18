package main

import (
	"context"
	"fmt"
	"time"

	"github.com/fogfish/curie"
	"github.com/fogfish/stream/service/s3url"
)

type Note struct {
	Author curie.IRI //`metadata:"author"`
	ID     curie.IRI //`metadata:"id"`
	// CacheControl *string   `metadata:"Cache-Control"`
	ContentType *string `metadata:"Content-Type"`
	// Expires      *time.Time `metadata:"Expires"`
}

func (n Note) HashKey() curie.IRI { return n.Author }
func (n Note) SortKey() curie.IRI { return n.ID }

func main() {
	db, err := s3url.New[Note]("swarm-example-s3-latest")
	if err != nil {
		panic(err)
	}

	note := Note{
		Author: curie.New("a:joe"),
		// ID:          curie.New("n:doc1"),
		// ContentType: aws.String("image/jpg"),
	}

	err = db.Match(context.TODO(), note, 30*time.Minute).
		FMap(func(s string) error {
			fmt.Println(s)
			return nil
		})

	fmt.Println(err)
	// fmt.Println(url)

}
