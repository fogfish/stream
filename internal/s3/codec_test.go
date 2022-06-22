package s3_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	a3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/it"
	"github.com/fogfish/stream/internal/s3"
)

type Note struct {
	// User-defined metadata
	Author    curie.IRI `metadata:"Author"`
	ID        curie.IRI `metadata:"Id"`
	Custom    string    `metadata:"Custom"`
	Attribute string    `metadata:"Attribute"`
	// System metadata
	CacheControl    string    `metadata:"Cache-Control"`
	ContentEncoding string    `metadata:"Content-Encoding"`
	ContentLanguage string    `metadata:"Content-Language"`
	ContentType     string    `metadata:"Content-Type"`
	Expires         time.Time `metadata:"Expires"`
}

func (n Note) HashKey() curie.IRI { return n.Author }
func (n Note) SortKey() curie.IRI { return n.ID }

var fixtureTime, _ = time.Parse(time.RFC1123, "Fri, 22 Apr 2022 12:34:56 UTC")

func fixtureNote() Note {
	return Note{
		Author:          "haskell",
		ID:              "8980789222",
		CacheControl:    "Cache-Control",
		ContentEncoding: "Content-Encoding",
		ContentLanguage: "Content-Language",
		ContentType:     "Content-Type",
		Expires:         fixtureTime,
		Custom:          "Custom",
		Attribute:       "Attribute",
	}
}

func fixtureObject() *a3.GetObjectOutput {
	return &a3.GetObjectOutput{
		CacheControl:    aws.String("Cache-Control"),
		ContentEncoding: aws.String("Content-Encoding"),
		ContentLanguage: aws.String("Content-Language"),
		ContentType:     aws.String("Content-Type"),
		Expires:         &fixtureTime,
		Metadata: map[string]string{
			"Author":    "haskell",
			"Id":        "8980789222",
			"Custom":    "Custom",
			"Attribute": "Attribute",
		},
	}
}

func TestEncode(t *testing.T) {
	codec := s3.NewCodec[Note](curie.Namespaces{})
	val := codec.Encode(fixtureNote())

	it.Ok(t).
		If(*val.CacheControl).Equal("Cache-Control").
		If(*val.ContentEncoding).Equal("Content-Encoding").
		If(*val.ContentLanguage).Equal("Content-Language").
		If(*val.ContentType).Equal("Content-Type").
		If(*val.Expires).Equal(fixtureTime).
		If(val.Metadata["Author"]).Equal("haskell").
		If(val.Metadata["Id"]).Equal("8980789222").
		If(val.Metadata["Custom"]).Equal("Custom").
		If(val.Metadata["Attribute"]).Equal("Attribute")
}

func TestDecode(t *testing.T) {
	codec := s3.NewCodec[Note](curie.Namespaces{})
	val := codec.Decode(fixtureObject())

	it.Ok(t).
		If(val.CacheControl).Equal("Cache-Control").
		If(val.ContentEncoding).Equal("Content-Encoding").
		If(val.ContentLanguage).Equal("Content-Language").
		If(val.ContentType).Equal("Content-Type").
		If(val.Expires).Equal(fixtureTime).
		If(val.Author).Equal(curie.IRI("haskell")).
		If(val.ID).Equal(curie.IRI("8980789222")).
		If(val.Custom).Equal("Custom").
		If(val.Attribute).Equal("Attribute")
}
