package s3_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	a3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/fogfish/it"
	"github.com/fogfish/stream/internal/s3"
)

type Note struct {
	Author          string
	ID              string
	CacheControl    string    `json:"Cache-Control,omitempty"`
	ContentEncoding string    `json:"Content-Encoding,omitempty"`
	ContentLanguage string    `json:"Content-Language,omitempty"`
	ContentType     string    `json:"Content-Type,omitempty"`
	Expires         time.Time `json:"Expires,omitempty"`
	Custom          string    `json:"custom,omitempty"`
	Attribute       string    `json:"attribute,omitempty"`
}

func (n Note) HashKey() string { return n.Author }
func (n Note) SortKey() string { return n.ID }

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
		Expires:         aws.String("Fri, 22 Apr 2022 12:34:56 UTC"),
		Metadata: map[string]*string{
			"custom":    aws.String("Custom"),
			"attribute": aws.String("Attribute"),
		},
	}
}

func TestEncode(t *testing.T) {
	codec := s3.NewCodec[Note]()
	val := codec.Encode(fixtureNote())

	it.Ok(t).
		If(*val.CacheControl).Equal("Cache-Control").
		If(*val.ContentEncoding).Equal("Content-Encoding").
		If(*val.ContentLanguage).Equal("Content-Language").
		If(*val.ContentType).Equal("Content-Type").
		If(*val.Expires).Equal(fixtureTime).
		If(*val.Metadata["custom"]).Equal("Custom").
		If(*val.Metadata["attribute"]).Equal("Attribute")
}

func TestDecode(t *testing.T) {
	codec := s3.NewCodec[Note]()
	val := codec.Decode(fixtureObject())

	it.Ok(t).
		If(val.CacheControl).Equal("Cache-Control").
		If(val.ContentEncoding).Equal("Content-Encoding").
		If(val.ContentLanguage).Equal("Content-Language").
		If(val.ContentType).Equal("Content-Type").
		If(val.Expires).Equal(fixtureTime).
		If(val.Custom).Equal("Custom").
		If(val.Attribute).Equal("Attribute")
}
