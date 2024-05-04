//
// Copyright (C) 2020 - 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package codec_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	a3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/it"
	"github.com/fogfish/stream/internal/codec"
)

type Note struct {
	// User-defined metadata
	ID        curie.IRI  `metadata:"Id"`
	IRI       *curie.IRI `metadata:"IRI"`
	Custom    string     `metadata:"Custom"`
	Attribute *string    `metadata:"Attribute"`
	// System metadata
	CacheControl    string     `metadata:"Cache-Control"`
	ContentEncoding string     `metadata:"Content-Encoding"`
	ContentLanguage *string    `metadata:"Content-Language"`
	ContentType     *string    `metadata:"Content-Type"`
	Expires         time.Time  `metadata:"Expires"`
	LastModified    *time.Time `metadata:"Last-Modified"`
}

func (n Note) HashKey() curie.IRI { return n.ID }

var fixtureTime, _ = time.Parse(time.RFC1123, "Fri, 22 Apr 2022 12:34:56 UTC")

func fixtureNote() Note {
	return Note{
		ID:              "haskell:8980789222",
		IRI:             (*curie.IRI)(aws.String("wiki:curie")),
		CacheControl:    "Cache-Control",
		ContentEncoding: "Content-Encoding",
		ContentLanguage: aws.String("Content-Language"),
		ContentType:     aws.String("Content-Type"),
		Expires:         fixtureTime,
		Custom:          "Custom",
		Attribute:       aws.String("Attribute"),
	}
}

func fixtureGetObject() *a3.GetObjectOutput {
	return &a3.GetObjectOutput{
		CacheControl:    aws.String("Cache-Control"),
		ContentEncoding: aws.String("Content-Encoding"),
		ContentLanguage: aws.String("Content-Language"),
		ContentType:     aws.String("Content-Type"),
		Expires:         &fixtureTime,
		LastModified:    &fixtureTime,
		Metadata: map[string]string{
			"Id":        "[haskell:8980789222]",
			"IRI":       "[wiki:curie]",
			"Custom":    "Custom",
			"Attribute": "Attribute",
		},
	}
}

func fixtureHasObject() *a3.HeadObjectOutput {
	return &a3.HeadObjectOutput{
		CacheControl:    aws.String("Cache-Control"),
		ContentEncoding: aws.String("Content-Encoding"),
		ContentLanguage: aws.String("Content-Language"),
		ContentType:     aws.String("Content-Type"),
		Expires:         &fixtureTime,
		LastModified:    &fixtureTime,
		Metadata: map[string]string{
			"Id":        "[haskell:8980789222]",
			"IRI":       "[wiki:curie]",
			"Custom":    "Custom",
			"Attribute": "Attribute",
		},
	}
}

func TestEncodeKey(t *testing.T) {
	codec := codec.New[Note](curie.Namespaces{})
	_, val := codec.EncodeKey(fixtureNote())
	it.Ok(t).
		If(val).Equal("haskell:8980789222")
}

func TestDecodeKey(t *testing.T) {
	codec := codec.New[Note](curie.Namespaces{})
	val := codec.DecodeKey("haskell:8980789222")
	it.Ok(t).
		If(val.ID).Equal(curie.IRI("haskell:8980789222"))
}

func TestEncode(t *testing.T) {
	codec := codec.New[Note](curie.Namespaces{})
	val := codec.Encode(fixtureNote())

	it.Ok(t).
		If(*val.CacheControl).Equal("Cache-Control").
		If(*val.ContentEncoding).Equal("Content-Encoding").
		If(*val.ContentLanguage).Equal("Content-Language").
		If(*val.ContentType).Equal("Content-Type").
		If(*val.Expires).Equal(fixtureTime).
		If(val.Metadata["Id"]).Equal("[haskell:8980789222]").
		If(val.Metadata["IRI"]).Equal("[wiki:curie]").
		If(val.Metadata["Custom"]).Equal("Custom").
		If(val.Metadata["Attribute"]).Equal("Attribute")
}

func TestDecodeWithGetObject(t *testing.T) {
	codec := codec.New[Note](curie.Namespaces{})
	val := codec.DecodeGetObject(fixtureGetObject())

	it.Ok(t).
		If(val.CacheControl).Equal("Cache-Control").
		If(val.ContentEncoding).Equal("Content-Encoding").
		If(*val.ContentLanguage).Equal("Content-Language").
		If(*val.ContentType).Equal("Content-Type").
		If(val.Expires).Equal(fixtureTime).
		If(*val.LastModified).Equal(fixtureTime).
		If(val.ID).Equal(curie.IRI("haskell:8980789222")).
		If(*val.IRI).Equal(curie.IRI("wiki:curie")).
		If(val.Custom).Equal("Custom").
		If(*val.Attribute).Equal("Attribute")
}

func TestDecodeWithHasObject(t *testing.T) {
	codec := codec.New[Note](curie.Namespaces{})
	val := codec.DecodeHasObject(fixtureHasObject())

	it.Ok(t).
		If(val.CacheControl).Equal("Cache-Control").
		If(val.ContentEncoding).Equal("Content-Encoding").
		If(*val.ContentLanguage).Equal("Content-Language").
		If(*val.ContentType).Equal("Content-Type").
		If(val.Expires).Equal(fixtureTime).
		If(*val.LastModified).Equal(fixtureTime).
		If(val.ID).Equal(curie.IRI("haskell:8980789222")).
		If(*val.IRI).Equal(curie.IRI("wiki:curie")).
		If(val.Custom).Equal("Custom").
		If(*val.Attribute).Equal("Attribute")
}
