package s3

import (
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/fogfish/curie"
	"github.com/fogfish/golem/pure/hseq"
	"github.com/fogfish/stream"
)

type Codec[T stream.Thing] struct {
	system   map[string]hseq.Type[T]
	metadata map[string]hseq.Type[T]
	prefixes curie.Prefixes
}

func NewCodec[T stream.Thing](prefixes curie.Prefixes) Codec[T] {
	codec := Codec[T]{
		system:   make(map[string]hseq.Type[T]),
		metadata: make(map[string]hseq.Type[T]),
		prefixes: prefixes,
	}

	hseq.FMap(
		hseq.Generic[T](),
		func(t hseq.Type[T]) error {
			name := strings.Split(t.StructField.Tag.Get("metadata"), ",")[0]
			if name != "" {
				if isSystemMetadata(name) {
					codec.system[name] = t
				} else {
					codec.metadata[name] = t
				}
			}
			return nil
		},
	)

	return codec
}

func isSystemMetadata(id string) bool {
	switch id {
	case "Cache-Control":
		return true
	case "Content-Encoding":
		return true
	case "Content-Language":
		return true
	case "Content-Type":
		return true
	case "Expires":
		return true
	default:
		return false
	}
}

//
func (codec Codec[T]) EncodeKey(key stream.Thing) string {
	hkey := curie.URI(codec.prefixes, key.HashKey())
	skey := curie.URI(codec.prefixes, key.SortKey())

	if skey == "" {
		return hkey
	}

	return hkey + "/" + skey
}

func (codec Codec[T]) Encode(entity T) *s3.PutObjectInput {
	req := &s3.PutObjectInput{}
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}

	codec.encodeCacheControl(val, req)
	codec.encodeContentEncoding(val, req)
	codec.encodeContentLanguage(val, req)
	codec.encodeContentType(val, req)
	codec.encodeExpires(val, req)
	codec.encodeMetadata(val, req)

	req.Key = aws.String(codec.EncodeKey(entity))
	return req
}

func (codec Codec[T]) encodeCacheControl(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Cache-Control"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.CacheControl = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentEncoding(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Encoding"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentEncoding = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentLanguage(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Language"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentLanguage = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentType(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Type"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentType = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeExpires(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Expires"]
	if ok {
		t := entity.FieldByIndex(f.Index).Interface().(time.Time)
		req.Expires = &t
	}
}

func (codec Codec[T]) encodeMetadata(entity reflect.Value, req *s3.PutObjectInput) {
	if len(codec.metadata) > 0 {
		req.Metadata = map[string]string{}
		for k, f := range codec.metadata {
			if val := entity.FieldByIndex(f.Index).String(); val != "" {
				req.Metadata[k] = val
			}
		}
	}
}

func (codec Codec[T]) Decode(obj *s3.GetObjectOutput) T {
	var val T

	// pointer to c makes reflect.ValueOf settable
	// see The third law of reflection
	// https://go.dev/blog/laws-of-reflection
	gen := reflect.ValueOf(&val).Elem()
	if gen.Kind() == reflect.Pointer {
		// T is a pointer type, therefore c is nil
		// it has to be filled with empty value before merging
		empty := reflect.New(gen.Type().Elem())
		gen.Set(empty)
		gen = gen.Elem()
	}

	codec.decodeCacheControl(gen, obj)
	codec.decodeContentEncoding(gen, obj)
	codec.decodeContentLanguage(gen, obj)
	codec.decodeContentType(gen, obj)
	codec.decodeExpires(gen, obj)
	codec.decodeMetadata(gen, obj)

	return val
}

func (codec Codec[T]) decodeCacheControl(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Cache-Control"]
	if ok {
		if obj.CacheControl != nil {
			entity.FieldByIndex(f.Index).SetString(aws.ToString(obj.CacheControl))
		}
	}
}

func (codec Codec[T]) decodeContentEncoding(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Encoding"]
	if ok {
		if obj.ContentEncoding != nil {
			entity.FieldByIndex(f.Index).SetString(aws.ToString(obj.ContentEncoding))
		}
	}
}

func (codec Codec[T]) decodeContentLanguage(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Language"]
	if ok {
		if obj.ContentLanguage != nil {
			entity.FieldByIndex(f.Index).SetString(aws.ToString(obj.ContentLanguage))
		}
	}
}

func (codec Codec[T]) decodeContentType(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Type"]
	if ok {
		if obj.ContentType != nil {
			entity.FieldByIndex(f.Index).SetString(aws.ToString(obj.ContentType))
		}
	}
}

func (codec Codec[T]) decodeExpires(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Expires"]
	if ok {
		if obj.Expires != nil {
			entity.FieldByIndex(f.Index).Set(reflect.ValueOf(*obj.Expires))
		}
	}
}

func (codec Codec[T]) decodeMetadata(entity reflect.Value, obj *s3.GetObjectOutput) {
	if len(codec.metadata) > 0 {
		for k, f := range codec.metadata {
			val, exists := obj.Metadata[k]
			if exists {
				entity.FieldByIndex(f.Index).SetString(val)
			}
		}
	}
}
