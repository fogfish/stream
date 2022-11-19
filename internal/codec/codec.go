package codec

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
	system    map[string]hseq.Type[T]
	metadata  map[string]hseq.Type[T]
	prefixes  curie.Prefixes
	Undefined T
}

func New[T stream.Thing](prefixes curie.Prefixes) Codec[T] {
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
	case "Last-Modified":
		return true
	default:
		return false
	}
}

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
		req.CacheControl = codec.encodeValueOfString(entity.FieldByIndex(f.Index))
	}
}

func (codec Codec[T]) encodeContentEncoding(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Encoding"]
	if ok {
		req.ContentEncoding = codec.encodeValueOfString(entity.FieldByIndex(f.Index))
	}
}

func (codec Codec[T]) encodeContentLanguage(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Language"]
	if ok {
		req.ContentLanguage = codec.encodeValueOfString(entity.FieldByIndex(f.Index))
	}
}

func (codec Codec[T]) encodeContentType(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Content-Type"]
	if ok {
		req.ContentType = codec.encodeValueOfString(entity.FieldByIndex(f.Index))
	}
}

func (codec Codec[T]) encodeExpires(entity reflect.Value, req *s3.PutObjectInput) {
	f, ok := codec.system["Expires"]
	if ok {
		val := entity.FieldByIndex(f.Index).Interface()
		switch t := val.(type) {
		case time.Time:
			req.Expires = &t
		case *time.Time:
			req.Expires = t
		}
	}
}

func (codec Codec[T]) encodeMetadata(entity reflect.Value, req *s3.PutObjectInput) {
	if len(codec.metadata) > 0 {
		req.Metadata = map[string]string{}
		for k, f := range codec.metadata {
			val := codec.encodeValueOfString(entity.FieldByIndex(f.Index))
			if val != nil {
				req.Metadata[k] = *val
			}
		}
	}
}

func (codec Codec[T]) DecodeGetObject(obj *s3.GetObjectOutput) T {
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

	codec.decodeCacheControl(gen, obj.CacheControl)
	codec.decodeContentEncoding(gen, obj.ContentEncoding)
	codec.decodeContentLanguage(gen, obj.ContentLanguage)
	codec.decodeContentType(gen, obj.ContentType)
	codec.decodeExpires(gen, obj.Expires)
	codec.decodeLastModified(gen, obj.LastModified)
	codec.decodeMetadata(gen, obj.Metadata)

	return val
}

func (codec Codec[T]) DecodeHasObject(obj *s3.HeadObjectOutput) T {
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

	codec.decodeCacheControl(gen, obj.CacheControl)
	codec.decodeContentEncoding(gen, obj.ContentEncoding)
	codec.decodeContentLanguage(gen, obj.ContentLanguage)
	codec.decodeContentType(gen, obj.ContentType)
	codec.decodeExpires(gen, obj.Expires)
	codec.decodeLastModified(gen, obj.LastModified)
	codec.decodeMetadata(gen, obj.Metadata)

	return val
}

func (codec Codec[T]) decodeCacheControl(entity reflect.Value, val *string) {
	f, ok := codec.system["Cache-Control"]
	if ok && val != nil {
		codec.decodeValueOfString(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeContentEncoding(entity reflect.Value, val *string) {
	f, ok := codec.system["Content-Encoding"]
	if ok && val != nil {
		codec.decodeValueOfString(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeContentLanguage(entity reflect.Value, val *string) {
	f, ok := codec.system["Content-Language"]
	if ok && val != nil {
		codec.decodeValueOfString(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeContentType(entity reflect.Value, val *string) {
	f, ok := codec.system["Content-Type"]
	if ok && val != nil {
		codec.decodeValueOfString(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeExpires(entity reflect.Value, val *time.Time) {
	f, ok := codec.system["Expires"]
	if ok && val != nil {
		codec.decodeValueOfTime(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeLastModified(entity reflect.Value, val *time.Time) {
	f, ok := codec.system["Last-Modified"]
	if ok && val != nil {
		codec.decodeValueOfTime(entity.FieldByIndex(f.Index), val)
	}
}

func (codec Codec[T]) decodeMetadata(entity reflect.Value, val map[string]string) {
	if len(codec.metadata) > 0 {
		for k, f := range codec.metadata {
			if val, exists := val[k]; exists {
				codec.decodeValueOfString(entity.FieldByIndex(f.Index), &val)
			}
		}
	}
}

func (codec Codec[T]) decodeValueOfTime(field reflect.Value, val *time.Time) {
	if field.Kind() == reflect.Pointer {
		field.Set(reflect.ValueOf(val))
		return
	}

	field.Set(reflect.ValueOf(*val))
}

func (codec Codec[T]) encodeValueOfString(field reflect.Value) *string {
	if field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return nil
		}

		field = field.Elem()
	}

	val := field.String()
	if val == "" {
		return nil
	}

	return &val
}

func (codec Codec[T]) decodeValueOfString(field reflect.Value, val *string) {
	if field.Kind() == reflect.Pointer {
		field.Set(reflect.ValueOf(val))
		return
	}

	field.SetString(*val)
}
