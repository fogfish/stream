package s3

import (
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fogfish/golem/pure/hseq"
	"github.com/fogfish/stream"
)

type Codec[T stream.Thing] struct {
	system   map[string]hseq.Type[T]
	metadata map[string]hseq.Type[T]
}

func NewCodec[T stream.Thing]() Codec[T] {
	codec := Codec[T]{
		system:   make(map[string]hseq.Type[T]),
		metadata: make(map[string]hseq.Type[T]),
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
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}

func (codec Codec[T]) Encode(entity T) *s3manager.UploadInput {
	req := &s3manager.UploadInput{}
	val := reflect.ValueOf(entity)

	codec.encodeCacheControl(val, req)
	codec.encodeContentEncoding(val, req)
	codec.encodeContentLanguage(val, req)
	codec.encodeContentType(val, req)
	codec.encodeExpires(val, req)
	codec.encodeMetadata(val, req)

	req.Key = aws.String(codec.EncodeKey(entity))
	return req
}

func (codec Codec[T]) encodeCacheControl(entity reflect.Value, req *s3manager.UploadInput) {
	f, ok := codec.system["Cache-Control"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.CacheControl = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentEncoding(entity reflect.Value, req *s3manager.UploadInput) {
	f, ok := codec.system["Content-Encoding"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentEncoding = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentLanguage(entity reflect.Value, req *s3manager.UploadInput) {
	f, ok := codec.system["Content-Language"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentLanguage = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeContentType(entity reflect.Value, req *s3manager.UploadInput) {
	f, ok := codec.system["Content-Type"]
	if ok {
		if val := entity.FieldByIndex(f.Index).String(); val != "" {
			req.ContentType = aws.String(val)
		}
	}
}

func (codec Codec[T]) encodeExpires(entity reflect.Value, req *s3manager.UploadInput) {
	f, ok := codec.system["Expires"]
	if ok {
		t := entity.FieldByIndex(f.Index).Interface().(time.Time)
		req.Expires = &t
	}
}

func (codec Codec[T]) encodeMetadata(entity reflect.Value, req *s3manager.UploadInput) {
	if len(codec.metadata) > 0 {
		req.Metadata = map[string]*string{}
		for k, f := range codec.metadata {
			if val := entity.FieldByIndex(f.Index).String(); val != "" {
				req.Metadata[k] = aws.String(val)
			}
		}
	}
}

func (codec Codec[T]) Decode(obj *s3.GetObjectOutput) *T {
	val := new(T)
	gen := reflect.ValueOf(val).Elem()

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
			entity.FieldByIndex(f.Index).SetString(aws.StringValue(obj.CacheControl))
		}
	}
}

func (codec Codec[T]) decodeContentEncoding(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Encoding"]
	if ok {
		if obj.ContentEncoding != nil {
			entity.FieldByIndex(f.Index).SetString(aws.StringValue(obj.ContentEncoding))
		}
	}
}

func (codec Codec[T]) decodeContentLanguage(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Language"]
	if ok {
		if obj.ContentLanguage != nil {
			entity.FieldByIndex(f.Index).SetString(aws.StringValue(obj.ContentLanguage))
		}
	}
}

func (codec Codec[T]) decodeContentType(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Content-Type"]
	if ok {
		if obj.ContentType != nil {
			entity.FieldByIndex(f.Index).SetString(aws.StringValue(obj.ContentType))
		}
	}
}

func (codec Codec[T]) decodeExpires(entity reflect.Value, obj *s3.GetObjectOutput) {
	f, ok := codec.system["Expires"]
	if ok {
		if obj.Expires != nil {
			t, err := time.Parse(time.RFC1123, *obj.Expires)
			if err == nil {
				entity.FieldByIndex(f.Index).Set(reflect.ValueOf(t))
			}
		}
	}
}

func (codec Codec[T]) decodeMetadata(entity reflect.Value, obj *s3.GetObjectOutput) {
	if len(codec.metadata) > 0 {
		for k, f := range codec.metadata {
			val, exists := obj.Metadata[k]
			if exists {
				entity.FieldByIndex(f.Index).SetString(*val)
			}
		}
	}
}
