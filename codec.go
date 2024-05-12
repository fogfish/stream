package stream

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fogfish/golem/hseq"
	"github.com/fogfish/golem/optics"
)

type codec[T any] struct {
	h optics.Isomorphism[T, s3.HeadObjectOutput]
	w optics.Isomorphism[T, s3.PutObjectInput]
	r optics.Isomorphism[T, s3.GetObjectOutput]
	s optics.Lens[T, string]
}

func newCodec[T any]() *codec[T] {
	c := &codec[T]{
		h: isomorphism[T, s3.HeadObjectOutput](),
		w: isomorphism[T, s3.PutObjectInput](),
		r: isomorphism[T, s3.GetObjectOutput](),
	}

	ts := hseq.New[T]()
	if t, has := hseq.ForNameMaybe(ts, "PreSignedUrl"); has {
		c.s = optics.NewLens[T, string](t)
	}

	return c
}

func (c *codec[T]) DecodeHeadOutput(s *s3.HeadObjectOutput, t *T) { c.h.Inverse(s, t) }
func (c *codec[T]) EncodePutInput(t *T, s *s3.PutObjectInput)     { c.w.Forward(t, s) }
func (c *codec[T]) DecodeGetOutput(s *s3.GetObjectOutput, t *T)   { c.r.Inverse(s, t) }

// codec for category S to T
func isomorphism[T, S any]() optics.Isomorphism[T, S] {
	ts := hseq.New[T]()
	sq := hseq.New[S]()

	iso := []optics.Isomorphism[T, S]{}
	for _, t := range ts {
		switch t.FieldKey() {
		case "CacheControl":
			iso = append(iso, codecString(ts, sq, "CacheControl"))
		case "ContentEncoding":
			iso = append(iso, codecString(ts, sq, "ContentEncoding"))
		case "ContentLanguage":
			iso = append(iso, codecString(ts, sq, "ContentLanguage"))
		case "ContentType":
			iso = append(iso, codecString(ts, sq, "ContentType"))
		case "Expires":
			iso = append(iso, codecTime(ts, sq, "Expires"))
		case "ETag":
			iso = append(iso, codecString(ts, sq, "ETag"))
		case "LastModified":
			iso = append(iso, codecTime(ts, sq, "LastModified"))
		case "StorageClass":
			iso = append(iso, codecStorageClass(ts, sq, "StorageClass"))
		case "PreSignedUrl":
		default:
			iso = append(iso, codecMetadata(t, sq))
		}
	}

	return optics.Morphism(iso...)
}

func codecString[T, S any](ts hseq.Seq[T], sq hseq.Seq[S], attr string) optics.Isomorphism[T, S] {
	t, has := hseq.ForNameMaybe(ts, attr)
	if !has {
		return nil
	}

	s, has := hseq.ForNameMaybe(sq, attr)
	if !has {
		return nil
	}

	dec := optics.BiMap(
		optics.NewLens[S, *string](s),
		aws.ToString,
		aws.String,
	)
	enc := optics.NewLens[T, string](t)
	return optics.Iso(enc, dec)
}

func codecStorageClass[T, S any](ts hseq.Seq[T], sq hseq.Seq[S], attr string) optics.Isomorphism[T, S] {
	t, has := hseq.ForNameMaybe(ts, attr)
	if !has {
		return nil
	}

	s, has := hseq.ForNameMaybe(sq, attr)
	if !has {
		return nil
	}

	dec := optics.BiMap(
		optics.NewLens[S, types.StorageClass](s),
		func(x types.StorageClass) string { return string(x) },
		func(x string) types.StorageClass { return types.StorageClass(x) },
	)
	enc := optics.NewLens[T, string](t)
	return optics.Iso(enc, dec)
}

func codecTime[T, S any](ts hseq.Seq[T], sq hseq.Seq[S], attr string) optics.Isomorphism[T, S] {
	t, has := hseq.ForNameMaybe(ts, attr)
	if !has {
		return nil
	}

	s, has := hseq.ForNameMaybe(sq, attr)
	if !has {
		return nil
	}

	dec := optics.NewLens[S, *time.Time](s)
	enc := optics.NewLens[T, *time.Time](t)
	return optics.Iso(enc, dec)
}

func codecMetadata[T, S any](t hseq.Type[T], sq hseq.Seq[S]) optics.Isomorphism[T, S] {
	attr := strings.Split(t.StructField.Tag.Get("hseq"), ",")[0]
	if attr == "" {
		attr = t.Name
	}

	s, has := hseq.ForNameMaybe(sq, "Metadata")
	if !has {
		return nil
	}

	dec := optics.Join(
		optics.NewLens[S, map[string]string](s),
		optics.NewLensM[map[string]string](strings.ToLower(attr)),
	)
	enc := optics.NewLens[T, string](t)
	return optics.Iso(enc, dec)
}
