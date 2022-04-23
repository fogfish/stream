package s3

import "github.com/fogfish/stream"

type Codec struct{}

//
func (codec Codec) EncodeKey(key stream.Thing) string {
	hkey := key.HashKey()
	skey := key.SortKey()

	if skey == "" {
		return hkey
	}

	return hkey + "/_/" + skey
}
