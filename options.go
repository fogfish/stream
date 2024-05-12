package stream

import "time"

type Option func(*Opts)

func WithS3(api S3) Option {
	return func(sfs *Opts) {
		sfs.api = api
	}
}

func WithS3Upload(api S3Upload) Option {
	return func(sfs *Opts) {
		sfs.upload = api
	}
}

func WithS3Signer(api S3Signer) Option {
	return func(sfs *Opts) {
		sfs.signer = api
	}
}

func WithIOTimeout(t time.Duration) Option {
	return func(sfs *Opts) {
		sfs.timeout = t
	}
}

func WithPreSignUrlTTL(t time.Duration) Option {
	return func(sfs *Opts) {
		sfs.ttlSignedUrl = t
	}
}

func WithListingLimit(limit int32) Option {
	return func(sfs *Opts) {
		sfs.lslimit = limit
	}
}
