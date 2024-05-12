//
// Copyright (C) 2024 Dmitry Kolesnikov
//
// This file may be modified and distributed under the terms
// of the MIT license.  See the LICENSE file for details.
// https://github.com/fogfish/stream
//

package mocks

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/fogfish/stream"
)

type Mock[T any] struct {
	stream.S3
	stream.S3Upload
	stream.S3Signer
	Delay     *time.Duration
	ExpectKey string
	ExpectVal string
	ReturnVal *T
	ReturnErr error
}

func (mock Mock[T]) Assert(ctx context.Context, inputKey *string) error {
	key := aws.ToString(inputKey)
	if key != mock.ExpectKey {
		return fmt.Errorf("expected key %s, got %s", mock.ExpectKey, key)
	}

	if mock.Delay != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*mock.Delay):
		}
	}

	return nil
}

//

type HeadObject struct{ Mock[s3.HeadObjectOutput] }

func (mock HeadObject) HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if err := mock.Assert(ctx, input.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	if mock.ReturnVal == nil {
		return nil, &types.NoSuchKey{}
	}

	return mock.ReturnVal, nil
}

//

type GetObject struct{ Mock[s3.GetObjectOutput] }

func (mock GetObject) GetObject(ctx context.Context, input *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := mock.Assert(ctx, input.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	if mock.ReturnVal == nil {
		return nil, &types.NoSuchKey{}
	}

	return mock.ReturnVal, nil
}

//

type ListObject struct{ Mock[s3.ListObjectsV2Output] }

func (mock ListObject) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := mock.Assert(ctx, params.Prefix); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}

//

type DeleteObject struct{ Mock[s3.DeleteObjectOutput] }

func (mock DeleteObject) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := mock.Assert(ctx, params.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}

//

type CopyObject struct{ Mock[s3.CopyObjectOutput] }

func (mock CopyObject) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if err := mock.Assert(ctx, params.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}

//

type PutObject struct{ Mock[manager.UploadOutput] }

func (mock PutObject) Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	if err := mock.Assert(ctx, input.Key); err != nil {
		return nil, err
	}

	buf, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	if string(buf) != mock.ExpectVal {
		return nil, fmt.Errorf("expected val %s, got %s", mock.ExpectVal, string(buf))
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}

//

type PresignGetObject struct{ Mock[v4.PresignedHTTPRequest] }

func (mock PresignGetObject) PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	if err := mock.Assert(ctx, params.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}

//

type PresignPutObject struct{ Mock[v4.PresignedHTTPRequest] }

func (mock PresignPutObject) PresignPutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	if err := mock.Assert(ctx, params.Key); err != nil {
		return nil, err
	}

	if mock.ReturnErr != nil {
		return nil, mock.ReturnErr
	}

	return mock.ReturnVal, nil
}
