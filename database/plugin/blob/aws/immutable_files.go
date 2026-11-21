//go:build dingo_extra_plugins

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
)

const immutableObjectPrefix = "immutable/"

func (d *BlobStoreS3) ReadImmutableFile(
	ctx context.Context,
	name string,
	dst io.Writer,
) (int64, error) {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	out, err := d.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    new(d.fullKey(immutableObjectPrefix + name)),
	})
	if err != nil {
		if isS3NotFound(err) {
			return 0, fs.ErrNotExist
		}
		return 0, fmt.Errorf("get ImmutableDB object %q: %w", name, err)
	}
	defer out.Body.Close()
	n, err := io.Copy(dst, out.Body)
	if err != nil {
		return n, fmt.Errorf("read ImmutableDB object %q: %w", name, err)
	}
	return n, nil
}

func (d *BlobStoreS3) WriteImmutableFile(
	ctx context.Context,
	name string,
	src io.Reader,
	size int64,
) error {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	if size < 0 {
		return fmt.Errorf("invalid ImmutableDB object size %d", size)
	}
	_, err := d.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &d.bucket,
		Key:           new(d.fullKey(immutableObjectPrefix + name)),
		Body:          src,
		ContentLength: &size,
	})
	if err != nil {
		return fmt.Errorf("put ImmutableDB object %q: %w", name, err)
	}
	return nil
}

func (d *BlobStoreS3) ListImmutableFiles(
	ctx context.Context,
) ([]blob.ImmutableObject, error) {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	prefix := d.fullKey(immutableObjectPrefix)
	paginator := s3.NewListObjectsV2Paginator(d.client, &s3.ListObjectsV2Input{
		Bucket: &d.bucket,
		Prefix: &prefix,
	})
	var files []blob.ImmutableObject
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list ImmutableDB objects: %w", err)
		}
		for _, item := range page.Contents {
			if item.Key == nil {
				continue
			}
			encoded, ok := strings.CutPrefix(*item.Key, d.prefix)
			if !ok {
				return nil, fmt.Errorf(
					"ImmutableDB object outside prefix %q",
					*item.Key,
				)
			}
			raw, err := hex.DecodeString(encoded)
			if err != nil {
				return nil, fmt.Errorf(
					"decode ImmutableDB object key %q: %w",
					*item.Key,
					err,
				)
			}
			name, ok := strings.CutPrefix(string(raw), immutableObjectPrefix)
			if !ok || name == "" || item.Size == nil {
				continue
			}
			files = append(
				files,
				blob.ImmutableObject{Name: name, Size: *item.Size},
			)
		}
	}
	return files, nil
}

func (d *BlobStoreS3) DeleteImmutableFile(
	ctx context.Context,
	name string,
) error {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	_, err := d.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &d.bucket,
		Key:    new(d.fullKey(immutableObjectPrefix + name)),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("delete ImmutableDB object %q: %w", name, err)
	}
	return nil
}

func (d *BlobStoreS3) isImmutableObject(fullObjectKey string) bool {
	encoded := strings.TrimPrefix(fullObjectKey, d.prefix)
	raw, err := hex.DecodeString(encoded)
	return err == nil && strings.HasPrefix(string(raw), immutableObjectPrefix)
}

func (d *BlobStoreS3) ResetImmutableFiles(ctx context.Context) error {
	files, err := d.ListImmutableFiles(ctx)
	if err != nil {
		return err
	}
	var resetErr error
	for _, file := range files {
		if err := d.DeleteImmutableFile(ctx, file.Name); err != nil {
			resetErr = errors.Join(resetErr, err)
		}
	}
	return resetErr
}

var _ blob.ImmutableObjects = (*BlobStoreS3)(nil)
