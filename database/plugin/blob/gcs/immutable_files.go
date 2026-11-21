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

package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"google.golang.org/api/iterator"
)

const immutableObjectPrefix = "immutable/"

func (d *BlobStoreGCS) ReadImmutableFile(
	ctx context.Context,
	name string,
	dst io.Writer,
) (int64, error) {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	r, err := d.object([]byte(immutableObjectPrefix + name)).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return 0, fs.ErrNotExist
	}
	if err != nil {
		return 0, fmt.Errorf("open ImmutableDB object %q: %w", name, err)
	}
	defer r.Close()
	n, err := io.Copy(dst, r)
	if err != nil {
		return n, fmt.Errorf("read ImmutableDB object %q: %w", name, err)
	}
	return n, nil
}

func (d *BlobStoreGCS) WriteImmutableFile(
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
	w := d.object([]byte(immutableObjectPrefix + name)).NewWriter(ctx)
	n, err := io.Copy(w, src)
	if err != nil {
		// Closing flushes the buffered prefix; cancellation aborts it instead.
		cancel()
		return fmt.Errorf("write ImmutableDB object %q: %w", name, err)
	}
	if n != size {
		cancel()
		return fmt.Errorf(
			"write ImmutableDB object %q: copied %d bytes, expected %d",
			name,
			n,
			size,
		)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("close ImmutableDB object %q: %w", name, err)
	}
	return nil
}

func (d *BlobStoreGCS) ListImmutableFiles(
	ctx context.Context,
) ([]blob.ImmutableObject, error) {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	iter := d.objects(ctx, []byte(immutableObjectPrefix))
	var files []blob.ImmutableObject
	for {
		attrs, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list ImmutableDB objects: %w", err)
		}
		name, err := d.externalKey(attrs.Name)
		if err != nil {
			return nil, err
		}
		if after, ok := strings.CutPrefix(name, immutableObjectPrefix); ok &&
			after != "" {
			files = append(
				files,
				blob.ImmutableObject{Name: after, Size: attrs.Size},
			)
		}
	}
	return files, nil
}

func (d *BlobStoreGCS) DeleteImmutableFile(
	ctx context.Context,
	name string,
) error {
	ctx, cancel := d.opContextFrom(ctx)
	defer cancel()
	err := d.object([]byte(immutableObjectPrefix + name)).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete ImmutableDB object %q: %w", name, err)
	}
	return nil
}

func (d *BlobStoreGCS) isImmutableObject(fullObjectKey string) bool {
	name, err := d.externalKey(fullObjectKey)
	return err == nil && strings.HasPrefix(name, immutableObjectPrefix)
}

func (d *BlobStoreGCS) ResetImmutableFiles(ctx context.Context) error {
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

var _ blob.ImmutableObjects = (*BlobStoreGCS)(nil)
