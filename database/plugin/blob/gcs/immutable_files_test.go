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
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestGCSImmutableFilesRemainOutsideBlobBackupAndReset(t *testing.T) {
	t.Parallel()
	store, _ := storeOnFakeCloud(t)
	ctx := context.Background()
	want := []byte("durable chunk bytes")
	require.NoError(
		t,
		store.WriteImmutableFile(
			ctx,
			"00001.chunk",
			bytes.NewReader(want),
			int64(len(want)),
		),
	)
	files, err := store.ListImmutableFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "00001.chunk", files[0].Name)
	var out bytes.Buffer
	n, err := store.ReadImmutableFile(ctx, files[0].Name, &out)
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), n)
	require.Equal(t, want, out.Bytes())
	txn := store.NewTransaction(false)
	defer txn.Rollback()
	it := store.NewIterator(txn, types.BlobIteratorOptions{})
	it.Rewind()
	require.False(
		t,
		it.Valid(),
		"ImmutableDB objects must not appear in blob iteration",
	)
	it.Close()
	var backup bytes.Buffer
	require.NoError(t, store.Backup(ctx, &backup))
	require.NotContains(t, backup.String(), string(want))
	require.NoError(t, store.Reset(ctx))
	out.Reset()
	_, err = store.ReadImmutableFile(ctx, "00001.chunk", &out)
	require.NoError(
		t,
		err,
		"blob reset must retain separately managed ImmutableDB files",
	)
	require.Equal(t, want, out.Bytes())
	require.NoError(t, store.ResetImmutableFiles(ctx))
	files, err = store.ListImmutableFiles(ctx)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestGCSFailedImmutableUploadPreservesDurableObject(t *testing.T) {
	t.Parallel()
	store, _ := storeOnFakeCloud(t)
	ctx := context.Background()
	want := []byte("durable chunk bytes")
	require.NoError(
		t,
		store.WriteImmutableFile(
			ctx,
			"00001.chunk",
			bytes.NewReader(want),
			int64(len(want)),
		),
	)
	require.Error(
		t,
		store.WriteImmutableFile(
			ctx,
			"00001.chunk",
			bytes.NewReader([]byte("bad")),
			10,
		),
	)
	var out bytes.Buffer
	_, err := store.ReadImmutableFile(ctx, "00001.chunk", &out)
	require.NoError(t, err)
	require.Equal(t, want, out.Bytes())
}
