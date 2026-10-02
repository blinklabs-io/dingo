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

package mithril

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("source failed")
}

// requireArtifactStoreContract exercises the behavior every ArtifactStore
// backend must share, so backend selection cannot change what the producer
// and the server observe. store must be empty.
func requireArtifactStoreContract(t *testing.T, store ArtifactStore) {
	t.Helper()
	ctx := context.Background()
	put := func(key, body string) {
		require.NoError(t, store.Put(ctx, key, strings.NewReader(body)), key)
	}

	put("aaa/one.bin", "0123456789")
	put("aaa/two.bin", "two")
	put("bbb/one.bin", "other")
	put("root.json", "{}")
	put("aaa/one.bin", "0123456789abcdef") // replaces

	r, err := store.Open(ctx, "aaa/one.bin")
	require.NoError(t, err)
	end, err := r.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(16), end)
	_, err = r.Seek(10, io.SeekStart)
	require.NoError(t, err)
	tail, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "abcdef", string(tail))
	_, err = r.Seek(2, io.SeekStart)
	require.NoError(t, err)
	mid := make([]byte, 3)
	_, err = io.ReadFull(r, mid)
	require.NoError(t, err)
	assert.Equal(t, "234", string(mid))
	require.NoError(t, r.Close())

	// A write that fails part way leaves the previous object intact.
	require.Error(t, store.Put(
		ctx, "aaa/one.bin",
		io.MultiReader(strings.NewReader("part"), failingReader{}),
	))
	r, err = store.Open(ctx, "aaa/one.bin")
	require.NoError(t, err)
	whole, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, "0123456789abcdef", string(whole))

	_, err = store.Open(ctx, "aaa/absent.bin")
	require.ErrorIs(t, err, ErrArtifactNotFound)
	_, err = store.Open(ctx, "aaa")
	require.ErrorIs(t, err, ErrArtifactNotFound)

	dirs, err := store.Subdirs(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"aaa", "bbb"}, dirs)
	dirs, err = store.Subdirs(ctx, "missing")
	require.NoError(t, err)
	assert.Empty(t, dirs)

	// A full key removes just that object; a directory removes its subtree
	// and not a sibling sharing the name prefix.
	put("aaa2/keep.bin", "keep")
	require.NoError(t, store.DeletePrefix(ctx, "aaa/two.bin"))
	_, err = store.Open(ctx, "aaa/two.bin")
	require.ErrorIs(t, err, ErrArtifactNotFound)
	r, err = store.Open(ctx, "aaa/one.bin")
	require.NoError(t, err)
	require.NoError(t, r.Close())

	require.NoError(t, store.DeletePrefix(ctx, "aaa"))
	_, err = store.Open(ctx, "aaa/one.bin")
	require.ErrorIs(t, err, ErrArtifactNotFound)
	for _, key := range []string{"aaa2/keep.bin", "bbb/one.bin", "root.json"} {
		r, err := store.Open(ctx, key)
		require.NoError(t, err, key)
		require.NoError(t, r.Close())
	}
	require.NoError(t, store.DeletePrefix(ctx, "never/existed"))

	for _, bad := range []string{"", "../escape", "/abs", "a//b", "a/../b"} {
		require.Error(t, store.Put(ctx, bad, strings.NewReader("x")), bad)
		_, err := store.Open(ctx, bad)
		require.Error(t, err, bad)
	}
	require.Error(t, store.DeletePrefix(ctx, ""))
}

func TestLocalArtifactStoreContract(t *testing.T) {
	t.Parallel()

	store, _ := newLocalStore(t)
	requireArtifactStoreContract(t, store)
}

func TestOpenArtifactStoreRejectsEmptyLocation(t *testing.T) {
	t.Parallel()

	_, err := OpenArtifactStore(context.Background(), "")
	require.Error(t, err)
}
