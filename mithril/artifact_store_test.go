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
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	for _, bad := range []string{"", ".", "..", "../escape", "a/../b"} {
		require.Error(t, store.DeletePrefix(ctx, bad), bad)
	}
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

func TestOpenArtifactStoreRejectsRemoteURIWithoutBucket(t *testing.T) {
	t.Parallel()

	for _, location := range []string{"s3:///tmp/artifacts", "gcs:///tmp/artifacts"} {
		_, err := OpenArtifactStore(context.Background(), location)
		require.ErrorContains(t, err, "requires a bucket", location)
	}
	for _, location := range []string{"s3://%zz", "gcs://%zz"} {
		_, err := OpenArtifactStore(context.Background(), location)
		require.ErrorContains(t, err, "invalid", location)
		require.NotContains(t, err.Error(), location)
	}
}

type stagedReader struct {
	first   string
	rest    string
	started chan struct{}
	resume  chan struct{}
	once    sync.Once
	stage   int
}

func (r *stagedReader) Read(p []byte) (int, error) {
	switch r.stage {
	case 0:
		r.stage++
		r.once.Do(func() { close(r.started) })
		return copy(p, r.first), nil
	case 1:
		<-r.resume
		r.stage++
		return copy(p, r.rest), nil
	default:
		return 0, io.EOF
	}
}

func TestLocalArtifactStoreConcurrentPutPublishesWholeObject(t *testing.T) {
	t.Parallel()

	store, dir := newLocalStore(t)
	first := &stagedReader{
		first: "first-", rest: "complete-value",
		started: make(chan struct{}), resume: make(chan struct{}),
	}
	firstErr := make(chan error, 1)
	go func() {
		firstErr <- store.Put(t.Context(), "same.bin", first)
	}()
	<-first.started
	require.NoError(t, store.Put(
		t.Context(), "same.bin", strings.NewReader("second-complete-value"),
	))
	close(first.resume)
	require.NoError(t, <-firstErr)
	data, err := os.ReadFile(filepath.Join(dir, "same.bin"))
	require.NoError(t, err)
	require.Contains(t, []string{
		"first-complete-value", "second-complete-value",
	}, string(data))
}
