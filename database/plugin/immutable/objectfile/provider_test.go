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

package objectfile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

type memoryObjects struct {
	mu         sync.Mutex
	files      map[string][]byte
	failDelete string
	failWrite  string
	readCounts map[string]int
}

func newMemoryObjects() *memoryObjects {
	return &memoryObjects{
		files: make(map[string][]byte), readCounts: make(map[string]int),
	}
}

func (m *memoryObjects) ReadImmutableFile(
	_ context.Context, name string, dst io.Writer,
) (int64, error) {
	m.mu.Lock()
	data, ok := m.files[name]
	data = bytes.Clone(data)
	m.readCounts[name]++
	m.mu.Unlock()
	if !ok {
		return 0, fs.ErrNotExist
	}
	n, err := dst.Write(data)
	return int64(n), err
}

func (m *memoryObjects) WriteImmutableFile(
	_ context.Context, name string, src io.Reader, size int64,
) error {
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return errors.New("unexpected write size")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failWrite == name {
		m.failWrite = ""
		return errors.New("injected write failure")
	}
	m.files[name] = data
	return nil
}

func (m *memoryObjects) ListImmutableFiles(
	_ context.Context,
) ([]blob.ImmutableObject, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make([]blob.ImmutableObject, 0, len(m.files))
	for name, data := range m.files {
		files = append(
			files,
			blob.ImmutableObject{Name: name, Size: int64(len(data))},
		)
	}
	sort.Slice(
		files,
		func(i, j int) bool { return files[i].Name < files[j].Name },
	)
	return files, nil
}

func (m *memoryObjects) DeleteImmutableFile(
	_ context.Context, name string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failDelete == name {
		m.failDelete = ""
		return errors.New("injected delete failure")
	}
	delete(m.files, name)
	return nil
}

func (m *memoryObjects) ResetImmutableFiles(ctx context.Context) error {
	files, err := m.ListImmutableFiles(ctx)
	if err != nil {
		return err
	}
	var resetErr error
	for _, file := range files {
		resetErr = errors.Join(resetErr, m.DeleteImmutableFile(ctx, file.Name))
	}
	return resetErr
}

func resolveObjectStore(
	t *testing.T,
	dataDir string,
	objects blob.ImmutableObjects,
) (*hostplugin.Host, immplugin.ImmutableDB) {
	t.Helper()
	host := hostplugin.NewHost()
	require.NoError(t, RegisterProvider(host, immplugin.S3ProviderName))
	svc, err := hostplugin.Resolve[immplugin.ImmutableDB](
		context.Background(), host,
		hostplugin.CapabilityStorageImmutable,
		immplugin.S3ProviderName, nil,
		immplugin.ProviderDependencies{DataDir: dataDir, ObjectFiles: objects},
	)
	require.NoError(t, err)
	return host, svc
}

func objectBlock(slot uint64) immutable.WriteBlock {
	hash := make([]byte, 32)
	hash[0] = byte(slot)
	return immutable.WriteBlock{
		Hash: hash,
		Cbor: []byte{0x82, 0x43, byte(slot), 1, 2, 0x44, 4, 5, 6, byte(slot)},
		Type: 6,
		Slot: slot,
	}
}

func TestRemoteStoreRoundTripAndTruncate(t *testing.T) {
	t.Parallel()
	objects := newMemoryObjects()
	host, svc := resolveObjectStore(t, t.TempDir(), objects)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })

	geom := immutable.Geometry{ChunkSlots: 100}
	st, err := svc.Open(geom)
	require.NoError(t, err)
	first := objectBlock(3)
	firstLoc, err := st.Append(first)
	require.NoError(t, err)
	_, err = st.Append(objectBlock(103))
	require.NoError(t, err)
	require.NoError(t, st.Sync())

	files, err := svc.ListFiles()
	require.NoError(t, err)
	require.Len(t, files, 6)
	localFiles, err := immutable.ListFiles(svc.Dir())
	require.NoError(t, err)
	require.Len(t, localFiles, 3)
	for _, file := range localFiles {
		require.Equal(t, uint64(1), file.Chunk)
	}
	typ, body, err := svc.ReadBlockAt(firstLoc)
	require.NoError(t, err)
	require.Equal(t, uint(6), typ)
	require.Equal(t, first.Cbor, body)
	block, foundLoc, err := st.GetBlock(first.Slot, first.Hash)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Equal(t, first.Cbor, block.Cbor)
	require.Equal(t, firstLoc, *foundLoc)
	missing, _, err := st.GetBlock(first.Slot+1, first.Hash)
	require.NoError(t, err)
	require.Nil(t, missing)
	objects.mu.Lock()
	readsAfterFirst := maps.Clone(objects.readCounts)
	objects.mu.Unlock()
	for range 10 {
		typ, body, err = svc.ReadBlockAt(firstLoc)
		require.NoError(t, err)
		require.Equal(t, uint(6), typ)
		require.Equal(t, first.Cbor, body)
	}
	objects.mu.Lock()
	require.Equal(t, readsAfterFirst, objects.readCounts)
	objects.mu.Unlock()
	require.NoError(t, st.Close())

	st, err = svc.Open(geom)
	require.NoError(t, err)
	tip, ok := st.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(103), tip.Slot)
	require.NoError(t, st.TruncateAfter(first.Slot, first.Hash))
	files, err = svc.ListFiles()
	require.NoError(t, err)
	require.Len(t, files, 3)
	for _, file := range files {
		require.Zero(t, file.Chunk)
	}
	require.NoError(t, st.Close())
}

func TestRestartAfterIncompleteNextChunkPublication(t *testing.T) {
	t.Parallel()
	for _, fail := range []string{"00001.secondary", "00001.primary"} {
		t.Run(fail, func(t *testing.T) {
			objects := newMemoryObjects()
			dir := t.TempDir()
			host, svc := resolveObjectStore(t, dir, objects)
			t.Cleanup(
				func() { require.NoError(t, host.Stop(context.Background())) },
			)
			geom := immutable.Geometry{ChunkSlots: 100}
			st, err := svc.Open(geom)
			require.NoError(t, err)
			first := objectBlock(3)
			loc, err := st.Append(first)
			require.NoError(t, err)
			require.NoError(t, st.Sync())
			_, err = st.Append(objectBlock(103))
			require.NoError(t, err)
			objects.failWrite = fail
			require.ErrorContains(t, st.Sync(), "injected write failure")
			require.NoError(t, st.Close())
			st, err = svc.Open(geom)
			require.NoError(t, err)
			tip, ok := st.Tip()
			require.True(
				t,
				ok,
				"the durable previous chunk must survive a partial next-chunk upload",
			)
			require.Equal(t, first.Slot, tip.Slot)
			_, body, err := svc.ReadBlockAt(loc)
			require.NoError(t, err)
			require.Equal(t, first.Cbor, body)
			require.NoError(
				t,
				st.Sync(),
				"recovery must not retain dirty entries for discarded chunks",
			)
			_, err = st.Append(objectBlock(103))
			require.NoError(t, err)
			require.NoError(t, st.Sync())
			require.NoError(t, st.Close())
		})
	}
}

func TestReopenPreservesActiveReadCache(t *testing.T) {
	t.Parallel()
	objects := newMemoryObjects()
	host, svc := resolveObjectStore(t, t.TempDir(), objects)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	geom := immutable.Geometry{ChunkSlots: 100}
	st, err := svc.Open(geom)
	require.NoError(t, err)
	first := objectBlock(3)
	loc, err := st.Append(first)
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	dir, release, err := svc.(*service).readChunkDir(loc.Chunk)
	require.NoError(t, err)
	defer release()
	require.NoError(t, st.Close())
	st, err = svc.Open(geom)
	require.NoError(t, err)
	_, err = os.Stat(dir)
	require.NoError(
		t,
		err,
		"reopening the writer must preserve a reader's leased files",
	)
	_, body, err := immutable.ReadBlockAt(dir, loc)
	require.NoError(t, err)
	require.Equal(t, first.Cbor, body)
	require.NoError(t, st.Close())
}

func TestPublishedChunkInvalidatesCachedPrefix(t *testing.T) {
	t.Parallel()
	objects := newMemoryObjects()
	host, svc := resolveObjectStore(t, t.TempDir(), objects)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	geom := immutable.Geometry{ChunkSlots: 100}
	st, err := svc.Open(geom)
	require.NoError(t, err)
	loc, err := st.Append(objectBlock(3))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	_, _, err = svc.ReadBlockAt(loc)
	require.NoError(t, err)
	require.NoError(t, st.Close())
	staged, err := immutable.OpenStore(svc.Dir(), geom)
	require.NoError(t, err)
	next := objectBlock(4)
	nextLoc, err := staged.Append(next)
	require.NoError(t, err)
	require.NoError(t, staged.Sync())
	require.NoError(t, staged.Close())
	require.NoError(t, svc.(*service).PublishChunk(nextLoc.Chunk))
	_, got, err := svc.ReadBlockAt(nextLoc)
	require.NoError(t, err)
	require.Equal(t, next.Cbor, got)
}

func TestTruncateAllClearsWriterPublicationState(t *testing.T) {
	t.Parallel()
	objects := newMemoryObjects()
	host, svc := resolveObjectStore(t, t.TempDir(), objects)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	geom := immutable.Geometry{ChunkSlots: 100}
	st, err := svc.Open(geom)
	require.NoError(t, err)
	_, err = st.Append(objectBlock(103))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	require.NoError(t, st.Close())
	st, err = svc.Open(geom)
	require.NoError(t, err)
	require.NoError(t, st.TruncateAll())
	require.NoError(t, st.Sync())
	_, err = st.Append(objectBlock(3))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	files, err := svc.ListFiles()
	require.NoError(t, err)
	require.Len(t, files, 3)
	for _, f := range files {
		require.Zero(t, f.Chunk)
	}
	require.NoError(t, st.Close())
}

func TestEphemeralStagingSurvivesStopUntilReaderRelease(t *testing.T) {
	t.Parallel()
	host, svc := resolveObjectStore(t, "", newMemoryObjects())
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	require.True(t, filepath.IsAbs(svc.Dir()))
	st, err := svc.Open(immutable.Geometry{ChunkSlots: 100})
	require.NoError(t, err)
	block := objectBlock(3)
	loc, err := st.Append(block)
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	dir, release, err := svc.(*service).readChunkDir(loc.Chunk)
	require.NoError(t, err)
	release = sync.OnceFunc(release)
	t.Cleanup(release)
	require.NoError(t, host.Stop(context.Background()))
	_, body, err := immutable.ReadBlockAt(dir, loc)
	require.NoError(t, err)
	require.Equal(t, block.Cbor, body)
	release()
	require.NoDirExists(t, svc.Dir())
}

// Only a staging directory the provider created for itself is removed on
// stop. Under a data directory the staged tail and the adoption record are
// the node's own and must survive.
func TestStopKeepsDataDirectoryStaging(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	host, svc := resolveObjectStore(t, dataDir, newMemoryObjects())
	require.Equal(t, filepath.Join(dataDir, immplugin.DirName), svc.Dir())
	st, err := svc.Open(immutable.Geometry{ChunkSlots: 100})
	require.NoError(t, err)
	_, err = st.Append(objectBlock(3))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	record := filepath.Join(svc.Dir(), "adopting.pending")
	require.NoError(t, os.WriteFile(record, []byte("0\n"), 0o600))
	require.NoError(t, host.Stop(context.Background()))
	require.FileExists(t, record)
	local, err := immutable.ListFiles(svc.Dir())
	require.NoError(t, err)
	require.NotEmpty(t, local, "the staged tail must survive stop")
}

// Chunks the writer skips still get empty files locally, as in cardano-node's
// layout; the remote set must carry them too, or a snapshot or a node reading
// the bucket sees a gap.
func TestSkippedChunksArePublished(t *testing.T) {
	t.Parallel()
	geom := immutable.Geometry{ChunkSlots: 100}
	local, err := immutable.OpenStore(t.TempDir(), geom)
	require.NoError(t, err)
	host, svc := resolveObjectStore(t, t.TempDir(), newMemoryObjects())
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	st, err := svc.Open(geom)
	require.NoError(t, err)
	for _, slot := range []uint64{3, 350} {
		_, err = local.Append(objectBlock(slot))
		require.NoError(t, err)
		_, err = st.Append(objectBlock(slot))
		require.NoError(t, err)
	}
	require.NoError(t, local.Sync())
	require.NoError(t, st.Sync())
	want, err := immutable.ListFiles(local.Dir())
	require.NoError(t, err)
	got, err := svc.ListFiles()
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, local.Close())
	require.NoError(t, st.Close())
}
