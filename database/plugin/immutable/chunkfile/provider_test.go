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

package chunkfile_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	immplugin "github.com/blinklabs-io/dingo/database/plugin/immutable"
	"github.com/blinklabs-io/dingo/database/plugin/immutable/chunkfile"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestEphemeralStoreUsesAndRemovesItsOwnDirectory(t *testing.T) {
	t.Parallel()
	host, svc := resolve(t, "")
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	require.True(t, filepath.IsAbs(svc.Dir()), "an ephemeral store must not write into the working directory")
	st, err := svc.Open(testGeometry)
	require.NoError(t, err)
	loc, err := st.Append(block(3))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	_, body, err := svc.ReadBlockAt(loc)
	require.NoError(t, err)
	require.Equal(t, blockBody(3), body)
	dir := svc.Dir()
	require.NoError(t, host.Stop(context.Background()))
	_, err = os.Stat(dir)
	require.ErrorIs(t, err, os.ErrNotExist)
}

var testGeometry = immutable.Geometry{ChunkSlots: 100}

func resolve(
	t *testing.T,
	dataDir string,
) (*hostplugin.Host, immplugin.ImmutableDB) {
	t.Helper()
	host := hostplugin.NewHost()
	require.NoError(t, chunkfile.RegisterProvider(host))
	svc, err := hostplugin.Resolve[immplugin.ImmutableDB](
		context.Background(), host,
		hostplugin.CapabilityStorageImmutable, chunkfile.ProviderName, nil,
		immplugin.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	return host, svc
}

// blockBody is a two-element CBOR array: a 3-byte header and a 4-byte body.
func blockBody(slot uint64) []byte {
	return []byte{
		0x82, 0x43, byte(slot), 1, 2, 0x44, 4, 5, 6, byte(slot),
	}
}

func block(slot uint64) immutable.WriteBlock {
	hash := make([]byte, 32)
	hash[0] = byte(slot)
	return immutable.WriteBlock{
		Hash: hash, Cbor: blockBody(slot), Type: 6, Slot: slot,
	}
}

func TestDescriptorIsDurableStorageImmutable(t *testing.T) {
	t.Parallel()
	host := hostplugin.NewHost()
	require.NoError(t, chunkfile.RegisterProvider(host))
	require.Equal(t, []hostplugin.Descriptor{{
		Capability:   hostplugin.CapabilityStorageImmutable,
		Name:         "chunkfile",
		Description:  "cardano-node compatible chunk files",
		DurableClose: true,
	}}, host.Providers())
	require.ErrorContains(
		t, chunkfile.RegisterProvider(host), "already registered",
	)
}

func TestServiceRoundTrip(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	host, svc := resolve(t, dataDir)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	require.Equal(t, filepath.Join(dataDir, "immutable"), svc.Dir())

	st, err := svc.Open(testGeometry)
	require.NoError(t, err)
	_, hasTip := st.Tip()
	require.False(t, hasTip)
	loc, err := st.Append(block(3))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	tip, hasTip := st.Tip()
	require.True(t, hasTip)
	require.Equal(t, uint64(3), tip.Slot)

	typ, inner, err := svc.ReadBlockAt(loc)
	require.NoError(t, err)
	require.Equal(t, uint(6), typ)
	require.Equal(t, blockBody(3), inner)
	files, err := svc.ListFiles()
	require.NoError(t, err)
	require.Len(t, files, 3)

	olderLoc, err := st.Append(block(103))
	require.NoError(t, err)
	require.NoError(t, st.Sync())
	require.Equal(t, uint64(1), olderLoc.Chunk)

	_, err = svc.Open(testGeometry)
	require.ErrorIs(t, err, chunkfile.ErrStoreOpen)
	require.NoError(t, st.Close())
	st2, err := svc.Open(testGeometry)
	require.NoError(t, err)
	tip, hasTip = st2.Tip()
	require.True(t, hasTip)
	require.Equal(t, uint64(103), tip.Slot)
}

// Stop must close a writer its holder left open, and leave the provider
// refusing a new one.
func TestStopClosesOpenWriterToCompletion(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	host, svc := resolve(t, dataDir)
	st, err := svc.Open(testGeometry)
	require.NoError(t, err)
	_, err = st.Append(block(1))
	require.NoError(t, err)

	require.NoError(t, host.Stop(context.Background()))

	_, err = st.Append(block(2))
	require.ErrorContains(t, err, "closed")
	_, err = svc.Open(testGeometry)
	require.ErrorIs(t, err, chunkfile.ErrClosed)

	reopened, err := immutable.OpenStore(svc.Dir(), testGeometry)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	tip, ok := reopened.Tip()
	require.True(t, ok)
	require.Equal(t, uint64(1), tip.Slot)
}

// TestMithrilCompressionArchivesCompletedChunks configures mithril
// compression: the chunk the store leaves is archived, still read, and
// listed, while the open chunk stays plain.
func TestMithrilCompressionArchivesCompletedChunks(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	host := hostplugin.NewHost()
	require.NoError(t, chunkfile.RegisterProvider(host))
	svc, err := hostplugin.Resolve[immplugin.ImmutableDB](
		context.Background(), host,
		hostplugin.CapabilityStorageImmutable, chunkfile.ProviderName,
		map[string]any{"compression": chunkfile.CompressionMithril},
		immplugin.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })

	st, err := svc.Open(testGeometry)
	require.NoError(t, err)
	loc, err := st.Append(block(3))
	require.NoError(t, err)
	_, err = st.Append(block(103))
	require.NoError(t, err)
	require.NoError(t, st.Sync())

	typ, inner, err := svc.ReadBlockAt(loc)
	require.NoError(t, err)
	require.Equal(t, uint(6), typ)
	require.Equal(t, blockBody(3), inner)
	files, err := svc.ListFiles()
	require.NoError(t, err)
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name)
	}
	require.Contains(t, names, immutable.ArchiveName(0))
	require.NotContains(t, names, "00000.chunk")
	require.Contains(t, names, "00001.chunk")
}

func TestInvalidCompressionIsRefused(t *testing.T) {
	t.Parallel()
	host := hostplugin.NewHost()
	require.NoError(t, chunkfile.RegisterProvider(host))
	_, err := hostplugin.Resolve[immplugin.ImmutableDB](
		context.Background(), host,
		hostplugin.CapabilityStorageImmutable, chunkfile.ProviderName,
		map[string]any{"compression": "gzip"},
		immplugin.ProviderDependencies{DataDir: t.TempDir()},
	)
	require.ErrorContains(t, err, "invalid ImmutableDB compression")
}
