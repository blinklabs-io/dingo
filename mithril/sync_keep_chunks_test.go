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
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestSyncConfigKeepsChunks(t *testing.T) {
	t.Parallel()
	require.False(t, SyncConfig{}.keepsChunks(false))
	require.True(t, SyncConfig{KeepChunks: true}.keepsChunks(false))
	require.False(t, SyncConfig{KeepChunks: true}.keepsChunks(true),
		"a catch-up joins onto blocks already in the blob store")
	require.False(t, SyncConfig{
		KeepChunks: true, RepairLegacyRewardState: true,
	}.keepsChunks(false))
}

func TestSyncConfigImmutableConfigNeedsBadger(t *testing.T) {
	t.Parallel()
	cfg, err := SyncConfig{Network: "preprod"}.immutableConfig()
	require.NoError(t, err)
	require.False(t, cfg.Enabled)

	cfg, err = SyncConfig{
		Network:    "preprod",
		KeepChunks: true,
	}.immutableConfig()
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.Equal(t, uint64(21600), cfg.ChunkSlots)
	require.True(t, cfg.CanContainEBB)

	badConfig := SyncConfig{Network: "preprod", KeepChunks: true}
	badConfig.StoragePlugins.Blob.Provider = "s3"
	_, err = badConfig.immutableConfig()
	require.ErrorContains(t, err, "badger")
}

// layoutPrimaryIndex rewrites chunk 0's primary index of db as cardano-node
// lays it out for the network's geometry, which adoption requires: an offset
// for the epoch boundary slot, every relative slot and the final one. The
// shared fixture's primary lists only the occupied slots.
func layoutPrimaryIndex(t *testing.T, db, network string) {
	t.Helper()
	cfg, err := SyncConfig{Network: network, KeepChunks: true}.immutableConfig()
	require.NoError(t, err)
	secondary, err := os.ReadFile(
		filepath.Join(db, "immutable", "00000.secondary"),
	)
	require.NoError(t, err)
	const entrySize = 56
	slots := cfg.ChunkSlots + 1
	primary := []byte{1}
	next := 0
	end := uint32(0)
	for rel := range slots {
		primary = binary.BigEndian.AppendUint32(primary, end)
		if next*entrySize < len(secondary) &&
			binary.BigEndian.Uint64(secondary[next*entrySize+48:])%
				cfg.ChunkSlots+1 == rel {
			next++
			// #nosec G115 -- a few fixture entries
			end = uint32(next * entrySize)
		}
	}
	primary = binary.BigEndian.AppendUint32(primary, end)
	require.NoError(t, os.WriteFile(
		filepath.Join(db, "immutable", "00000.primary"), primary, 0o640,
	))
}

func syncedBlocks(
	t *testing.T,
	cfg SyncConfig,
) (*database.Database, []models.Block) {
	t.Helper()
	runtime, err := openDatabase(
		context.Background(), cfg, slog.New(slog.DiscardHandler), 1,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	blocks, err := database.BlocksRecent(
		context.Background(), runtime.Database, 10,
	)
	require.NoError(t, err)
	return runtime.Database, blocks
}

// Syncing one snapshot with and without keeping the chunk files gives the same
// blocks; with the flag the bodies stay in the extracted chunk files, which
// moved under the database's own directory.
func TestSyncKeepChunksMatchesBodyCopy(t *testing.T) {
	t.Parallel()
	db := newSyncableDB(t)
	layoutPrimaryIndex(t, db, "preprod")
	_, key := newSigningKey(t)
	store, _ := newLocalStore(t)
	_, err := CreateSnapshot(
		context.Background(), newSnapshotConfig(t, db, store, key),
	)
	require.NoError(t, err)
	srv := newMithrilTestServer(t, ServerConfig{Store: store})

	run := func(keep bool) SyncConfig {
		cfg := SyncConfig{
			Network:           "preprod",
			DataDir:           t.TempDir(),
			StorageMode:       "core",
			Backend:           BackendV2,
			AggregatorURL:     srv.URL,
			AllowInsecureHTTP: true,
			CleanupAfterLoad:  true,
			KeepChunks:        keep,
			Logger:            slog.New(slog.DiscardHandler),
		}
		result, err := Sync(context.Background(), cfg)
		require.NoError(t, err)
		require.Equal(t, uint64(1000), result.LedgerSlot)
		return cfg
	}
	copied := run(false)
	kept := run(true)

	_, wantBlocks := syncedBlocks(t, copied)
	keptDB, gotBlocks := syncedBlocks(t, kept)
	require.NotEmpty(t, wantBlocks)
	require.Equal(t, len(wantBlocks), len(gotBlocks))
	for i := range wantBlocks {
		require.Equal(t, wantBlocks[i].Hash, gotBlocks[i].Hash)
		require.Equal(t, wantBlocks[i].ID, gotBlocks[i].ID)
		require.Equal(t, wantBlocks[i].Cbor, gotBlocks[i].Cbor, "block %d", i)
	}

	st, ok := keptDB.ImmutableState()
	require.True(t, ok)
	require.True(t, st.HasTip)
	require.Equal(t, wantBlocks[0].Hash, st.TipHash)
	for _, ext := range []string{".chunk", ".primary", ".secondary"} {
		_, err := os.Stat(filepath.Join(kept.DataDir, "immutable", "00000"+ext))
		require.NoError(t, err, ext)
	}
	_, err = os.Stat(filepath.Join(copied.DataDir, "immutable"))
	require.ErrorIs(t, err, os.ErrNotExist,
		"without the flag nothing is placed in an ImmutableDB")
}
