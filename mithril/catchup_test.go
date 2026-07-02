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
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const immutableTestdataDir = "../database/immutable/testdata"

// firstImmutableBlock returns a real (slot, hash) from the immutable testdata so
// the intersection check is exercised against genuine on-chain block data.
func firstImmutableBlock(t *testing.T) (uint64, []byte) {
	t.Helper()
	imm, err := immutable.New(immutableTestdataDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0})
	require.NoError(t, err)
	defer func() { _ = iter.Close() }()
	blk, err := iter.Next()
	require.NoError(t, err)
	require.NotNil(t, blk, "immutable testdata must contain at least one block")
	return blk.Slot, bytes.Clone(blk.Hash)
}

// TestVerifyCatchupIntersection pins the divergence gate: a local tip that
// matches the target artifact's block at that slot is accepted (the local chain
// is an ancestor), while a local tip with a different hash at the same slot is
// rejected so the operator is told to perform a full resync.
func TestVerifyCatchupIntersection(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	slot, hash := firstImmutableBlock(t)

	t.Run("matching tip is accepted", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     slot,
			Hash:     hash,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     6,
		}, nil))
		require.NoError(
			t, verifyCatchupIntersection(db, immutableTestdataDir, discard),
		)
	})

	t.Run("mismatched tip diverges", func(t *testing.T) {
		db := newSyncModeTestDB(t)
		wrong := bytes.Clone(hash)
		wrong[0] ^= 0xff
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     slot,
			Hash:     wrong,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     6,
		}, nil))
		err := verifyCatchupIntersection(db, immutableTestdataDir, discard)
		require.Error(t, err)
		require.ErrorContains(t, err, "diverges")
	})
}

// artifactTipBlock returns the (slot, hash) of the last block in the immutable
// testdata so tests can construct local chains that are ahead of the artifact.
func artifactTipBlock(t *testing.T) (uint64, []byte) {
	t.Helper()
	imm, err := immutable.New(immutableTestdataDir)
	require.NoError(t, err)
	tip, err := imm.GetTip()
	require.NoError(t, err)
	require.NotNil(t, tip, "immutable testdata must contain blocks")
	return tip.Slot, bytes.Clone(tip.Hash)
}

// TestVerifyCatchupIntersectionLocalAhead pins the fail-closed behavior when
// the local chain tip is above the target artifact's sealed range: catch-up
// must never import an older snapshot over a newer database. A local chain
// containing the artifact's tip block is a strict descendant, so there is
// nothing to catch up (errCatchUpLocalAhead); a local chain on which the
// artifact tip block cannot be found diverges and must abort.
//
// The subtests share one database (database.New is expensive) and are
// order-dependent: each rebuilds the chain shape it needs via
// deleteBlobBlocksAboveSlot + BlockCreate. Run the whole function, not
// individual subtests.
func TestVerifyCatchupIntersectionLocalAhead(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	artSlot, artHash := artifactTipBlock(t)
	db := newSyncModeTestDB(t)

	aheadBlock := models.Block{
		Slot:     artSlot + 5000,
		Hash:     bytes.Repeat([]byte{0xcc}, 32),
		PrevHash: bytes.Repeat([]byte{0xcd}, 32),
		Cbor:     []byte{0x80},
		Number:   99,
		Type:     6,
	}
	atArtifactTip := func(hash []byte) models.Block {
		return models.Block{
			Slot:     artSlot,
			Hash:     hash,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Cbor:     []byte{0x80},
			Number:   98,
			Type:     6,
		}
	}

	t.Run("ahead chain with no block at or below the artifact tip diverges",
		func(t *testing.T) {
			require.NoError(t, db.BlockCreate(aheadBlock, nil))
			err := verifyCatchupIntersection(db, immutableTestdataDir, discard)
			require.Error(t, err)
			require.NotErrorIs(t, err, errCatchUpLocalAhead)
			require.ErrorContains(t, err, "diverges")
		})

	t.Run("ahead chain without the artifact tip block diverges",
		func(t *testing.T) {
			wrong := bytes.Clone(artHash)
			wrong[0] ^= 0xff
			require.NoError(t, db.BlockCreate(atArtifactTip(wrong), nil))
			err := verifyCatchupIntersection(db, immutableTestdataDir, discard)
			require.Error(t, err)
			require.NotErrorIs(t, err, errCatchUpLocalAhead)
			require.ErrorContains(t, err, "diverges")
		})

	t.Run("descendant chain is reported ahead", func(t *testing.T) {
		// Replace the wrong-hash block at the artifact tip slot.
		require.NoError(t, deleteBlobBlocksAboveSlot(db, artSlot-1))
		require.NoError(t, db.BlockCreate(atArtifactTip(artHash), nil))
		require.NoError(t, db.BlockCreate(aheadBlock, nil))
		err := verifyCatchupIntersection(db, immutableTestdataDir, discard)
		require.ErrorIs(t, err, errCatchUpLocalAhead)
	})
}

// TestVerifyCatchupBeforeImport pins the Sync-side handling of the
// intersection check: an ancestor tip proceeds with the import without moving
// the marker, a strictly-ahead local chain is reported up-to-date and the
// import marker advances to the target (so later runs no-op without
// re-downloading), and a divergent chain aborts.
//
// The subtests share one database and are order-dependent (see
// TestVerifyCatchupIntersectionLocalAhead). Run the whole function.
func TestVerifyCatchupBeforeImport(t *testing.T) {
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	artSlot, artHash := artifactTipBlock(t)
	db := newSyncModeTestDB(t)

	t.Run("ancestor tip proceeds with the import", func(t *testing.T) {
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     artSlot,
			Hash:     artHash,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Cbor:     []byte{0x80},
			Number:   98,
			Type:     6,
		}, nil))
		upToDate, err := verifyCatchupBeforeImport(
			db, immutableTestdataDir, 42, discard,
		)
		require.NoError(t, err)
		require.False(t, upToDate)
		_, ok, err := getImmutableImportMarker(db)
		require.NoError(t, err)
		require.False(t, ok, "marker must not move before the import runs")
	})

	t.Run("local-ahead marks up to date and records the marker",
		func(t *testing.T) {
			require.NoError(t, db.BlockCreate(models.Block{
				Slot:     artSlot + 5000,
				Hash:     bytes.Repeat([]byte{0xcc}, 32),
				PrevHash: bytes.Repeat([]byte{0xcd}, 32),
				Cbor:     []byte{0x80},
				Number:   99,
				Type:     6,
			}, nil))
			upToDate, err := verifyCatchupBeforeImport(
				db, immutableTestdataDir, 42, discard,
			)
			require.NoError(t, err)
			require.True(t, upToDate)
			marker, ok, err := getImmutableImportMarker(db)
			require.NoError(t, err)
			require.True(t, ok, "marker must be recorded on local-ahead")
			require.EqualValues(t, 42, marker)
		})

	t.Run("divergent tip aborts", func(t *testing.T) {
		// Rebuild the chain with a wrong-hash block at the artifact tip.
		require.NoError(t, deleteBlobBlocksAboveSlot(db, artSlot-1))
		wrong := bytes.Clone(artHash)
		wrong[0] ^= 0xff
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     artSlot,
			Hash:     wrong,
			PrevHash: bytes.Repeat([]byte{0x01}, 32),
			Cbor:     []byte{0x80},
			Number:   98,
			Type:     6,
		}, nil))
		_, err := verifyCatchupBeforeImport(
			db, immutableTestdataDir, 42, discard,
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "diverges")
	})
}
