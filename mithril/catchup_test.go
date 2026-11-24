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
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	dledger "github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// seedCompleteDB creates a database at dataDir that looks like a finished sync:
// one chain block and a clear sync_status, optionally with an immutable-import
// marker. determineSyncMode classifies it as syncModeCatchUp.
func seedCompleteDB(
	t *testing.T,
	dataDir, storageMode string,
	marker uint64,
	setMarker bool,
) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		StorageMode: storageMode,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     42,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}, nil))
	if setMarker {
		require.NoError(t, setImmutableImportMarker(db, marker))
	}
	require.NoError(t, dbtest.CloseDatabase(db))
}

// TestSyncCatchUpDispatch covers the two state-detected catch-up decisions that
// must not mutate the database: an up-to-date core DB is a no-op, and an api-mode
// DB is rejected (api catch-up is unsupported in this version).
func TestSyncCatchUpDispatch(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("up-to-date core DB returns without syncing", func(t *testing.T) {
		t.Parallel()
		fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
		dataDir := t.TempDir()
		// Marker at the artifact's immutable tip → already up to date.
		seedCompleteDB(t, dataDir, "core", 5, true)

		res, err := Sync(context.Background(), SyncConfig{
			UtxoSet:           newSyncTestSet(t),
			Network:           "preview",
			DataDir:           dataDir,
			Backend:           BackendV2,
			AggregatorURL:     fix.server.URL,
			AllowInsecureHTTP: true,
			StoragePlugins:    testStoragePlugins(),
			DatabaseWorkers:   1,
			Logger:            discard,
		})
		require.NoError(t, err)
		require.Nil(t, res.Snapshot, "up-to-date catch-up should not sync")
	})

	t.Run("a divergent chain is rejected before mutation", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			validLedgerState:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		wrongHash := bytes.Clone(anchorHash)
		wrongHash[0] ^= 0xff
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     wrongHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		_, err = Sync(context.Background(), SyncConfig{
			UtxoSet:           newSyncTestSet(t),
			Network:           "preprod",
			DataDir:           dataDir,
			Backend:           BackendV2,
			AggregatorURL:     fixture.server.URL,
			AllowInsecureHTTP: true,
			StoragePlugins:    testStoragePlugins(),
			DatabaseWorkers:   1,
			Logger:            discard,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "diverg")
		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		block, err := database.BlockByHash(context.Background(), db, wrongHash)
		require.NoError(t, err)
		require.EqualValues(t, 1000, block.Slot)
		status, err := db.GetChainState("sync_status", nil)
		require.NoError(t, err)
		require.Empty(t, status)
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	t.Run("legacy reward repair reconciles an existing database in place", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			validLedgerState:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     anchorHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))
		// The set applied a block after the selected state.
		set := newSyncTestSet(t)
		statePoint := ocommon.NewPoint(1000, anchorHash)
		seedRewardRepairSet(t, set, statePoint, nil, nil,
			[]rewardRepairSetCommit{{
				point:   ocommon.NewPoint(1050, bytes.Repeat([]byte{0x67}, 32)),
				created: bytes.Repeat([]byte{0x68}, 32),
			}},
		)

		result, err := Sync(context.Background(), SyncConfig{
			UtxoSet:                 set,
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			Logger:                  discard,
			RepairLegacyRewardState: true,
		})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		pending, err := RewardStateRepairPending(db)
		require.NoError(t, err)
		require.False(t, pending)
		block, err := database.BlockByHash(context.Background(), db, anchorHash)
		require.NoError(t, err)
		require.EqualValues(t, 1000, block.Slot,
			"repair must retain the existing chain anchor")
		require.NoError(t, dbtest.CloseDatabase(db))
		requireSetHoldsOnlyState(t, set, statePoint)
	})

	t.Run("legacy reward repair preserves a verified local tail", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			validLedgerState:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		localTailHash := bytes.Repeat([]byte{0xcc}, 32)
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     anchorHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1100,
			Hash:     localTailHash,
			PrevHash: anchorHash,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.SetChainState(
			mithrilLedgerSlotSyncKey, "1000", nil,
		))
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))
		// The set applied the local tail after the selected state.
		set := newSyncTestSet(t)
		statePoint := ocommon.NewPoint(1000, anchorHash)
		seedRewardRepairSet(t, set, statePoint, nil, nil,
			[]rewardRepairSetCommit{{
				point:   ocommon.NewPoint(1100, localTailHash),
				created: bytes.Repeat([]byte{0x69}, 32),
			}},
		)

		result, err := Sync(context.Background(), SyncConfig{
			UtxoSet:                 set,
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			Logger:                  discard,
			RepairLegacyRewardState: true,
		})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		pending, err := RewardStateRepairPending(db)
		require.NoError(t, err)
		require.False(t, pending)
		block, err := database.BlockByHash(context.Background(), db, localTailHash)
		require.NoError(t, err)
		require.EqualValues(t, 1100, block.Slot,
			"validated local volatile blocks must remain for ordinary ledger replay")
		stableTip, err := db.GetTip(nil)
		require.NoError(t, err)
		require.EqualValues(t, 1000, stableTip.Point.Slot)
		require.Equal(t, anchorHash, stableTip.Point.Hash)
		recent, err := database.BlocksRecent(context.Background(), db, 1)
		require.NoError(t, err)
		require.Len(t, recent, 1)
		require.EqualValues(t, 1100, recent[0].Slot,
			"the retained blob tail must remain the node's replay frontier")
		require.NoError(t, dbtest.CloseDatabase(db))
		requireSetHoldsOnlyState(t, set, statePoint)
	})

	t.Run("legacy reward repair refuses to rewind the stable ledger anchor", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			validLedgerState:    true,
		})
		_, snapshotHash := validImmutableFiles(t, 1000)
		stableHash := bytes.Repeat([]byte{0xdd}, 32)
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     snapshotHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.SetChainState(
			mithrilLedgerSlotSyncKey, "1050", nil,
		))
		require.NoError(t, db.SetChainState(
			mithrilLedgerHashSyncKey, hex.EncodeToString(stableHash), nil,
		))
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		_, err = Sync(context.Background(), SyncConfig{
			UtxoSet:                 newSyncTestSet(t),
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			Logger:                  discard,
			RepairLegacyRewardState: true,
		})
		require.ErrorIs(t, err, ErrRewardStateRepairWaitingForSnapshot)

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		for _, hash := range [][]byte{snapshotHash} {
			_, err := database.BlockByHash(context.Background(), db, hash)
			require.NoError(t, err, "the database must remain untouched before the trust check")
		}
		status, err := db.GetChainState("sync_status", nil)
		require.NoError(t, err)
		require.Empty(t, status)
		stableSlot, err := db.GetChainState(mithrilLedgerSlotSyncKey, nil)
		require.NoError(t, err)
		require.Equal(t, "1050", stableSlot)
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	t.Run("legacy reward repair resumes its durable artifact pin", func(t *testing.T) {
		t.Parallel()
		fixture := newV2Fixture(t, v2FixtureOptions{
			immutableFileNumber: 0,
			validImmutable:      true,
			validLedgerState:    true,
		})
		_, anchorHash := validImmutableFiles(t, 1000)
		dataDir := t.TempDir()
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot:     1000,
			Hash:     anchorHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		}, nil))
		require.NoError(t, setImmutableImportMarker(db, 0))
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, db.SetChainState(
			RewardStateRepairActiveKey, "1", nil,
		))
		require.NoError(t, db.SetChainState(
			"sync_status", syncStatusInProgress, nil,
		))
		require.NoError(t, setPinnedArtifact(db, pinnedArtifact{
			Backend:             BackendV2,
			Network:             "preprod",
			Digest:              fixture.artifact.Hash,
			Epoch:               fixture.artifact.Beacon.Epoch,
			ImmutableFileNumber: fixture.artifact.Beacon.ImmutableFileNumber,
			CertificateHash:     fixture.artifact.CertificateHash,
		}))
		require.NoError(t, dbtest.CloseDatabase(db))
		// The interrupted run stopped before its catch-up load, so the set
		// still holds the block it applied after the pinned state.
		set := newSyncTestSet(t)
		statePoint := ocommon.NewPoint(1000, anchorHash)
		seedRewardRepairSet(t, set, statePoint, nil, nil,
			[]rewardRepairSetCommit{{
				point:   ocommon.NewPoint(1050, bytes.Repeat([]byte{0x6b}, 32)),
				created: bytes.Repeat([]byte{0x6c}, 32),
			}},
		)

		result, err := Sync(context.Background(), SyncConfig{
			UtxoSet:                 set,
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			AggregatorURL:           fixture.server.URL,
			AllowInsecureHTTP:       true,
			StoragePlugins:          testStoragePlugins(),
			DatabaseWorkers:         1,
			Logger:                  discard,
			RepairLegacyRewardState: true,
		})
		require.NoError(t, err)
		require.NotNil(t, result.Snapshot)

		db, err = dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		pending, err := RewardStateRepairPending(db)
		require.NoError(t, err)
		require.False(t, pending)
		require.NoError(t, dbtest.CloseDatabase(db))
		requireSetHoldsOnlyState(t, set, statePoint)
	})
}

func TestSyncRewardRepairBelowCertifiedTipDropsOnlyForkBlocks(t *testing.T) {
	t.Parallel()
	testSyncRewardRepairBelowCertifiedTip(t, true)
}

func TestSyncRewardRepairWithoutLocalTailUnspendsAtCertifiedFloor(t *testing.T) {
	t.Parallel()
	testSyncRewardRepairBelowCertifiedTip(t, false)
}

func testSyncRewardRepairBelowCertifiedTip(t *testing.T, withLocalTail bool) {
	t.Helper()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	files, certifiedHash := validImmutableFiles(t, 1000)
	stateHash := bytes.Clone(files["immutable/00000.secondary"][16:48])
	block1050 := rewardRepairTestBlock(t, 1050, 3, certifiedHash)
	block1100 := rewardRepairTestBlock(t, 1100, 4, block1050.Hash)
	staleFork := rewardRepairTestBlock(
		t, 1025, 99, bytes.Repeat([]byte{0xee}, 32),
	)
	txID := bytes.Repeat([]byte{0x62}, 32)
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x71}, 28)...)
	txInCBOR, err := cbor.Encode([]any{txID, uint64(0)})
	require.NoError(t, err)
	txOutCBOR, err := cbor.Encode([]any{address, uint64(42)})
	require.NoError(t, err)
	utxoMap := append([]byte{0xa1}, txInCBOR...)
	utxoMap = append(utxoMap, txOutCBOR...)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber:     0,
		validImmutable:          true,
		validLedgerState:        true,
		validLedgerStateSlot:    999,
		validLedgerStateUTxOMap: utxoMap,
	})

	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     999,
		Hash:     stateHash,
		PrevHash: bytes.Repeat([]byte{0}, 32),
		Cbor:     []byte{0x80},
		Number:   1,
		Type:     uint(shelley.BlockTypeShelley),
	}, nil))
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     1000,
		Hash:     certifiedHash,
		PrevHash: stateHash,
		Cbor:     []byte{0x80},
		Number:   2,
		Type:     uint(shelley.BlockTypeShelley),
	}, nil))
	if !withLocalTail {
		require.NoError(t, db.SetTip(ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1000, Hash: certifiedHash},
			BlockNumber: 2,
		}, nil))
		localTip, err := db.GetTip(nil)
		require.NoError(t, err)
		require.EqualValues(t, 1000, localTip.Point.Slot,
			"fixture must exercise a local tip equal to the certified tip")
	}
	if withLocalTail {
		for _, block := range []models.Block{block1050, staleFork, block1100} {
			require.NoError(t, db.BlockCreate(block, nil))
		}
	}
	// The set holds the snapshot's output and applied the blocks after the
	// state: the certified block spends that output and adds another, and the
	// local tail adds more.
	set := newSyncTestSet(t)
	statePoint := ocommon.NewPoint(999, stateHash)
	aboveState := []rewardRepairSetCommit{{
		point:   ocommon.NewPoint(1000, certifiedHash),
		spent:   []models.UtxoId{{Hash: txID, Idx: 0}},
		created: bytes.Repeat([]byte{0x63}, 32),
	}}
	if withLocalTail {
		aboveState = append(aboveState, rewardRepairSetCommit{
			point:   ocommon.NewPoint(block1050.Slot, block1050.Hash),
			created: bytes.Repeat([]byte{0x64}, 32),
		}, rewardRepairSetCommit{
			point:   ocommon.NewPoint(block1100.Slot, block1100.Hash),
			created: bytes.Repeat([]byte{0x65}, 32),
		})
	}
	seedRewardRepairSet(
		t, set, statePoint, txID, txOutCBOR, aboveState,
	)
	require.NoError(t, setImmutableImportMarker(db, 0))
	require.NoError(t, db.SetChainState(
		mithrilLedgerSlotSyncKey, "999", nil,
	))
	require.NoError(t, db.SetChainState(
		mithrilLedgerHashSyncKey, hex.EncodeToString(stateHash), nil,
	))
	require.NoError(t, db.SetChainState(
		RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, dbtest.CloseDatabase(db))

	result, err := Sync(context.Background(), SyncConfig{
		UtxoSet:                 set,
		Network:                 "preprod",
		DataDir:                 dataDir,
		Backend:                 BackendV2,
		PinnedDigest:            "original-bootstrap-pin",
		AggregatorURL:           fixture.server.URL,
		AllowInsecureHTTP:       true,
		StoragePlugins:          testStoragePlugins(),
		DatabaseWorkers:         1,
		Logger:                  discard,
		RepairLegacyRewardState: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result.Snapshot)

	db, err = dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)
	if withLocalTail {
		_, err = database.BlockByHash(context.Background(), db, staleFork.Hash)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
		retained, err := database.BlockByHash(context.Background(), db, block1100.Hash)
		require.NoError(t, err)
		require.EqualValues(t, 1100, retained.Slot,
			"canonical blocks after the selected state must remain for replay")
	}
	requireSetHoldsOnlyState(
		t, set, statePoint, models.UtxoId{Hash: txID, Idx: 0},
	)
}

func TestSyncRewardRepairUnspendsOutputsSpentAfterSnapshotState(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, certifiedHash := validImmutableFiles(t, 1000)
	// The local chain already holds the two blocks of the signed ancillary
	// chunk, which end at the ledger state's slot.
	trio, _ := ancillaryChunkAfter(t, 1100, 2, certifiedHash)
	signed, err := readAncillaryChunk(
		discard, signedAncillary(t, trio, nil), 0,
	)
	require.NoError(t, err)
	require.Len(t, signed, 2)
	block1099 := models.Block{
		Slot: signed[0].Slot, Hash: signed[0].Hash, PrevHash: certifiedHash,
		Cbor: signed[0].Cbor, Number: 3, Type: signed[0].Type,
	}
	block1100 := models.Block{
		Slot: signed[1].Slot, Hash: signed[1].Hash, PrevHash: signed[0].Hash,
		Cbor: signed[1].Cbor, Number: 4, Type: signed[1].Type,
	}
	txID := bytes.Repeat([]byte{0x61}, 32)
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x71}, 28)...)
	txInCBOR, err := cbor.Encode([]any{txID, uint64(0)})
	require.NoError(t, err)
	txOutCBOR, err := cbor.Encode([]any{address, uint64(42)})
	require.NoError(t, err)
	utxoMap := append([]byte{0xa1}, txInCBOR...)
	utxoMap = append(utxoMap, txOutCBOR...)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 0,
		validImmutable:      true,
		ancillaryChunk:      trio,
		ancillaryLedgerSlot: 1100,
		ancillaryLedgerState: minimalLedgerStateWithUTxOMap(
			t, 1100, block1100.Hash, utxoMap,
		),
	})

	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	for _, block := range []models.Block{
		{
			Slot:     1000,
			Hash:     certifiedHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		},
		block1099,
		block1100,
	} {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	// The set applied a block after the state that spent the snapshot's
	// output.
	set := newSyncTestSet(t)
	statePoint := ocommon.NewPoint(block1100.Slot, block1100.Hash)
	seedRewardRepairSet(
		t, set, statePoint, txID, txOutCBOR,
		[]rewardRepairSetCommit{{
			point: ocommon.NewPoint(1200, bytes.Repeat([]byte{0x66}, 32)),
			spent: []models.UtxoId{{Hash: txID, Idx: 0}},
		}},
	)
	require.NoError(t, setImmutableImportMarker(db, 0))
	require.NoError(t, db.SetChainState(
		mithrilLedgerSlotSyncKey, "1000", nil,
	))
	require.NoError(t, db.SetChainState(
		mithrilLedgerHashSyncKey, hex.EncodeToString(certifiedHash), nil,
	))
	require.NoError(t, db.SetChainState(
		RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, dbtest.CloseDatabase(db))

	_, err = Sync(context.Background(), SyncConfig{
		UtxoSet: set,
		Network: "preprod",
		DataDir: dataDir,
		CardanoNodeConfig: testNodeConfigWithMithrilKeys(
			t, fixture.genesisVKey, fixture.ancillaryVKey,
		),
		Backend:                 BackendV2,
		PinnedDigest:            "original-bootstrap-pin",
		VerifyCertChain:         true,
		AggregatorURL:           fixture.server.URL,
		AllowInsecureHTTP:       true,
		StoragePlugins:          testStoragePlugins(),
		DatabaseWorkers:         1,
		Logger:                  discard,
		RepairLegacyRewardState: true,
	})
	require.NoError(t, err)

	requireSetHoldsOnlyState(
		t, set, statePoint, models.UtxoId{Hash: txID, Idx: 0},
	)
}

// TestSyncRewardRepairKeepsSignedChunkPastLocalTip covers a reward repair
// whose local chain ends inside the signed ancillary chunk and whose state is
// at the certified tip. The chunk's blocks past the old local tip continue
// the verified tail, so they stay stored for replay.
func TestSyncRewardRepairKeepsSignedChunkPastLocalTip(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, certifiedHash := validImmutableFiles(t, 1000)
	trio, _ := ancillaryChunkAfter(t, 1100, 3, certifiedHash)
	signed, err := readAncillaryChunk(
		discard, signedAncillary(t, trio, nil), 0,
	)
	require.NoError(t, err)
	require.Len(t, signed, 3)
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 0,
		validImmutable:      true,
		ancillaryChunk:      trio,
		ancillaryLedgerSlot: 1000,
		ancillaryLedgerState: minimalLedgerState(
			t, 1000, certifiedHash,
		),
	})

	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	for _, block := range []models.Block{
		{
			Slot:     1000,
			Hash:     certifiedHash,
			PrevHash: bytes.Repeat([]byte{0}, 32),
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     uint(shelley.BlockTypeShelley),
		},
		{
			Slot: signed[0].Slot, Hash: signed[0].Hash,
			PrevHash: certifiedHash, Cbor: signed[0].Cbor, Number: 3,
			Type: signed[0].Type,
		},
	} {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	require.NoError(t, setImmutableImportMarker(db, 0))
	require.NoError(t, db.SetChainState(
		mithrilLedgerSlotSyncKey, "1000", nil,
	))
	require.NoError(t, db.SetChainState(
		mithrilLedgerHashSyncKey, hex.EncodeToString(certifiedHash), nil,
	))
	require.NoError(t, db.SetChainState(
		RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, dbtest.CloseDatabase(db))

	_, err = Sync(context.Background(), SyncConfig{
		UtxoSet: newSyncTestSet(t),
		Network: "preprod",
		DataDir: dataDir,
		CardanoNodeConfig: testNodeConfigWithMithrilKeys(
			t, fixture.genesisVKey, fixture.ancillaryVKey,
		),
		Backend:                 BackendV2,
		PinnedDigest:            "original-bootstrap-pin",
		VerifyCertChain:         true,
		AggregatorURL:           fixture.server.URL,
		AllowInsecureHTTP:       true,
		StoragePlugins:          testStoragePlugins(),
		DatabaseWorkers:         1,
		Logger:                  discard,
		RepairLegacyRewardState: true,
	})
	require.NoError(t, err)

	db, err = dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)
	for _, block := range signed {
		_, err := database.BlockByHash(context.Background(), db, block.Hash)
		require.NoError(t, err, "signed chunk block at slot %d", block.Slot)
	}
}

// TestSyncRewardRepairWaitsForAStateItsSetReaches covers a reward repair
// whose LedgerDB set was built past the selected state with no journal back
// to it, as the v0.94.2 migration leaves one. The repair must wait for a later
// artifact before it writes anything, not stop the node on every resume.
func TestSyncRewardRepairWaitsForAStateItsSetReaches(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	fixture := newV2Fixture(t, v2FixtureOptions{
		immutableFileNumber: 0,
		validImmutable:      true,
		validLedgerState:    true,
	})
	_, anchorHash := validImmutableFiles(t, 1000)
	localTailHash := bytes.Repeat([]byte{0xcc}, 32)
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	for _, block := range []models.Block{
		{
			Slot: 1000, Hash: anchorHash,
			PrevHash: bytes.Repeat([]byte{0}, 32), Cbor: []byte{0x80},
			Number: 2, Type: uint(shelley.BlockTypeShelley),
		},
		{
			Slot: 1100, Hash: localTailHash, PrevHash: anchorHash,
			Cbor: []byte{0x80}, Number: 3,
			Type: uint(shelley.BlockTypeShelley),
		},
	} {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	require.NoError(t, setImmutableImportMarker(db, 0))
	require.NoError(t, db.SetChainState(
		mithrilLedgerSlotSyncKey, "1000", nil,
	))
	require.NoError(t, db.SetChainState(
		RewardStateRepairPendingKey, "1", nil,
	))
	closing := db
	db = nil
	require.NoError(t, dbtest.CloseDatabase(closing))

	setTip := ocommon.NewPoint(1100, localTailHash)
	set := newSyncTestSet(t)
	require.NoError(t, set.LoadUtxoSet(setTip,
		func(put func([]byte, uint32, []byte) error) error {
			return put(bytes.Repeat([]byte{0x6a}, 32), 0, []byte{0x80})
		},
	))
	require.NoError(t, set.FinishImport())

	_, err = Sync(context.Background(), SyncConfig{
		UtxoSet:                 set,
		Network:                 "preprod",
		DataDir:                 dataDir,
		Backend:                 BackendV2,
		PinnedDigest:            "original-bootstrap-pin",
		AggregatorURL:           fixture.server.URL,
		AllowInsecureHTTP:       true,
		StoragePlugins:          testStoragePlugins(),
		DatabaseWorkers:         1,
		Logger:                  discard,
		RepairLegacyRewardState: true,
	})
	require.ErrorIs(t, err, ErrRewardStateRepairWaitingForSnapshot)

	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, setTip, tip, "the set is untouched")
	n, err := set.Count()
	require.NoError(t, err)
	require.EqualValues(t, 1, n, "the set is untouched")
	db, err = dbtest.NewDatabase(t, &database.Config{
		DataDir:     dataDir,
		StorageMode: "core",
		Logger:      discard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		closing := db
		db = nil
		require.NoError(t, dbtest.CloseDatabase(closing))
	})
	status, err := db.GetChainState("sync_status", nil)
	require.NoError(t, err)
	require.Empty(t, status, "nothing marks the database mid-repair")
	pending, err := RewardStateRepairPending(db)
	require.NoError(t, err)
	require.True(t, pending)
}

func TestVerifyRewardRepairLocalTailResolvesHashlessAnchorOnChain(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	canonical := rewardRepairTestBlock(
		t, 1000, 2, bytes.Repeat([]byte{0x11}, 32),
	)
	sibling := rewardRepairTestBlock(
		t, 1000, 2, bytes.Repeat([]byte{0x22}, 32),
	)
	localTip := rewardRepairTestBlock(t, 1100, 3, canonical.Hash)
	for _, block := range []models.Block{canonical, sibling, localTip} {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	require.NoError(t, db.SetChainState(mithrilLedgerSlotSyncKey, "1000", nil))

	preserved, err := verifyRewardRepairLocalTail(
		context.Background(),
		db,
		localTip,
		&preparedLedgerStateImport{state: &ledgerstate.RawLedgerState{
			Tip: &ledgerstate.SnapshotTip{
				Slot:      1000,
				BlockHash: canonical.Hash,
			},
		}},
	)
	require.NoError(t, err)
	_, ok := preserved[string(localTip.Hash)]
	require.True(t, ok,
		"hashless legacy anchor resolution must follow the local chain")
}

func rewardRepairTestBlock(
	t *testing.T,
	slot, number uint64,
	previousHash []byte,
) models.Block {
	t.Helper()
	var previous lcommon.Blake2b256
	copy(previous[:], previousHash)
	header := shelley.ShelleyBlockHeader{
		Body: shelley.ShelleyBlockHeaderBody{
			BlockNumber:       number,
			Slot:              slot,
			PrevHash:          previous,
			BlockBodySize:     0,
			ProtoMajorVersion: 1,
		},
	}
	headerCBOR, err := cbor.Encode(header)
	require.NoError(t, err)
	blockBody, err := cbor.Encode([]any{
		cbor.RawMessage(headerCBOR), []any{}, []any{}, map[uint]any{},
	})
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(headerCBOR)
	return models.Block{
		Slot: slot, Hash: hash[:], PrevHash: bytes.Clone(previousHash),
		Cbor: blockBody, Number: number,
		Type: uint(shelley.BlockTypeShelley),
	}
}

func TestSyncRewardRepairRequiresItsDurableMarkers(t *testing.T) {
	t.Parallel()
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("complete database has no repair marker", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		seedCompleteDB(t, dataDir, "core", 0, false)
		_, err := Sync(context.Background(), SyncConfig{
			UtxoSet:                 newSyncTestSet(t),
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			RepairLegacyRewardState: true,
			Logger:                  discard,
		})
		require.ErrorContains(t, err, "pending repair marker")
	})

	t.Run("interrupted sync is not an active repair", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		seedCompleteDB(t, dataDir, "core", 0, false)
		db, err := dbtest.NewDatabase(t, &database.Config{
			DataDir:     dataDir,
			StorageMode: "core",
			Logger:      discard,
		})
		require.NoError(t, err)
		require.NoError(t, db.SetChainState(
			RewardStateRepairPendingKey, "1", nil,
		))
		require.NoError(t, db.SetChainState(
			"sync_status", syncStatusInProgress, nil,
		))
		require.NoError(t, dbtest.CloseDatabase(db))

		_, err = Sync(context.Background(), SyncConfig{
			UtxoSet:                 newSyncTestSet(t),
			Network:                 "preprod",
			DataDir:                 dataDir,
			Backend:                 BackendV2,
			PinnedDigest:            "original-bootstrap-pin",
			RepairLegacyRewardState: true,
			Logger:                  discard,
		})
		require.ErrorContains(t, err, "without its in-progress marker")
	})
}

// TestDecideCatchUp pins the dispatch decision that selects catch-up semantics
// (divergence check before mutation + reconcile of stale live rows) for every
// v2 import into a previously-complete database — including interrupted
// catch-ups and databases without an import marker — so no path can re-import
// a snapshot over live state without reconciliation.
//
// The subtests share one database (database.New is expensive) and are
// order-dependent: the fresh-database case runs before any block is seeded,
// and each later subtest sets sync_status and the import marker to exactly
// the state it needs. Run the whole function, not individual subtests.
func TestDecideCatchUp(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	// Unroutable aggregator: proves decision paths that must not fetch.
	const noAggregator = "http://127.0.0.1:1"

	db := newSyncModeTestDB(t)

	// setState pins the shared database to exactly the sync_status ("" =
	// clear) and marker (hasMarker=false = absent) a subtest needs.
	setState := func(
		t *testing.T, status string, marker uint64, hasMarker bool,
	) {
		t.Helper()
		if status == "" {
			require.NoError(t, db.DeleteChainState("sync_status", nil))
		} else {
			require.NoError(t, db.SetChainState("sync_status", status, nil))
		}
		if hasMarker {
			require.NoError(t, setImmutableImportMarker(db, marker))
		} else {
			require.NoError(t, db.DeleteChainState(syncKeyImmutableMax, nil))
		}
		require.NoError(t, db.DeleteChainState(
			RewardStateRepairPendingKey, nil,
		))
		require.NoError(t, db.DeleteChainState(syncKeyCatchUpActive, nil))
	}
	modeOf := func(t *testing.T) syncMode {
		t.Helper()
		mode, err := determineSyncMode(context.Background(), db)
		require.NoError(t, err)
		return mode
	}

	t.Run("fresh database bootstraps", func(t *testing.T) {
		mode := modeOf(t)
		require.Equal(t, syncModeBootstrap, mode)
		dec, err := decideCatchUp(
			ctx, db, mode, BackendV2, noAggregator, true, discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.False(t, dec.upToDate)
	})

	// Every remaining subtest runs against a database with chain data.
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:     42,
		Hash:     bytes.Repeat([]byte{0xaa}, 32),
		PrevHash: bytes.Repeat([]byte{0xbb}, 32),
		Cbor:     []byte{0x80},
		Number:   7,
		Type:     6,
	}, nil))

	t.Run("markerless complete database catches up over the full range",
		func(t *testing.T) {
			setState(t, "", 0, false)
			mode := modeOf(t)
			require.Equal(t, syncModeCatchUp, mode)
			dec, err := decideCatchUp(
				ctx, db, mode, BackendV2, noAggregator, true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"markerless complete DB must reconcile on re-import")
			require.EqualValues(t, 0, dec.start,
				"no marker means the full artifact range")
		})

	t.Run("unsupported backend never engages", func(t *testing.T) {
		setState(t, "", 2, true)
		dec, err := decideCatchUp(
			ctx, db, modeOf(t), "v0", noAggregator, true, discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.False(t, dec.upToDate)
	})

	t.Run("marker behind target engages catch-up from the marker",
		func(t *testing.T) {
			fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
			setState(t, "", 2, true)
			dec, err := decideCatchUp(
				ctx, db, modeOf(t), BackendV2, fix.server.URL,
				true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage)
			require.EqualValues(t, 2, dec.start)
		})

	t.Run("marker at target is up to date", func(t *testing.T) {
		fix := newV2Fixture(t, v2FixtureOptions{immutableFileNumber: 5})
		setState(t, "", 5, true)
		dec, err := decideCatchUp(
			ctx,
			db,
			modeOf(t),
			BackendV2,
			fix.server.URL,
			true,
			discard,
		)
		require.NoError(t, err)
		require.False(t, dec.engage)
		require.True(t, dec.upToDate)
	})

	t.Run("interrupted sync with marker re-runs as catch-up",
		func(t *testing.T) {
			setState(t, syncStatusInProgress, 7, true)
			mode := modeOf(t)
			require.Equal(t, syncModeResume, mode)
			dec, err := decideCatchUp(
				ctx, db, mode, BackendV2, noAggregator, true, discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"interrupted catch-up must re-run with reconcile")
			require.EqualValues(t, 7, dec.start)
		})

	t.Run("interrupted sync without marker resumes normally",
		func(t *testing.T) {
			setState(t, syncStatusInProgress, 0, false)
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.False(t, dec.engage)
			require.False(t, dec.upToDate)
		})

	t.Run("interrupted markerless catch-up re-runs as catch-up",
		func(t *testing.T) {
			// A markerless catch-up (fix for pre-marker databases) writes no
			// marker until completion; the active flag is its only trace.
			setState(t, syncStatusInProgress, 0, false)
			require.NoError(t, setCatchUpActive(db))
			dec, err := decideCatchUp(
				ctx,
				db,
				modeOf(t),
				BackendV2,
				noAggregator,
				true,
				discard,
			)
			require.NoError(t, err)
			require.True(t, dec.engage,
				"interrupted markerless catch-up must re-run with reconcile")
			require.EqualValues(t, 0, dec.start)
		})
}

const immutableTestdataDir = "../database/immutable/testdata"

// testImmutable opens the immutable testdata by pathname. The catch-up checks
// take an open ImmutableDB rather than a directory name so production can hand
// them the handle the bootstrap vetted; these tests have no such handle and
// nothing racing them, so a plain open is what they want.
func testImmutable(t *testing.T) *immutable.ImmutableDb {
	t.Helper()
	imm, err := immutable.New(immutableTestdataDir)
	require.NoError(t, err)
	return imm
}

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
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	slot, hash := firstImmutableBlock(t)

	t.Run("matching tip is accepted", func(t *testing.T) {
		t.Parallel()
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
			t, verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard),
		)
	})

	t.Run("mismatched tip diverges", func(t *testing.T) {
		t.Parallel()
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
		err := verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard)
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

func catchupTestBlock(
	slot uint64,
	hash []byte,
	prevHash []byte,
	number uint64,
) models.Block {
	return models.Block{
		Slot:     slot,
		Hash:     bytes.Clone(hash),
		PrevHash: bytes.Clone(prevHash),
		Cbor:     []byte{0x80},
		Number:   number,
		Type:     6,
	}
}

// TestVerifyCatchupIntersectionLocalAhead pins the fail-closed behavior when
// the local chain tip is above the target artifact's sealed range: catch-up
// must never import an older snapshot over a newer database. A local chain
// descended from the artifact's tip block has nothing to catch up
// (errCatchUpLocalAhead); any other ahead chain diverges and must abort.
func TestVerifyCatchupIntersectionLocalAhead(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	artSlot, artHash := artifactTipBlock(t)
	aheadHash := bytes.Repeat([]byte{0xcc}, 32)
	atArtifactTip := func(hash []byte) models.Block {
		return catchupTestBlock(
			artSlot, hash, bytes.Repeat([]byte{0x01}, 32), 98,
		)
	}

	t.Run("ahead chain with no block at or below the artifact tip diverges",
		func(t *testing.T) {
			t.Parallel()
			db := newSyncModeTestDB(t)
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot+5000,
				aheadHash,
				bytes.Repeat([]byte{0xcd}, 32),
				99,
			), nil))
			err := verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard)
			require.Error(t, err)
			require.NotErrorIs(t, err, errCatchUpLocalAhead)
			require.ErrorContains(t, err, "diverges")
		})

	t.Run("ahead chain without the artifact tip block diverges",
		func(t *testing.T) {
			t.Parallel()
			db := newSyncModeTestDB(t)
			wrong := bytes.Clone(artHash)
			wrong[0] ^= 0xff
			require.NoError(t, db.BlockCreate(atArtifactTip(wrong), nil))
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot+5000, aheadHash, wrong, 99,
			), nil))
			err := verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard)
			require.Error(t, err)
			require.NotErrorIs(t, err, errCatchUpLocalAhead)
			require.ErrorContains(t, err, "diverges")
		})

	t.Run("ahead chain with artifact tip only on stale fork diverges",
		func(t *testing.T) {
			t.Parallel()
			db := newSyncModeTestDB(t)
			wrong := bytes.Clone(artHash)
			wrong[0] ^= 0xff
			require.NoError(t, db.BlockCreate(atArtifactTip(artHash), nil))
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot, wrong, bytes.Repeat([]byte{0x02}, 32), 98,
			), nil))
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot+5000, aheadHash, wrong, 99,
			), nil))
			err := verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard)
			require.Error(t, err)
			require.NotErrorIs(t, err, errCatchUpLocalAhead)
			require.ErrorContains(t, err, "diverges")
		})

	t.Run("descendant chain is reported ahead", func(t *testing.T) {
		t.Parallel()
		db := newSyncModeTestDB(t)
		require.NoError(t, db.BlockCreate(atArtifactTip(artHash), nil))
		require.NoError(t, db.BlockCreate(catchupTestBlock(
			artSlot+5000, aheadHash, artHash, 99,
		), nil))
		err := verifyCatchupIntersection(context.Background(), db, testImmutable(t), discard)
		require.ErrorIs(t, err, errCatchUpLocalAhead)
	})
}

// TestVerifyCatchupBeforeImport pins the Sync-side handling of the
// intersection check: an ancestor tip proceeds with the import without moving
// the marker, a strictly-ahead local chain is reported up-to-date and the
// import marker advances to the target (so later runs no-op without
// re-downloading), and a divergent chain aborts.
func TestVerifyCatchupBeforeImport(t *testing.T) {
	t.Parallel()

	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	artSlot, artHash := artifactTipBlock(t)
	aheadHash := bytes.Repeat([]byte{0xcc}, 32)
	artBlock := catchupTestBlock(
		artSlot, artHash, bytes.Repeat([]byte{0x01}, 32), 98,
	)

	t.Run("ancestor tip proceeds with the import", func(t *testing.T) {
		t.Parallel()
		db := newSyncModeTestDB(t)
		require.NoError(t, db.BlockCreate(artBlock, nil))
		upToDate, err := verifyCatchupBeforeImport(
			context.Background(),
			db, testImmutable(t), 42, false, discard,
		)
		require.NoError(t, err)
		require.False(t, upToDate)
		_, ok, err := getImmutableImportMarker(db)
		require.NoError(t, err)
		require.False(t, ok, "marker must not move before the import runs")
	})

	t.Run("local-ahead marks up to date and records the marker",
		func(t *testing.T) {
			t.Parallel()
			db := newSyncModeTestDB(t)
			require.NoError(t, db.BlockCreate(artBlock, nil))
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot+5000, aheadHash, artHash, 99,
			), nil))
			upToDate, err := verifyCatchupBeforeImport(
				context.Background(),
				db, testImmutable(t), 42, false, discard,
			)
			require.NoError(t, err)
			require.True(t, upToDate)
			marker, ok, err := getImmutableImportMarker(db)
			require.NoError(t, err)
			require.True(t, ok, "marker must be recorded on local-ahead")
			require.EqualValues(t, 42, marker)
		})

	// A resuming run (interrupted sync) must NOT short-circuit on a
	// local-ahead chain: the gap-fill of the interrupted run stored blocks
	// past the artifact's sealed range, and the import must proceed so the
	// completion bookkeeping (sync-state cleanup, deferred index rebuild)
	// runs. The marker must not move either — completion records it.
	t.Run("local-ahead while resuming proceeds without moving the marker",
		func(t *testing.T) {
			t.Parallel()
			db := newSyncModeTestDB(t)
			require.NoError(t, db.BlockCreate(artBlock, nil))
			require.NoError(t, db.BlockCreate(catchupTestBlock(
				artSlot+5000, aheadHash, artHash, 99,
			), nil))
			upToDate, err := verifyCatchupBeforeImport(
				context.Background(),
				db, testImmutable(t), 43, true, discard,
			)
			require.NoError(t, err)
			require.False(
				t, upToDate,
				"resume must fall through to the import, not no-op",
			)
			_, ok, err := getImmutableImportMarker(db)
			require.NoError(t, err)
			require.False(
				t, ok,
				"marker must not move while resuming an interrupted sync",
			)
		})

	t.Run("divergent tip aborts", func(t *testing.T) {
		t.Parallel()
		db := newSyncModeTestDB(t)
		wrong := bytes.Clone(artHash)
		wrong[0] ^= 0xff
		require.NoError(t, db.BlockCreate(catchupTestBlock(
			artSlot, wrong, bytes.Repeat([]byte{0x01}, 32), 98,
		), nil))
		_, err := verifyCatchupBeforeImport(
			context.Background(),
			db, testImmutable(t), 42, false, discard,
		)
		require.Error(t, err)
		require.ErrorContains(t, err, "diverges")
	})
}

type rewardRepairSetCommit struct {
	point   ocommon.Point
	spent   []models.UtxoId
	created []byte
}

// seedRewardRepairSet leaves set as a node's LedgerDB set that holds the
// output txID#0, when txID is set, at state and then applied commits, each
// its own journaled block with a block nonce.
func seedRewardRepairSet(
	t *testing.T,
	set *utxoset.Set,
	state ocommon.Point,
	txID, outCBOR []byte,
	commits []rewardRepairSetCommit,
) {
	t.Helper()
	if outCBOR == nil {
		outCBOR = []byte{0x80}
	}
	require.NoError(t, set.LoadUtxoSet(
		ocommon.NewPoint(state.Slot-1, bytes.Repeat([]byte{0x5f}, 32)),
		func(put func([]byte, uint32, []byte) error) error {
			if txID == nil {
				return nil
			}
			return put(txID, 0, outCBOR)
		},
	))
	require.NoError(t, set.FinishImport())
	commits = append([]rewardRepairSetCommit{{point: state}}, commits...)
	for _, c := range commits {
		tip := c.point
		evt := dledger.UtxoEvent{Point: tip, Removed: c.spent}
		if c.created != nil {
			evt.Added = []dledger.UtxoOutput{{
				Ref: models.UtxoId{Hash: c.created}, Cbor: outCBOR,
			}}
		}
		require.NoError(t, set.ApplyLedgerChanges(&tip, dledger.LedgerChanges{
			Utxo: []dledger.UtxoEvent{evt},
			Nonces: []models.BlockNonce{{
				Slot: tip.Slot, Hash: tip.Hash,
				Nonce: bytes.Repeat([]byte{byte(tip.Slot)}, 32),
			}},
		}))
	}
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, tip.Slot, state.Slot,
		"fixture must leave the set past the state")
}

// requireSetHoldsOnlyState requires set to hold exactly the artifact's state
// at state: that tip, the artifact's outputs live, and no block nonce
// recorded after the state's slot.
func requireSetHoldsOnlyState(
	t *testing.T,
	set *utxoset.Set,
	state ocommon.Point,
	live ...models.UtxoId,
) {
	t.Helper()
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, state.Slot, tip.Slot)
	require.Equal(t, state.Hash, tip.Hash)
	n, err := set.Count()
	require.NoError(t, err)
	require.EqualValues(t, len(live), n,
		"the set holds only the artifact's outputs")
	for _, ref := range live {
		_, found, err := set.Get(ref.Hash, ref.Idx)
		require.NoError(t, err)
		require.True(t, found,
			"an artifact output spent after the state is live again")
	}
	above, err := set.BlockNoncesInSlotRange(state.Slot+1, ^uint64(0))
	require.NoError(t, err)
	require.Empty(t, above, "no block nonce is recorded above the state")
}
