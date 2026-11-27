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
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/forging"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestUpdateMithrilReadyStateKeepsForgeFence pins that the sync-state clear
// ending a Mithril import leaves every pool's last-forged-slot fence in place.
// The fence is written by the forger during normal operation and guards a
// slot whose block may already have been signed and diffused, so a producer
// that re-bootstraps from a snapshot must still refuse it afterwards.
//
// It also pins that the consensus database identity survives the clear.
// Without it the next start reads the database as unmarked Praos and refuses
// it under any other selected provider.
func TestUpdateMithrilReadyStateKeepsForgeFence(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)

	poolA := lcommon.PoolKeyHash{0xA1}
	poolB := lcommon.PoolKeyHash{0xB2}
	const fenceA, fenceB = uint64(12_345), uint64(777)
	storeA := forging.NewChainStateForgeFenceStore(db, poolA)
	storeB := forging.NewChainStateForgeFenceStore(db, poolB)
	require.NoError(t, storeA.StoreLastForgedSlot(fenceA))
	require.NoError(t, storeB.StoreLastForgedSlot(fenceB))

	const identity = "example/v1"
	require.NoError(t, db.SetChainState(
		database.ConsensusProviderIdentityKey, identity, nil,
	))

	ledgerStateHash := bytes.Repeat([]byte{0x77}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))
	require.NoError(t, db.SetChainState("sync_status", "bootstrap", nil))

	require.NoError(t, updateMithrilReadyState(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
	))

	gotIdentity, err := db.GetChainState(
		database.ConsensusProviderIdentityKey, nil,
	)
	require.NoError(t, err)
	require.Equal(t, identity, gotIdentity)

	// A store built after the import is what a restarted node reads.
	for _, tc := range []struct {
		pool lcommon.PoolKeyHash
		want uint64
	}{{poolA, fenceA}, {poolB, fenceB}} {
		restarted := forging.NewChainStateForgeFenceStore(db, tc.pool)
		slot, found, err := restarted.LoadLastForgedSlot()
		require.NoError(t, err)
		require.True(t, found, "the fence must survive the import")
		require.Equal(t, tc.want, slot)
		// A forge at or below the surviving fence cannot lower it.
		require.NoError(t, restarted.StoreLastForgedSlot(tc.want-1))
		slot, _, err = restarted.LoadLastForgedSlot()
		require.NoError(t, err)
		require.Equal(t, tc.want, slot)
	}

	status, err := db.GetChainState("sync_status", nil)
	require.NoError(t, err)
	require.Empty(t, status, "sync_status must still be cleared")
}

// newMithrilFileTestDB is newMithrilTestDB with a data directory, so the
// SQLite catalog can be inspected directly.
func newMithrilFileTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	return db
}

// The mark of how far an immutable copy stored produced-UTxO offsets belongs
// to the import that wrote it, so completion removes it with the other
// transient state.
func TestUpdateMithrilReadyStateClearsTheImmutableOffsetsMark(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	require.NoError(t, node.MarkImmutableUtxoOffsetsComplete(db, 30))
	_, marked, err := node.ImmutableUtxoOffsetsTipSlot(db)
	require.NoError(t, err)
	require.True(t, marked)

	hash := bytes.Repeat([]byte{0x77}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, hash),
	}, nil))
	require.NoError(t, updateMithrilReadyState(
		db,
		slog.New(slog.DiscardHandler),
		nil,
		30,
		hash,
	))
	_, marked, err = node.ImmutableUtxoOffsetsTipSlot(db)
	require.NoError(t, err)
	require.False(t, marked)
}

// An import replaces the chain, so chain state describing the old one must
// not outlive it: the mark of the highest block the old chain held would
// bound rollbacks on the new one.
func TestUpdateMithrilReadyStateClearsImportedChainState(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		database.RollbackHighWaterKey,
		database.PendingTruncateKey,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newMithrilTestDB(t)
			require.NoError(t, db.SetChainState(name, "900", nil))
			hash := bytes.Repeat([]byte{0x77}, 32)
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.NewPoint(30, hash),
			}, nil))
			require.NoError(t, updateMithrilReadyState(
				db,
				slog.New(slog.DiscardHandler),
				nil,
				30,
				hash,
			))
			value, err := db.GetChainState(name, nil)
			require.NoError(t, err)
			require.Empty(t, value)
		})
	}
}

func newMithrilTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbtest.CloseDatabase(db))
	})
	return db
}

func TestUpdateMithrilReadyStateKeepsTrustBoundaryAtStableLedgerTip(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	tipHash := bytes.Repeat([]byte{0x11}, 32)
	ledgerStateHash := bytes.Repeat([]byte{0x22}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(30, ledgerStateHash),
		BlockNumber: 1,
	}, nil))
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     2,
		Slot:   42,
		Hash:   tipHash,
		Number: 2,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))

	require.NoError(t, updateMithrilReadyState(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
	))

	slot, err := db.GetChainState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetChainState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ledgerStateHash), hash)
	storedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), storedTip.Point.Slot)
	require.Equal(t, ledgerStateHash, storedTip.Point.Hash)
}

func TestSetStableMithrilLedgerTipUsesCertifiedBlockNumber(t *testing.T) {
	t.Parallel()

	db := newMithrilTestDB(t)
	ledgerStateHash := bytes.Repeat([]byte{0x23}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     1,
		Slot:   30,
		Hash:   ledgerStateHash,
		Number: 123,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))

	require.NoError(t, setStableMithrilLedgerTip(
		context.Background(),
		db,
		30,
		ledgerStateHash,
	))

	storedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), storedTip.Point.Slot)
	require.Equal(t, ledgerStateHash, storedTip.Point.Hash)
	require.Equal(t, uint64(123), storedTip.BlockNumber)
}

func TestSetStableMithrilLedgerTipRejectsPointOutsideCertifiedChain(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	certifiedHash := bytes.Repeat([]byte{0x24}, 32)
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     1,
		Slot:   30,
		Hash:   certifiedHash,
		Number: 123,
		Type:   1,
		Cbor:   []byte{0x80},
	}, nil))

	err := setStableMithrilLedgerTip(
		context.Background(),
		db,
		30,
		bytes.Repeat([]byte{0x25}, 32),
	)
	require.ErrorContains(t, err, "is not present in certified ImmutableDB")
}

func TestUpdateMithrilReadyStateClearsStaleTrustBoundaryHash(
	t *testing.T,
) {
	t.Parallel()

	db := newMithrilTestDB(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, nil),
	}, nil))
	require.NoError(t, db.SetChainState(
		mithrilLedgerHashSyncKey,
		hex.EncodeToString(bytes.Repeat([]byte{0x44}, 32)),
		nil,
	))

	require.NoError(t, updateMithrilReadyState(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		nil,
	))

	slot, err := db.GetChainState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetChainState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, hash)
}

func TestUpdateMithrilReadyStateKeepsHistoryExpiryCursor(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	hash := bytes.Repeat([]byte{0x55}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, hash),
	}, nil))
	require.NoError(t, db.SetChainState(database.HistoryExpiryCursorKey, "8640", nil))
	require.NoError(t, db.SetChainState("sync_status", "bootstrap", nil))
	require.NoError(t, updateMithrilReadyState(
		db, slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil, 30, hash,
	))
	cursor, err := db.GetChainState(database.HistoryExpiryCursorKey, nil)
	require.NoError(t, err)
	require.Equal(t, "8640", cursor)
	status, err := db.GetChainState("sync_status", nil)
	require.NoError(t, err)
	require.Empty(t, status)
}

func TestUpdateMithrilReadyStateClearsImportProgress(t *testing.T) {
	t.Parallel()
	db := newMithrilTestDB(t)
	hash := bytes.Repeat([]byte{0x61}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, hash),
	}, nil))
	for _, key := range importProgressKeys {
		require.NoError(t, db.SetChainState(key, "1", nil))
	}
	// The boundary of the import this one replaces.
	require.NoError(t, db.SetImportedLedgerStateBoundary(
		ocommon.NewPoint(10, bytes.Repeat([]byte{0x62}, 32)), nil,
	))

	require.NoError(t, updateMithrilReadyState(
		db, slog.New(slog.DiscardHandler), nil, 30, hash,
	))

	for _, key := range importProgressKeys {
		value, err := db.GetChainState(key, nil)
		require.NoError(t, err)
		require.Empty(t, value, "%s outlived the import", key)
	}
	slot, err := db.ImportedLedgerStateBoundarySlotStrict(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), slot)
	gotHash, err := db.ImportedLedgerStateBoundaryHash()
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(hash), gotHash)
}
