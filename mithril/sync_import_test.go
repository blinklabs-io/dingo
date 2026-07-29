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
	"encoding/hex"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

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

func TestEnsureMithrilBackfillCheckpointCreatesMissing(t *testing.T) {
	db := newMithrilTestDB(t)

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(0), cp.LastSlot)
	require.False(t, cp.Completed)
	require.False(t, cp.StartedAt.IsZero())
	require.False(t, cp.UpdatedAt.IsZero())
}

func TestEnsureMithrilBackfillCheckpointPreservesIncomplete(t *testing.T) {
	db := newMithrilTestDB(t)
	startedAt := time.Now().Add(-time.Hour)
	updatedAt := time.Now().Add(-time.Minute)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      node.BackfillPhase,
			LastSlot:   1042527,
			TotalSlots: 2000000,
			StartedAt:  startedAt,
			UpdatedAt:  updatedAt,
			Completed:  false,
		},
		nil,
	))

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(1042527), cp.LastSlot)
	require.Equal(t, uint64(2000000), cp.TotalSlots)
	require.False(t, cp.Completed)
	require.Equal(t, startedAt.UnixNano(), cp.StartedAt.UnixNano())
	require.Equal(t, updatedAt.UnixNano(), cp.UpdatedAt.UnixNano())
}

func TestEnsureMithrilBackfillCheckpointReopensCompleted(t *testing.T) {
	db := newMithrilTestDB(t)
	startedAt := time.Now().Add(-time.Hour)
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      node.BackfillPhase,
			LastSlot:   5000,
			TotalSlots: 5000,
			StartedAt:  startedAt,
			UpdatedAt:  startedAt,
			Completed:  true,
		},
		nil,
	))

	require.NoError(t, ensureMithrilBackfillCheckpoint(db))

	cp, err := db.Metadata().GetBackfillCheckpoint(
		node.BackfillPhase, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, uint64(5000), cp.LastSlot)
	require.Equal(t, uint64(5000), cp.TotalSlots)
	require.False(t, cp.Completed)
	require.Equal(t, startedAt.UnixNano(), cp.StartedAt.UnixNano())
	require.True(t, cp.UpdatedAt.After(startedAt))
}

func TestUpdateMithrilReadyStateKeepsTrustBoundaryAtStableLedgerTip(
	t *testing.T,
) {
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
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ledgerStateHash), hash)
	storedTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), storedTip.Point.Slot)
	require.Equal(t, ledgerStateHash, storedTip.Point.Hash)
}

func TestSetStableMithrilLedgerTipUsesCertifiedBlockNumber(t *testing.T) {
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
		db,
		30,
		bytes.Repeat([]byte{0x25}, 32),
	)
	require.ErrorContains(t, err, "is not present in certified ImmutableDB")
}

func TestUpdateMithrilReadyStateStoresTrustBoundaryFromLedgerState(
	t *testing.T,
) {
	db := newMithrilTestDB(t)
	ledgerStateHash := bytes.Repeat([]byte{0x33}, 32)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, ledgerStateHash),
	}, nil))

	require.NoError(t, updateMithrilReadyState(
		db,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		30,
		ledgerStateHash,
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(ledgerStateHash), hash)
}

func TestUpdateMithrilReadyStateClearsStaleTrustBoundaryHash(
	t *testing.T,
) {
	db := newMithrilTestDB(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(30, nil),
	}, nil))
	require.NoError(t, db.SetSyncState(
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
		"",
		true,
	))

	slot, err := db.GetSyncState(mithrilLedgerSlotSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "30", slot)
	hash, err := db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, hash)
}
