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

package ledger

import (
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// newTestDBForCleanup builds an in-memory Database in the requested storage
// mode. An empty mode defaults to core (per Database.New).
func newTestDBForCleanup(t *testing.T, mode string) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     "",
		StorageMode: mode,
	})
	require.NoError(t, err)
	return db
}

// seedSpentUtxoForCleanup writes a single spent UTxO row directly. The
// row is "consumed at deletedSlot but still well within the periodic
// cleanup eligibility window" — matching what a normal block application
// would produce after the consumed input was soft-marked.
func seedSpentUtxoForCleanup(
	t *testing.T,
	db *database.Database,
	txId []byte,
	outputIdx uint32,
	addedSlot, deletedSlot uint64,
) {
	t.Helper()
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		return db.CreateUtxo(txn, &models.Utxo{
			TxId:        txId,
			OutputIdx:   outputIdx,
			AddedSlot:   addedSlot,
			DeletedSlot: deletedSlot,
			Amount:      types.Uint64(1),
		})
	}))
}

// newLedgerStateForCleanup wires the minimum surface area
// cleanupConsumedUtxos needs: db, currentTip, currentEra, and a logger.
// CardanoNodeConfig is intentionally left nil so
// calculateStabilityWindowForEra returns the default
// (blockfetchBatchSlotThresholdDefault = 50000); the tip slot is then
// chosen well past that window so consumed-UTxO cleanup is eligible to
// run in core mode. The upstream tip is initialized to the local tip so the
// test represents a node that is near the network tip.
func newLedgerStateForCleanup(
	db *database.Database,
	tipSlot uint64,
) *LedgerState {
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, nil),
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.syncUpstreamTipSlot.Store(tipSlot)
	return ls
}

// TestCleanupConsumedUtxos_CoreModePrunes asserts the pre-existing
// invariant that core mode hard-deletes consumed UTxO rows once the
// stability window has passed. Without this baseline, the API-mode
// retention test below could pass by accident if the cleanup loop were
// silently dead for both modes.
func TestCleanupConsumedUtxos_CoreModePrunes(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA1}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"core mode must hard-delete consumed UTxO rows after stability "+
			"window",
	)
}

func TestCleanupConsumedUtxos_ProcessesOneBoundedBatch(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeCore)
	mdTxn := db.MetadataTxn(true)
	require.NoError(t, mdTxn.Do(func(txn *database.Txn) error {
		for idx := 0; idx <= cleanupConsumedUtxoBatchSize; idx++ {
			txID := bytes.Repeat([]byte{0}, 32)
			binary.BigEndian.PutUint32(txID[:4], uint32(idx+1))
			if err := db.CreateUtxo(txn, &models.Utxo{
				TxId:        txID,
				OutputIdx:   0,
				AddedSlot:   1_000,
				DeletedSlot: 5_000,
				Amount:      types.Uint64(1),
			}); err != nil {
				return err
			}
		}
		return nil
	}))

	ls := newLedgerStateForCleanup(db, 100_000)
	ls.cleanupConsumedUtxos()

	remaining, err := db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Len(t, remaining, 1, "one eligible row must remain for a later run")

	ls.cleanupConsumedUtxos()
	remaining, err = db.Metadata().GetUtxosDeletedBeforeSlot(
		50_000,
		cleanupConsumedUtxoBatchSize+1,
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, remaining, "the next run must resume the bounded cleanup")
}

func TestCleanupConsumedUtxos_DefersDuringCatchup(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xA3}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
		upstreamTip uint64 = 200_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.syncUpstreamTipSlot.Store(upstreamTip)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, post, "cleanup must defer while the ledger is catching up")
}

// TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip covers the
// distinction the catch-up deferral has to make: an upstream tip of 0 means
// unknown, not "infinitely far behind". A node that has never connected to a
// peer -- or that lost its last active connection, which zeroes the value in
// chainsync.go -- would otherwise defer cleanup for as long as it stays
// peerless, growing the utxo table without bound in core mode, and silently:
// no error, no crash. Cleanup ran off the local tip alone before the deferral
// existed, so that is the behavior an unknown upstream tip falls back to.
func TestCleanupConsumedUtxos_RunsWithoutKnownUpstreamTip(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xB4}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		// Far past the 50_000 default stability window, so the only
		// reason to retain the row would be the deferral itself.
		tipSlot uint64 = 10_000_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	pre, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, pre, "seed must succeed before cleanup")

	ls := newLedgerStateForCleanup(db, tipSlot)
	// No peer has ever reported a tip.
	ls.syncUpstreamTipSlot.Store(0)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(
		t, post,
		"an unknown upstream tip must not defer cleanup: the local tip "+
			"is already far past the stability window",
	)
}

// TestCleanupConsumedUtxos_APIModeRetains is the regression fix for
// issue #2350: in API storage mode the periodic cleanup must leave
// spent UTxO metadata rows in place so historical transaction queries
// can resolve input / collateral / reference-input associations via
// spent_at_tx_id, collateral_by_tx_id, and referenced_by_tx_id.
func TestCleanupConsumedUtxos_APIModeRetains(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeAPI)
	txId := bytes.Repeat([]byte{0xA2}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t, post,
		"API mode must retain spent UTxO row past the cleanup threshold "+
			"so historical transaction queries can still resolve input / "+
			"collateral / reference-input associations",
	)
	assert.Equal(
		t, deletedSlot, post.DeletedSlot,
		"retained row must keep deleted_slot as the spent-state encoding",
	)

	// Live-UTxO queries must still filter the retained row out: it has
	// a non-zero deleted_slot so it is no longer part of the active set.
	live, err := db.Metadata().GetUtxo(txId, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, live,
		"live UTxO view must continue to exclude spent rows in API mode")
}
