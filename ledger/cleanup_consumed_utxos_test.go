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
// run in core mode.
func newLedgerStateForCleanup(
	db *database.Database,
	tipSlot uint64,
) *LedgerState {
	return &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, nil),
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
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
