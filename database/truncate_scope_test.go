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

package database_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// stubNonceSource answers block nonce reads with a fixed nonce.
type stubNonceSource struct {
	nonce         []byte
	hasCheckpoint bool
}

func (s stubNonceSource) BlockNonceAt(ocommon.Point) ([]byte, error) {
	return s.nonce, nil
}

func (s stubNonceSource) HasCheckpointAtOrBefore(uint64) (bool, error) {
	return s.hasCheckpoint, nil
}

// newTruncateScopeDB opens a database of role behind guard with a target
// block and a block after it.
func newTruncateScopeDB(
	t *testing.T,
	role string,
	guard *dbtest.SQLRefusal,
) (*database.Database, ocommon.Point) {
	t.Helper()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir(), Role: role},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	target := models.Block{
		ID: 1, Slot: 1500, Hash: bytes.Repeat([]byte{0x15}, 32),
		Cbor: []byte{0x80}, Number: 1, Type: babbage.BlockTypeBabbage,
	}
	later := models.Block{
		ID: 2, Slot: 1600, Hash: bytes.Repeat([]byte{0x16}, 32),
		PrevHash: target.Hash, Cbor: []byte{0x80}, Number: 2,
		Type: babbage.BlockTypeBabbage,
	}
	require.NoError(t, db.BlockCreate(target, nil))
	require.NoError(t, db.BlockCreate(later, nil))
	return db, ocommon.Point{Slot: target.Slot, Hash: target.Hash}
}

// The ledger's rollback moves the tip and reads the target's nonce from the
// LedgerDB, with no metadata store to issue a statement on.
func TestTruncateAfterSlotRevertsTheBlockStoreAlone(t *testing.T) {
	t.Parallel()
	guard := &dbtest.SQLRefusal{}
	db, point := newTruncateScopeDB(t, "", guard)
	want := bytes.Repeat([]byte{0xc2}, 32)
	db.SetNonceSource(stubNonceSource{nonce: want, hasCheckpoint: true})
	guard.Arm()

	tip, nonce, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.NoError(t, err)
	require.Empty(t, guard.Statements())
	require.Equal(t, point, tip.Point)
	require.Equal(t, want, nonce)
	stored, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, point, stored.Point)
	require.Nil(t, db.Metadata(), "the ledger's database keeps no SQL rows")
}

// The sweep of a v0.94.2 database's rows removes what the chain-only rollback
// leaves, and refuses a transaction that could not commit the rows together.
func TestRevertStoredLedgerAfterSlotRevertsTheRows(t *testing.T) {
	t.Parallel()
	db, point := newTruncateScopeDB(
		t, database.RoleReleased, &dbtest.SQLRefusal{},
	)
	require.NoError(t, db.SetEpoch(
		1600, 7, nil, nil, nil, nil, 1, 1, 100, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		point.Hash, point.Slot, bytes.Repeat([]byte{0xc2}, 32), true, nil,
	))

	blobOnly := db.BlobTxn(true)
	_, _, err := db.RevertStoredLedgerAfterSlot(
		t.Context(), point, 0, blobOnly,
	)
	require.ErrorIs(t, err, types.ErrNilTxn)
	require.NoError(t, blobOnly.Rollback())
	require.ErrorIs(
		t,
		db.RollbackMetadataAfterSlot(t.Context(), point, db.BlobTxn(true)),
		types.ErrNilTxn,
	)

	tip, _, err := db.RevertStoredLedgerAfterSlot(t.Context(), point, 0, nil)
	require.NoError(t, err)
	require.Equal(t, point, tip.Point)
	epochs, err := db.GetEpochs(nil)
	require.NoError(t, err)
	for _, e := range epochs {
		require.LessOrEqual(t, e.StartSlot, point.Slot)
	}
}
