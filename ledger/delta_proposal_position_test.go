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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// TestLedgerDeltaRecordsProposalBlockPosition applies one block holding two
// proposal transactions, the second with the lower hash, through the live
// delta path. Each proposal records its transaction's position in the block,
// which is the order Conway RATIFY needs for equal-priority actions.
func TestLedgerDeltaRecordsProposalBlockPosition(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	pparams := mockledger.NewMockConwayProtocolParams()
	pparams.GovActionValidityPeriod = 20
	pparams.DRepInactivityPeriod = 20
	ls := &LedgerState{
		db:             db,
		currentEpoch:   models.Epoch{EpochId: 12},
		currentPParams: &pparams,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, bytes.Repeat([]byte{0xAB}, 28)...),
	)
	require.NoError(t, err)
	proposalTx := func(seed byte) lcommon.Transaction {
		procedure, err := conway.NewConwayProposalProcedure(
			42,
			rewardAddress,
			&lcommon.InfoGovAction{Type: uint(lcommon.GovActionTypeInfo)},
			lcommon.GovAnchor{Url: "https://example.com/position"},
		)
		require.NoError(t, err)
		tx := mockledger.NewTransactionBuilder()
		tx.WithId(bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size))
		tx.WithType(gledger.TxTypeConway)
		tx.WithProposalProcedures(procedure)
		tx.WithValid(true)
		return tx
	}
	first, second := proposalTx(0x52), proposalTx(0x51)

	delta := NewLedgerDelta(
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x22}, 32)),
		uint(conway.EraIdConway),
		1,
	)
	defer delta.Release()
	delta.addTransaction(first, 0)
	delta.addTransaction(second, 1)
	txOffsets := make(map[[32]byte]database.CborOffset)
	for _, tx := range []lcommon.Transaction{first, second} {
		var hash [32]byte
		copy(hash[:], tx.Hash().Bytes())
		txOffsets[hash] = database.CborOffset{}
	}
	delta.Offsets = &database.BlockIngestionResult{
		TxOffsets:   txOffsets,
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	for position, tx := range []lcommon.Transaction{first, second} {
		stored, err := db.GetGovernanceProposal(tx.Hash().Bytes(), 0, nil)
		require.NoError(t, err)
		require.NotNil(t, stored.TxIndex)
		require.Equal(t, uint32(position), *stored.TxIndex) //nolint:gosec
	}
}
