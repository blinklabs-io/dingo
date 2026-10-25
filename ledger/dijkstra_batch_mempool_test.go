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
	"context"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	dingomempool "github.com/blinklabs-io/dingo/mempool"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestDijkstraBatchPendingBalanceAgreesWithAppliedBalance admits the batch
// TestDijkstraBatchApplyAgreesAcrossLedgerPaths applies, child withdrawals and
// direct deposit included, into a mempool validating through a real
// LedgerState. A later pending withdrawal must see the 15 the applied batch
// leaves, not the stored 100 nor a balance that skips the child levels.
func TestDijkstraBatchPendingBalanceAgreesWithAppliedBalance(t *testing.T) {
	t.Parallel()
	stakeKey := bytes.Repeat([]byte{0xe1}, 28)
	addr := stateRewardAddress(stakeKey)
	childIn, childIn2, topIn := batchRef{
		seed: 0xe2,
	}, batchRef{
		seed: 0xe3,
	}, batchRef{
		seed: 0xe4,
	}
	batch := buildStateBatch(t,
		[]batchLevel{
			{
				inputs:      []batchRef{childIn},
				outputs:     []uint64{1_000_000},
				withdrawals: map[string]uint64{addr: 30},
			},
			{
				inputs:        []batchRef{childIn2},
				outputs:       []uint64{1_100_000},
				withdrawals:   map[string]uint64{addr: 20},
				directDeposit: map[string]uint64{addr: 5},
			},
		},
		batchLevel{
			inputs:      []batchRef{topIn},
			outputs:     []uint64{1_300_000},
			fee:         7,
			withdrawals: map[string]uint64{addr: 40},
		},
		false,
	)
	later := func(seed byte, amount uint64) []byte {
		tx := buildStateBatch(t, nil, batchLevel{
			inputs:      []batchRef{{seed: seed}},
			outputs:     []uint64{1_000_000},
			fee:         7,
			withdrawals: map[string]uint64{addr: amount},
		}, false)
		return tx.Cbor()
	}

	db := newTestDB(t)
	require.NoError(t, db.CreateAccount(context.Background(), nil, &models.Account{
		StakingKey: stakeKey,
		AddedSlot:  1,
		Reward:     dbtypes.Uint64(100),
		Active:     true,
	}))
	era := eras.EraDesc{
		Id:   eras.DijkstraEraDesc.Id,
		Name: "batch-withdrawal-rule",
		ValidateTxFunc: func(
			tx lcommon.Transaction,
			slot uint64,
			state lcommon.LedgerState,
			pp lcommon.ProtocolParameters,
		) error {
			return dijkstra.UtxoValidateBatchWithdrawals(tx, slot, state, pp)
		},
	}
	tip := ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0xf2}, 32)},
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{era},
		currentEra: era,
		currentEpoch: models.Epoch{
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         era.Id,
		},
		currentPParams:    dijkstraTestProtocolParameters(),
		currentTip:        tip,
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
		Validator:       ls,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})
	txType := uint(gledger.TxTypeDijkstra)
	require.NoError(
		t,
		pool.AddTransaction(context.Background(), txType, later(0xf1, 100)),
		"control: the stored balance covers a full withdrawal",
	)
	pool.RemoveTxsByHash([]string{pool.Transactions()[0].Hash})

	require.NoError(t, pool.AddTransaction(context.Background(), txType, batch.Cbor()))
	var exceeds dijkstra.WithdrawalsExceedAccountBalanceError
	require.ErrorAs(
		t,
		pool.AddTransaction(context.Background(), txType, later(0xf3, 16)),
		&exceeds,
		"the pending batch leaves 15 after its child levels",
	)
	for _, amounts := range exceeds.Withdrawals {
		require.Equal(t, []uint64{16, 15}, amounts)
	}
	require.NoError(t, pool.AddTransaction(context.Background(), txType, later(0xf4, 15)))
	require.Len(t, pool.Transactions(), 2)
}

// TestMempoolIndexesConfirmedInputsThroughLedgerSession admits a Dijkstra
// batch into a mempool validating through a real LedgerState. The batch
// spends a stored output at its top level and another from a sub-transaction,
// and the pool indexes it under both owners through the session's resolver.
// An input with no stored output, which this era's partial rule set accepts,
// is left out instead of stalling admission.
func TestMempoolIndexesConfirmedInputsThroughLedgerSession(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	owner := func(seed byte) lcommon.Address {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			lcommon.AddressNetworkTestnet,
			bytes.Repeat([]byte{seed}, lcommon.AddressHashSize),
			nil,
		)
		require.NoError(t, err)
		return addr
	}
	store := func(ref batchRef, addr lcommon.Address) {
		require.NoError(t, db.CreateUtxo(context.Background(), nil, &models.Utxo{
			TxId:       ref.txID(),
			OutputIdx:  uint32(ref.index), // #nosec G115 -- test index
			PaymentKey: addr.PaymentKeyHash().Bytes(),
			Amount:     5_000_000,
			AddedSlot:  1,
		}))
		raw, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
			OutputAddress: addr,
			OutputAmount:  5_000_000,
		})
		require.NoError(t, err)
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			return db.Blob().SetUtxo(
				txn.Blob(), ref.txID(), uint32(ref.index), raw, // #nosec G115 -- test index
			)
		}))
	}
	topIn, childIn, missing := batchRef{seed: 0xa1}, batchRef{seed: 0xa2},
		batchRef{seed: 0xa3}
	topOwner, childOwner := owner(0xb1), owner(0xb2)
	store(topIn, topOwner)
	store(childIn, childOwner)
	batch := buildStateBatch(t,
		[]batchLevel{{inputs: []batchRef{childIn}, outputs: []uint64{1_000_000}}},
		batchLevel{
			inputs:  []batchRef{topIn, missing},
			outputs: []uint64{1_000_000},
			fee:     7,
		},
		false,
	)

	era := eras.EraDesc{
		Id:   eras.DijkstraEraDesc.Id,
		Name: "no-input-rules",
		ValidateTxFunc: func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error {
			return nil
		},
	}
	tip := ochainsync.Tip{
		Point: ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0xf2}, 32)},
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls := &LedgerState{
		db:         db,
		activeEras: []eras.EraDesc{era},
		currentEra: era,
		currentEpoch: models.Epoch{
			SlotLength:    1,
			LengthInSlots: 1_000,
			EraId:         era.Id,
		},
		currentPParams:    dijkstraTestProtocolParameters(),
		currentTip:        tip,
		validationEnabled: true,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	pool, err := dingomempool.NewMempool(dingomempool.MempoolConfig{
		Validator:       ls,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:    prometheus.NewRegistry(),
		MempoolCapacity: 1 << 20,
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, pool.Stop(ctx))
	})

	require.NoError(t, pool.AddTransaction(
		context.Background(), uint(gledger.TxTypeDijkstra), batch.Cbor(),
	))
	hash := batch.Hash().String()
	require.Equal(t, []string{hash},
		pool.TransactionHashesByAddress(topOwner.String()))
	require.Equal(t, []string{hash},
		pool.TransactionHashesByAddress(childOwner.String()))
}
