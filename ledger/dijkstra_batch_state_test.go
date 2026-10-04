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
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// batchLevel is the subset of a Dijkstra body these tests vary. Zero values
// are omitted from the encoded body.
type batchLevel struct {
	inputs        []batchRef
	outputs       []uint64
	withdrawals   map[string]uint64
	directDeposit map[string]uint64
	donation      uint64
	collateral    []batchRef
	collateralRet uint64
	fee           uint64
}

type batchRef struct {
	seed  byte
	index uint64
}

func (r batchRef) txID() []byte { return bytes.Repeat([]byte{r.seed}, 32) }

func (l batchLevel) encode(t *testing.T, top bool) map[uint]any {
	t.Helper()
	address := append([]byte{0x60}, bytes.Repeat([]byte{0x42}, 28)...)
	inputs := make([]any, 0, len(l.inputs))
	for _, in := range l.inputs {
		inputs = append(inputs, []any{in.txID(), in.index})
	}
	outputs := make([]any, 0, len(l.outputs))
	for _, amount := range l.outputs {
		outputs = append(outputs, map[uint]any{0: address, 1: amount})
	}
	body := map[uint]any{0: inputs, 1: outputs}
	if top {
		body[2] = l.fee
	}
	if len(l.withdrawals) > 0 {
		withdrawals := make(map[cbor.ByteString]uint64)
		for addr, amount := range l.withdrawals {
			withdrawals[cbor.NewByteString([]byte(addr))] = amount
		}
		body[5] = withdrawals
	}
	if len(l.directDeposit) > 0 {
		deposits := make(map[cbor.ByteString]uint64)
		for addr, amount := range l.directDeposit {
			deposits[cbor.NewByteString([]byte(addr))] = amount
		}
		body[25] = deposits
	}
	if l.donation > 0 {
		body[22] = l.donation
	}
	if len(l.collateral) > 0 {
		collateral := make([]any, 0, len(l.collateral))
		for _, in := range l.collateral {
			collateral = append(collateral, []any{in.txID(), in.index})
		}
		body[13] = collateral
	}
	if l.collateralRet > 0 {
		body[16] = map[uint]any{0: address, 1: l.collateralRet}
	}
	return body
}

func buildStateBatch(
	t *testing.T,
	children []batchLevel,
	top batchLevel,
	invalid bool,
) *dijkstra.DijkstraTransaction {
	t.Helper()
	encodedChildren := make([]cbor.RawMessage, 0, len(children))
	for _, child := range children {
		body, err := cbor.Encode(child.encode(t, false))
		require.NoError(t, err)
		encoded, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, nil})
		require.NoError(t, err)
		encodedChildren = append(encodedChildren, encoded)
	}
	topBody := top.encode(t, true)
	if len(encodedChildren) > 0 {
		topBody[23] = cbor.NewSetType(encodedChildren, true)
	}
	body, err := cbor.Encode(topBody)
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{cbor.RawMessage(body), map[uint]any{}, true, nil})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	dijkstraTx, ok := tx.(*dijkstra.DijkstraTransaction)
	require.True(t, ok)
	// The wire format cannot carry is_valid=false, so a phase-2 failure only
	// exists in-process, set after decoding.
	dijkstraTx.TxIsValid = !invalid
	return dijkstraTx
}

type stateBatchHarness struct {
	db     *database.Database
	ls     *LedgerState
	slot   uint64
	hashID byte
}

func newStateBatchHarness(t *testing.T) *stateBatchHarness {
	t.Helper()
	db := newTestDB(t)
	return &stateBatchHarness{
		db: db,
		ls: &LedgerState{
			db:             db,
			currentPParams: &dijkstra.DijkstraProtocolParameters{},
			config: LedgerStateConfig{
				CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			},
		},
		slot: 10,
	}
}

func (h *stateBatchHarness) seedUtxo(t *testing.T, ref batchRef, amount uint64) {
	t.Helper()
	require.NoError(t, h.db.Transaction(true).Do(func(txn *database.Txn) error {
		return h.db.CreateUtxo(txn, &models.Utxo{
			TxId:       ref.txID(),
			OutputIdx:  uint32(ref.index), //nolint:gosec
			PaymentKey: bytes.Repeat([]byte{0x42}, 28),
			AddedSlot:  1,
			Amount:     dbtypes.Uint64(amount),
		})
	}))
}

func (h *stateBatchHarness) seedAccount(
	t *testing.T,
	stakeKey []byte,
	reward uint64,
) {
	t.Helper()
	require.NoError(t, h.db.CreateAccount(nil, &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		AddedSlot:     1,
		Reward:        dbtypes.Uint64(reward),
		Active:        true,
	}))
}

// block wraps tx in a one-transaction block and computes its offsets. A
// phase-2-invalid tx is indexed as valid, because the wire format cannot carry
// the flag, and its collateral-return offsets are filled in afterwards.
func (h *stateBatchHarness) block(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
) (*dijkstra.DijkstraBlock, ocommon.Point, *database.BlockIngestionResult) {
	t.Helper()
	wasValid := tx.TxIsValid
	tx.TxIsValid = true
	block := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber:  1,
					Slot:         h.slot,
					ProtoVersion: babbage.BabbageProtoVersion{Major: 12},
				},
			},
		},
		BlockBody: dijkstra.DijkstraBlockBody{
			Transactions: []dijkstra.DijkstraTransaction{*tx},
		},
	}
	bodyCbor, err := block.BlockBody.MarshalCBOR()
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = uint64(len(bodyCbor))
	blockCbor, err := block.MarshalCBOR()
	require.NoError(t, err)
	block.SetCbor(blockCbor)
	blockHash := bytes.Repeat([]byte{0x70 + h.hashID}, 32)
	h.hashID++
	offsets, err := database.NewBlockIndexer(h.slot, blockHash).
		ComputeOffsets(blockCbor, block)
	require.NoError(t, err)
	tx.TxIsValid = wasValid
	if !wasValid {
		var sample database.CborOffset
		for _, offset := range offsets.UtxoOffsets {
			sample = offset
			break
		}
		for _, utxo := range tx.Produced() {
			var id [32]byte
			copy(id[:], tx.Hash().Bytes())
			offsets.UtxoOffsets[database.UtxoRef{
				TxId:      id,
				OutputIdx: uint32(utxo.Id.Index()),
			}] = sample
		}
	}
	return block, ocommon.Point{Slot: h.slot, Hash: blockHash}, offsets
}

// apply applies tx as one ledger delta. It returns the delta so a caller can
// inspect accumulated donations.
func (h *stateBatchHarness) apply(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
) (*LedgerDelta, error) {
	t.Helper()
	_, point, offsets := h.block(t, tx)
	delta := NewLedgerDelta(point, uint(dijkstra.EraIdDijkstra), 1)
	delta.Offsets = offsets
	delta.addTransaction(tx, 0)
	t.Cleanup(delta.Release)
	err := h.db.Transaction(true).Do(func(txn *database.Txn) error {
		return delta.applyWithoutRecordingDonations(h.ls, txn)
	})
	return delta, err
}

// process runs tx through the block-processing path. With validation off, as
// in replay, the path hands back an unapplied delta, which is applied here;
// with validation on, as for live blocks, it applies each delta itself.
func (h *stateBatchHarness) process(
	t *testing.T,
	tx *dijkstra.DijkstraTransaction,
	validate bool,
) error {
	t.Helper()
	block, point, offsets := h.block(t, tx)
	pparams := dijkstraTestProtocolParameters()
	pparams.MaxBlockBodySize = 100_000
	pparams.MaxBlockHeaderSize = 100_000
	h.ls.config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	h.ls.config.SkipDijkstraTxValidation = true
	h.ls.config.CardanoNodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	return h.db.Transaction(true).Do(func(txn *database.Txn) error {
		delta, err := h.ls.ledgerProcessBlock(
			txn, point, block,
			validate, false, false,
			nil, envelopeParent{}, offsets,
			eras.DijkstraEraDesc, pparams, nil,
			0, 0, false,
		)
		if err != nil || validate {
			if delta != nil {
				delta.Release()
			}
			return err
		}
		defer delta.Release()
		return delta.apply(h.ls, txn)
	})
}

func (h *stateBatchHarness) utxoLive(t *testing.T, txID []byte, index uint32) bool {
	t.Helper()
	utxo, err := h.db.Metadata().GetUtxo(txID, index, nil)
	require.NoError(t, err)
	return utxo != nil
}

func (h *stateBatchHarness) reward(t *testing.T, stakeKey []byte) uint64 {
	t.Helper()
	account, err := h.db.GetAccountByCredential(0, stakeKey, false, nil)
	require.NoError(t, err)
	return uint64(account.Reward)
}

func stateRewardAddress(stakeKey []byte) string {
	return string(append([]byte{0xe0}, stakeKey...))
}

func childBodyID(tx *dijkstra.DijkstraTransaction, idx int) []byte {
	id := tx.Body.TxSubTransactions.Items()[idx].Body.Id()
	return id.Bytes()
}

func TestDijkstraBatchApplyConsumesAndCreatesEveryLevel(t *testing.T) {
	t.Parallel()
	h := newStateBatchHarness(t)
	childIn := batchRef{seed: 0x81}
	childIn2 := batchRef{seed: 0x82}
	topIn := batchRef{seed: 0x83}
	for _, ref := range []batchRef{childIn, childIn2, topIn} {
		h.seedUtxo(t, ref, 5_000_000)
	}
	tx := buildStateBatch(t,
		[]batchLevel{
			{inputs: []batchRef{childIn}, outputs: []uint64{1_000_000, 1_100_000}},
			{inputs: []batchRef{childIn2}, outputs: []uint64{1_200_000}},
		},
		batchLevel{inputs: []batchRef{topIn}, outputs: []uint64{1_300_000}, fee: 7},
		false,
	)
	_, err := h.apply(t, tx)
	require.NoError(t, err)

	for _, ref := range []batchRef{childIn, childIn2, topIn} {
		require.False(t, h.utxoLive(t, ref.txID(), 0), "input %x must be spent", ref.seed)
	}
	require.True(t, h.utxoLive(t, childBodyID(tx, 0), 0))
	require.True(t, h.utxoLive(t, childBodyID(tx, 0), 1))
	require.True(t, h.utxoLive(t, childBodyID(tx, 1), 0))
	require.True(t, h.utxoLive(t, tx.Hash().Bytes(), 0))
	require.False(t, h.utxoLive(t, tx.Hash().Bytes(), 1))

	// Only the enclosing body pays a fee.
	for idx := range 2 {
		child, err := h.db.Metadata().GetTransactionByHash(childBodyID(tx, idx), nil)
		require.NoError(t, err)
		require.Zero(t, child.Fee)
	}
	top, err := h.db.Metadata().GetTransactionByHash(tx.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Equal(t, dbtypes.Uint64(7), top.Fee)
}

func TestDijkstraPhase2InvalidBatchAppliesOnlyCollateral(t *testing.T) {
	t.Parallel()
	h := newStateBatchHarness(t)
	childIn := batchRef{seed: 0x91}
	topIn := batchRef{seed: 0x92}
	collateral := batchRef{seed: 0x93}
	stakeKey := bytes.Repeat([]byte{0x94}, 28)
	h.seedAccount(t, stakeKey, 100)
	for _, ref := range []batchRef{childIn, topIn, collateral} {
		h.seedUtxo(t, ref, 5_000_000)
	}
	tx := buildStateBatch(t,
		[]batchLevel{{
			inputs:      []batchRef{childIn},
			outputs:     []uint64{1_000_000},
			withdrawals: map[string]uint64{stateRewardAddress(stakeKey): 40},
			donation:    11,
		}},
		batchLevel{
			inputs:        []batchRef{topIn},
			outputs:       []uint64{1_300_000},
			collateral:    []batchRef{collateral},
			collateralRet: 4_000_000,
			directDeposit: map[string]uint64{stateRewardAddress(stakeKey): 9},
			donation:      13,
		},
		true,
	)
	delta, err := h.apply(t, tx)
	require.NoError(t, err)

	require.True(t, h.utxoLive(t, childIn.txID(), 0), "child input stays unspent")
	require.True(t, h.utxoLive(t, topIn.txID(), 0), "top-level input stays unspent")
	require.False(t, h.utxoLive(t, collateral.txID(), 0), "collateral is consumed")
	require.False(t, h.utxoLive(t, childBodyID(tx, 0), 0), "no child output is created")
	require.False(t, h.utxoLive(t, tx.Hash().Bytes(), 0), "no regular output is created")
	require.True(t, h.utxoLive(t, tx.Hash().Bytes(), 1), "collateral return is created")
	require.Equal(t, uint64(100), h.reward(t, stakeKey))
	require.Zero(t, delta.donation)
}

func TestDijkstraBatchRollbackAndReapplyRestoresEveryLevel(t *testing.T) {
	t.Parallel()
	h := newStateBatchHarness(t)
	require.NoError(t, h.db.BlockCreate(models.Block{
		Slot: 1,
		Hash: bytes.Repeat([]byte{0x01}, 32),
		Type: gledger.BlockTypeDijkstra,
	}, nil))
	require.NoError(t, h.db.SetBlockNonce(
		bytes.Repeat([]byte{0x01}, 32),
		1,
		bytes.Repeat([]byte{0x02}, 32),
		true,
		nil,
	))
	childIn := batchRef{seed: 0xa1}
	topIn := batchRef{seed: 0xa2}
	for _, ref := range []batchRef{childIn, topIn} {
		h.seedUtxo(t, ref, 5_000_000)
	}
	tx := buildStateBatch(t,
		[]batchLevel{{inputs: []batchRef{childIn}, outputs: []uint64{1_000_000}}},
		batchLevel{inputs: []batchRef{topIn}, outputs: []uint64{1_300_000}},
		false,
	)
	snapshot := func() []bool {
		return []bool{
			h.utxoLive(t, childIn.txID(), 0),
			h.utxoLive(t, topIn.txID(), 0),
			h.utxoLive(t, childBodyID(tx, 0), 0),
			h.utxoLive(t, tx.Hash().Bytes(), 0),
		}
	}
	applied := []bool{false, false, true, true}
	before := snapshot()
	require.Equal(t, []bool{true, true, false, false}, before)

	_, err := h.apply(t, tx)
	require.NoError(t, err)
	require.Equal(t, applied, snapshot())

	require.NoError(t, h.db.Transaction(true).Do(func(txn *database.Txn) error {
		_, _, err := h.db.TruncateAfterSlot(
			ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{0x01}, 32)},
			0,
			txn,
		)
		return err
	}))
	require.Equal(t, before, snapshot(), "rollback restores inputs and drops every level's outputs")

	_, err = h.apply(t, tx)
	require.NoError(t, err)
	require.Equal(t, applied, snapshot(), "reapply reaches the same UTxO set")
}

func TestDijkstraBatchChildWithdrawalThenTopLevelWithdrawal(t *testing.T) {
	t.Parallel()
	h := newStateBatchHarness(t)
	stakeKey := bytes.Repeat([]byte{0xb1}, 28)
	addr := stateRewardAddress(stakeKey)
	h.seedAccount(t, stakeKey, 100)
	h.seedUtxo(t, batchRef{seed: 0xb2}, 5_000_000)
	tx := buildStateBatch(t,
		[]batchLevel{{withdrawals: map[string]uint64{addr: 40}}},
		batchLevel{
			inputs:      []batchRef{{seed: 0xb2}},
			outputs:     []uint64{1_000_000},
			withdrawals: map[string]uint64{addr: 60},
		},
		false,
	)
	_, err := h.apply(t, tx)
	require.NoError(t, err)
	require.Zero(t, h.reward(t, stakeKey))
}

func TestDijkstraBatchWithdrawalsThreadInChildOrder(t *testing.T) {
	t.Parallel()
	stakeKey := bytes.Repeat([]byte{0xc1}, 28)
	addr := stateRewardAddress(stakeKey)
	deposit := batchLevel{directDeposit: map[string]uint64{addr: 50}}
	withdraw := batchLevel{withdrawals: map[string]uint64{addr: 60}}
	top := batchLevel{inputs: []batchRef{{seed: 0xc2}}, outputs: []uint64{1_000_000}}

	t.Run("deposit child before withdrawing child", func(t *testing.T) {
		t.Parallel()
		h := newStateBatchHarness(t)
		h.seedAccount(t, stakeKey, 10)
		h.seedUtxo(t, batchRef{seed: 0xc2}, 5_000_000)
		tx := buildStateBatch(t, []batchLevel{deposit, withdraw}, top, false)
		_, err := h.apply(t, tx)
		require.NoError(t, err)
		require.Zero(t, h.reward(t, stakeKey))
	})

	t.Run("reversed order overdraws", func(t *testing.T) {
		t.Parallel()
		h := newStateBatchHarness(t)
		h.seedAccount(t, stakeKey, 10)
		h.seedUtxo(t, batchRef{seed: 0xc2}, 5_000_000)
		tx := buildStateBatch(t, []batchLevel{withdraw, deposit}, top, false)
		_, err := h.apply(t, tx)
		require.Error(t, err)
		require.True(
			t,
			errors.Is(err, models.ErrRewardWithdrawalExceedsBalance),
			"want overdraft error, got %v", err,
		)
	})
}

func TestDijkstraBatchDonationsApplyOncePerLevel(t *testing.T) {
	t.Parallel()
	h := newStateBatchHarness(t)
	h.seedUtxo(t, batchRef{seed: 0xd1}, 5_000_000)
	tx := buildStateBatch(t,
		[]batchLevel{{donation: 11}, {donation: 12}},
		batchLevel{
			inputs:   []batchRef{{seed: 0xd1}},
			outputs:  []uint64{1_000_000},
			donation: 100,
		},
		false,
	)
	delta, err := h.apply(t, tx)
	require.NoError(t, err)
	require.Equal(t, uint64(11+12+100), delta.donation)
}

// TestDijkstraBatchApplyAgreesAcrossLedgerPaths drives one batch with several
// child withdrawals and direct deposits through the delta applier, block replay and
// live block processing, and requires the same UTxO set and reward balance from each.
func TestDijkstraBatchApplyAgreesAcrossLedgerPaths(t *testing.T) {
	t.Parallel()
	stakeKey := bytes.Repeat([]byte{0xe1}, 28)
	addr := stateRewardAddress(stakeKey)
	childIn, childIn2, topIn := batchRef{seed: 0xe2}, batchRef{seed: 0xe3}, batchRef{seed: 0xe4}
	for name, run := range map[string]func(*stateBatchHarness, *testing.T, *dijkstra.DijkstraTransaction) error{
		"delta": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			_, err := h.apply(t, tx)
			return err
		},
		"replay": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			return h.process(t, tx, false)
		},
		"live": func(h *stateBatchHarness, t *testing.T, tx *dijkstra.DijkstraTransaction) error {
			return h.process(t, tx, true)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStateBatchHarness(t)
			h.seedAccount(t, stakeKey, 100)
			for _, ref := range []batchRef{childIn, childIn2, topIn} {
				h.seedUtxo(t, ref, 5_000_000)
			}
			tx := buildStateBatch(t,
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
			require.NoError(t, run(h, t, tx))

			for _, ref := range []batchRef{childIn, childIn2, topIn} {
				require.False(t, h.utxoLive(t, ref.txID(), 0), "input %x spent", ref.seed)
			}
			require.True(t, h.utxoLive(t, childBodyID(tx, 0), 0))
			require.True(t, h.utxoLive(t, childBodyID(tx, 1), 0))
			require.True(t, h.utxoLive(t, tx.Hash().Bytes(), 0))
			require.Equal(t, uint64(15), h.reward(t, stakeKey))
		})
	}
}
