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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type deltaMockTransaction struct {
	hash     lcommon.Blake2b256
	consumed []lcommon.TransactionInput
	produced []lcommon.Utxo
	donation *big.Int
}

func (m *deltaMockTransaction) Type() int { return 0 }

func (m *deltaMockTransaction) Cbor() []byte { return []byte{0x82} }

func (m *deltaMockTransaction) Hash() lcommon.Blake2b256 { return m.hash }

func (m *deltaMockTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *deltaMockTransaction) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (m *deltaMockTransaction) AuxiliaryData() lcommon.AuxiliaryData {
	return nil
}

func (m *deltaMockTransaction) IsValid() bool { return true }

func (m *deltaMockTransaction) Consumed() []lcommon.TransactionInput {
	return m.consumed
}

func (m *deltaMockTransaction) Produced() []lcommon.Utxo {
	return m.produced
}

func (m *deltaMockTransaction) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

func (m *deltaMockTransaction) Fee() *big.Int { return big.NewInt(0) }

func (m *deltaMockTransaction) Id() lcommon.Blake2b256 { return m.hash }

func (m *deltaMockTransaction) Inputs() []lcommon.TransactionInput {
	return m.consumed
}

func (m *deltaMockTransaction) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (m *deltaMockTransaction) TTL() uint64 { return 0 }

func (m *deltaMockTransaction) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return 0, nil
}

func (m *deltaMockTransaction) ValidityIntervalStart() uint64 { return 0 }

func (m *deltaMockTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (m *deltaMockTransaction) Collateral() []lcommon.TransactionInput {
	return nil
}

func (m *deltaMockTransaction) CollateralReturn() lcommon.TransactionOutput {
	return nil
}

func (m *deltaMockTransaction) TotalCollateral() *big.Int {
	return big.NewInt(0)
}

func (m *deltaMockTransaction) Certificates() []lcommon.Certificate {
	return nil
}

func (m *deltaMockTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (m *deltaMockTransaction) AuxDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *deltaMockTransaction) RequiredSigners() []lcommon.Blake2b224 {
	return nil
}

func (m *deltaMockTransaction) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return nil
}

func (m *deltaMockTransaction) ScriptDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *deltaMockTransaction) VotingProcedures() lcommon.VotingProcedures {
	return lcommon.VotingProcedures{}
}

func (m *deltaMockTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return nil
}

func (m *deltaMockTransaction) CurrentTreasuryValue() *big.Int {
	return big.NewInt(0)
}

func (m *deltaMockTransaction) Donation() *big.Int {
	if m.donation == nil {
		return big.NewInt(0)
	}
	return m.donation
}

func (m *deltaMockTransaction) Utxorpc() (*cardano.Tx, error) {
	return nil, nil
}

type deltaMockInput struct {
	hash  lcommon.Blake2b256
	index uint32
}

func (m deltaMockInput) Id() lcommon.Blake2b256 { return m.hash }

func (m deltaMockInput) Index() uint32 { return m.index }

func (m deltaMockInput) String() string { return m.hash.String() }

func (m deltaMockInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}

func (m deltaMockInput) ToPlutusData() data.PlutusData {
	return nil
}

type deltaMockOutput struct {
	amount *big.Int
}

func (m *deltaMockOutput) Address() lcommon.Address { return lcommon.Address{} }

func (m *deltaMockOutput) Amount() *big.Int { return m.amount }

func (m *deltaMockOutput) Assets() *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	return nil
}

func (m *deltaMockOutput) Datum() *lcommon.Datum { return nil }

func (m *deltaMockOutput) DatumHash() *lcommon.Blake2b256 { return nil }

func (m *deltaMockOutput) Cbor() []byte { return nil }

func (m *deltaMockOutput) Utxorpc() (*cardano.TxOutput, error) {
	return nil, nil
}

func (m *deltaMockOutput) ScriptRef() lcommon.Script { return nil }

func (m *deltaMockOutput) ToPlutusData() data.PlutusData { return nil }

func (m *deltaMockOutput) String() string { return "" }

func deltaTestHash(seed byte) lcommon.Blake2b256 {
	var h lcommon.Blake2b256
	for i := range h {
		h[i] = seed
	}
	return h
}

func deltaTestOffsets(
	point ocommon.Point,
	txs ...lcommon.Transaction,
) *database.BlockIngestionResult {
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)
	ret := &database.BlockIngestionResult{
		TxOffsets:   make(map[[32]byte]database.CborOffset, len(txs)),
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}
	for i, tx := range txs {
		offset := database.CborOffset{
			BlockSlot:  point.Slot,
			BlockHash:  blockHash,
			ByteOffset: uint32(i), //nolint:gosec
			ByteLength: 1,
		}
		var txHash [32]byte
		copy(txHash[:], tx.Hash().Bytes())
		ret.TxOffsets[txHash] = offset
		for _, utxo := range tx.Produced() {
			var txID [32]byte
			copy(txID[:], utxo.Id.Id().Bytes())
			ret.UtxoOffsets[database.UtxoRef{
				TxId:      txID,
				OutputIdx: utxo.Id.Index(),
			}] = offset
		}
	}
	return ret
}

func TestDeltaApplySkipsConflictingSpeculativeTransactionFully(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosTolerateEndorserConflicts = true
	ls.currentEpoch = models.Epoch{EpochId: 42}

	sourceHash := deltaTestHash(0x10)
	otherSpenderHash := deltaTestHash(0x20)
	skippedHash := deltaTestHash(0x30)
	appliedHash := deltaTestHash(0x40)

	require.NoError(t, gdb.Create(&models.Transaction{
		Hash: otherSpenderHash.Bytes(),
		Slot: 100,
	}).Error)
	require.NoError(t, gdb.Create(&models.Utxo{
		TxId:        sourceHash.Bytes(),
		OutputIdx:   0,
		AddedSlot:   90,
		DeletedSlot: 100,
		SpentAtTxId: types.NullableHash(otherSpenderHash.Bytes()),
	}).Error)

	skippedTx := &deltaMockTransaction{
		hash: skippedHash,
		consumed: []lcommon.TransactionInput{
			deltaMockInput{hash: sourceHash, index: 0},
		},
		produced: []lcommon.Utxo{{
			Id: deltaMockInput{hash: skippedHash, index: 0},
			Output: &deltaMockOutput{
				amount: big.NewInt(1),
			},
		}},
		donation: big.NewInt(100),
	}
	appliedTx := &deltaMockTransaction{
		hash:     appliedHash,
		donation: big.NewInt(7),
	}

	point := ocommon.Point{Slot: 200, Hash: deltaTestHash(0xAA).Bytes()}
	delta := NewLedgerDelta(point, 0, 1)
	defer delta.Release()
	delta.Speculative = true
	delta.Offsets = deltaTestOffsets(point, skippedTx, appliedTx)
	delta.addTransaction(skippedTx, 0)
	delta.addTransaction(appliedTx, 1)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	var skippedCount, appliedCount int64
	require.NoError(t, gdb.Model(&models.Transaction{}).
		Where("hash = ?", skippedHash.Bytes()).
		Count(&skippedCount).Error)
	require.Equal(t, int64(0), skippedCount)
	require.NoError(t, gdb.Model(&models.Transaction{}).
		Where("hash = ?", appliedHash.Bytes()).
		Count(&appliedCount).Error)
	require.Equal(t, int64(1), appliedCount)

	out, err := db.Metadata().GetUtxoIncludingSpent(
		skippedHash.Bytes(),
		0,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, out)

	input, err := db.Metadata().GetUtxoIncludingSpent(sourceHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, otherSpenderHash.Bytes(), []byte(input.SpentAtTxId))

	sum, err := db.Metadata().SumNetworkDonationsForEpoch(42, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(7), sum)

	readTxn := db.BlobTxn(false)
	defer readTxn.Release()
	_, err = db.Blob().GetTx(readTxn.Blob(), skippedHash.Bytes())
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
	_, err = db.Blob().GetUtxo(readTxn.Blob(), skippedHash.Bytes(), 0)
	require.ErrorIs(t, err, types.ErrBlobKeyNotFound)
}

func TestDeltaApplySkipsExistingSpeculativeTransactionWithoutCleanup(t *testing.T) {
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosTolerateEndorserConflicts = true
	ls.currentEpoch = models.Epoch{EpochId: 42}

	sourceHash := deltaTestHash(0x11)
	otherSpenderHash := deltaTestHash(0x21)
	existingHash := deltaTestHash(0x31)

	require.NoError(t, gdb.Create(&models.Transaction{
		Hash: otherSpenderHash.Bytes(),
		Slot: 100,
	}).Error)
	require.NoError(t, gdb.Create(&models.Utxo{
		TxId:        sourceHash.Bytes(),
		OutputIdx:   0,
		AddedSlot:   90,
		DeletedSlot: 100,
		SpentAtTxId: types.NullableHash(otherSpenderHash.Bytes()),
	}).Error)

	existingTx := models.Transaction{
		Hash:  existingHash.Bytes(),
		Slot:  80,
		Valid: true,
	}
	require.NoError(t, gdb.Create(&existingTx).Error)
	require.NoError(t, gdb.Create(&models.Utxo{
		TxId:          existingHash.Bytes(),
		OutputIdx:     0,
		AddedSlot:     80,
		TransactionID: &existingTx.ID,
	}).Error)

	duplicateTx := &deltaMockTransaction{
		hash: existingHash,
		consumed: []lcommon.TransactionInput{
			deltaMockInput{hash: sourceHash, index: 0},
		},
		produced: []lcommon.Utxo{{
			Id: deltaMockInput{hash: existingHash, index: 0},
			Output: &deltaMockOutput{
				amount: big.NewInt(1),
			},
		}},
		donation: big.NewInt(100),
	}

	point := ocommon.Point{Slot: 200, Hash: deltaTestHash(0xAB).Bytes()}
	delta := NewLedgerDelta(point, 0, 1)
	defer delta.Release()
	delta.Speculative = true
	delta.Offsets = deltaTestOffsets(point, duplicateTx)
	delta.addTransaction(duplicateTx, 0)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	var existingCount int64
	require.NoError(t, gdb.Model(&models.Transaction{}).
		Where("hash = ?", existingHash.Bytes()).
		Count(&existingCount).Error)
	require.Equal(t, int64(1), existingCount)

	out, err := db.Metadata().GetUtxoIncludingSpent(
		existingHash.Bytes(),
		0,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, out)

	input, err := db.Metadata().GetUtxoIncludingSpent(sourceHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, otherSpenderHash.Bytes(), []byte(input.SpentAtTxId))

	sum, err := db.Metadata().SumNetworkDonationsForEpoch(42, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), sum)

	remaining, err := db.FilterEndorserTransactions(
		[][]byte{existingHash.Bytes()},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, remaining)
}
