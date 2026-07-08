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
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type deltaMockTransaction struct {
	hash        lcommon.Blake2b256
	consumed    []lcommon.TransactionInput
	produced    []lcommon.Utxo
	donation    *big.Int
	updateEpoch uint64
	pparam      map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate
	certs       []lcommon.Certificate
	votes       lcommon.VotingProcedures
	proposals   []lcommon.ProposalProcedure
	invalid     bool
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

func (m *deltaMockTransaction) IsValid() bool { return !m.invalid }

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
	return m.updateEpoch, m.pparam
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
	return m.certs
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
	return m.votes
}

func (m *deltaMockTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return m.proposals
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

func deltaTestHash28(seed byte) lcommon.Blake2b224 {
	var h lcommon.Blake2b224
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

// TestDeltaApplyDirectTransactionDataAndGovernance applies one transaction with
// inputs, outputs, certs, pparams, governance, and donations through apply.
func TestDeltaApplyDirectTransactionDataAndGovernance(t *testing.T) {
	// Set up a Conway ledger state whose key deposit can prove cert deposit calculation.
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.currentEpoch = models.Epoch{EpochId: 100}
	keyDeposit := uint(1_234_567)
	ls.currentPParams = &conway.ConwayProtocolParameters{
		KeyDeposit:              keyDeposit,
		GovActionValidityPeriod: 20,
		DRepInactivityPeriod:    30,
	}

	// Seed a source UTxO so the test transaction has a real input to spend.
	sourceHash := deltaTestHash(0x51)
	txHash := deltaTestHash(0x52)
	sourceTx := models.Transaction{
		Hash:  sourceHash.Bytes(),
		Slot:  90,
		Valid: true,
	}
	require.NoError(t, gdb.Create(&sourceTx).Error)
	require.NoError(t, gdb.Create(&models.Utxo{
		TxId:          sourceHash.Bytes(),
		OutputIdx:     0,
		Amount:        types.Uint64(9_000),
		AddedSlot:     90,
		TransactionID: &sourceTx.ID,
	}).Error)

	// Add a deposit-bearing certificate so apply must calculate and pass a deposit.
	stakeCred := deltaTestHash28(0x53)
	cert := &lcommon.StakeRegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeRegistration),
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: stakeCred,
		},
	}
	// Add a protocol parameter update so apply must persist update metadata.
	pparamKeyDeposit := uint(7_654_321)
	pparamUpdate := babbage.BabbageProtocolParameterUpdate{
		KeyDeposit: &pparamKeyDeposit,
	}
	pparamUpdateCbor, err := cbor.Encode(&pparamUpdate)
	require.NoError(t, err)
	pparamUpdate.SetCbor(pparamUpdateCbor)
	genesisHash := deltaTestHash28(0x54)
	// Add a governance proposal so Conway proposal processing runs inside apply.
	rewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, deltaTestHash28(0x55).Bytes()...),
	)
	require.NoError(t, err)
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       42,
		PPRewardAccount: rewardAddr,
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			},
		},
		PPAnchor: lcommon.GovAnchor{
			Url:      "https://example.invalid/delta-apply",
			DataHash: [32]byte(deltaTestHash(0x56)),
		},
	}
	// Add a DRep vote against that proposal so vote processing runs too.
	voterHash := [28]byte(deltaTestHash28(0x57))
	actionTxHash := [32]byte(txHash)
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeDRepKeyHash,
		Hash: voterHash,
	}
	actionID := &lcommon.GovActionId{
		TransactionId: actionTxHash,
		GovActionIdx:  0,
	}
	// Build one mock transaction containing every field this direct apply test covers.
	tx := &deltaMockTransaction{
		hash: txHash,
		consumed: []lcommon.TransactionInput{
			deltaMockInput{hash: sourceHash, index: 0},
		},
		produced: []lcommon.Utxo{{
			Id: deltaMockInput{hash: txHash, index: 0},
			Output: &deltaMockOutput{
				amount: big.NewInt(1_000),
			},
		}},
		donation:    big.NewInt(99),
		updateEpoch: 101,
		pparam: map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate{
			genesisHash: pparamUpdate,
		},
		certs:     []lcommon.Certificate{cert},
		proposals: []lcommon.ProposalProcedure{proposal},
		votes: lcommon.VotingProcedures{
			voter: {
				actionID: {Vote: lcommon.GovVoteYes},
			},
		},
	}

	// Put the transaction into a LedgerDelta exactly as block processing would.
	point := ocommon.Point{Slot: 200, Hash: deltaTestHash(0x58).Bytes()}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), 5)
	defer delta.Release()
	delta.Offsets = deltaTestOffsets(point, tx)
	delta.addTransaction(tx, 3)

	// Apply the delta directly so this test targets LedgerDelta.apply itself.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	// Verify the transaction row was stored with the delta's block position.
	var storedTx models.Transaction
	require.NoError(t, gdb.Where("hash = ?", txHash.Bytes()).Take(&storedTx).Error)
	require.Equal(t, uint32(3), storedTx.BlockIndex)
	require.Equal(t, point.Slot, storedTx.Slot)

	// Verify the source input was marked spent by this transaction.
	input, err := db.Metadata().GetUtxoIncludingSpent(sourceHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, point.Slot, input.DeletedSlot)
	require.Equal(t, txHash.Bytes(), []byte(input.SpentAtTxId))

	// Verify the produced output was inserted at the delta slot.
	output, err := db.Metadata().GetUtxoIncludingSpent(txHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.NotNil(t, output)
	require.Equal(t, types.Uint64(1_000), output.Amount)
	require.Equal(t, point.Slot, output.AddedSlot)

	// Verify the certificate and calculated deposit were persisted together.
	var certRow models.Certificate
	require.NoError(t, gdb.Where(
		"transaction_id = ? AND cert_index = ?",
		storedTx.ID,
		0,
	).Take(&certRow).Error)
	require.Equal(t, uint(lcommon.CertificateTypeStakeRegistration), certRow.CertType)
	var stakeReg models.StakeRegistration
	require.NoError(t, gdb.Where("certificate_id = ?", certRow.ID).
		Take(&stakeReg).Error)
	require.Equal(t, types.Uint64(keyDeposit), stakeReg.DepositAmount)
	require.Equal(t, stakeCred.Bytes(), stakeReg.StakingKey)

	// Verify the protocol parameter update was written for its target epoch.
	var storedUpdate models.PParamUpdate
	require.NoError(t, gdb.Where(
		"genesis_hash = ? AND added_slot = ? AND epoch = ?",
		genesisHash.Bytes(),
		point.Slot,
		uint64(101),
	).Take(&storedUpdate).Error)
	require.Equal(t, pparamUpdateCbor, storedUpdate.Cbor)

	// Verify the governance proposal was processed from the transaction.
	storedProposal, err := db.GetGovernanceProposal(txHash.Bytes(), 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), storedProposal.ProposedEpoch)
	require.Equal(t, uint64(120), storedProposal.ExpiresEpoch)
	require.Equal(t, uint64(42), storedProposal.Deposit)

	// Verify the governance vote was processed and linked to the proposal.
	votes, err := db.GetGovernanceVotes(storedProposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoterTypeDRep), votes[0].VoterType)
	require.Equal(t, uint8(lcommon.GovVoteYes), votes[0].Vote)
	require.Equal(t, voterHash[:], votes[0].VoterCredential)

	// Verify the transaction donation was accumulated for the current epoch.
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(100, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(99), sum)
}

// TestDeltaApplyInvalidTransactionSkipsGovernanceAndDonation applies a
// phase-2-invalid transaction and checks apply still records the transaction
// row but skips governance processing and donation accumulation for it.
func TestDeltaApplyInvalidTransactionSkipsGovernanceAndDonation(t *testing.T) {
	// Set up a Conway ledger state so proposal processing would run if reached.
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.currentEpoch = models.Epoch{EpochId: 200}
	ls.currentPParams = &conway.ConwayProtocolParameters{
		GovActionValidityPeriod: 20,
	}

	// Build a proposal that would be persisted if governance processing ran.
	txHash := deltaTestHash(0x91)
	rewardAddr, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, deltaTestHash28(0x92).Bytes()...),
	)
	require.NoError(t, err)
	proposal := conway.ConwayProposalProcedure{
		PPDeposit:       7,
		PPRewardAccount: rewardAddr,
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			},
		},
		PPAnchor: lcommon.GovAnchor{
			Url:      "https://example.invalid/delta-apply-invalid",
			DataHash: [32]byte(deltaTestHash(0x93)),
		},
	}

	// Mark the transaction invalid (phase-2 failed) and give it a donation, so
	// a passing test proves both governance and donation processing are skipped.
	tx := &deltaMockTransaction{
		hash:      txHash,
		invalid:   true,
		donation:  big.NewInt(55),
		proposals: []lcommon.ProposalProcedure{proposal},
	}

	// Put the invalid transaction into a delta and apply it directly.
	point := ocommon.Point{Slot: 500, Hash: deltaTestHash(0x94).Bytes()}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), 7)
	defer delta.Release()
	delta.Offsets = deltaTestOffsets(point, tx)
	delta.addTransaction(tx, 0)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	}))

	// Verify the transaction row was still stored despite being invalid.
	var storedTx models.Transaction
	require.NoError(t, gdb.Where("hash = ?", txHash.Bytes()).Take(&storedTx).Error)
	require.False(t, storedTx.Valid)

	// Verify governance processing was skipped for the invalid transaction.
	_, err = db.GetGovernanceProposal(txHash.Bytes(), 0, nil)
	require.ErrorIs(t, err, models.ErrGovernanceProposalNotFound)

	// Verify the donation on the invalid transaction was not accumulated.
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(200, nil)
	require.NoError(t, err)
	require.Zero(t, sum)
}

// TestDeltaApplyErrorRollsBackPartiallyPopulatedState forces apply to fail
// after staged writes and checks the surrounding transaction rolls them back.
func TestDeltaApplyErrorRollsBackPartiallyPopulatedState(t *testing.T) {
	// Set up ledger params so the certificate path reaches metadata processing.
	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.currentPParams = &conway.ConwayProtocolParameters{
		KeyDeposit: 1_000,
	}

	// Build a transaction that can create rows before its bad certificate fails.
	txHash := deltaTestHash(0x61)
	tx := &deltaMockTransaction{
		hash: txHash,
		produced: []lcommon.Utxo{{
			Id: deltaMockInput{hash: txHash, index: 0},
			Output: &deltaMockOutput{
				amount: big.NewInt(1),
			},
		}},
		certs: []lcommon.Certificate{
			&lcommon.StakeRegistrationCertificate{
				CertType: uint(lcommon.CertificateTypeStakeRegistration),
				StakeCredential: lcommon.Credential{
					CredType: 99,
				},
			},
		},
	}

	// Put the bad transaction into a delta and apply it directly.
	point := ocommon.Point{Slot: 300, Hash: deltaTestHash(0x62).Bytes()}
	delta := NewLedgerDelta(point, uint(conway.EraIdConway), 6)
	defer delta.Release()
	delta.Offsets = deltaTestOffsets(point, tx)
	delta.addTransaction(tx, 0)

	// Confirm the invalid certificate makes apply return an error.
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		return delta.apply(ls, txn)
	})
	require.Error(t, err)

	// Verify no partially-created transaction, output, or certificate rows remain.
	var txCount, utxoCount, certCount int64
	require.NoError(t, gdb.Model(&models.Transaction{}).
		Where("hash = ?", txHash.Bytes()).
		Count(&txCount).Error)
	require.NoError(t, gdb.Model(&models.Utxo{}).
		Where("tx_id = ?", txHash.Bytes()).
		Count(&utxoCount).Error)
	require.NoError(t, gdb.Model(&models.Certificate{}).
		Count(&certCount).Error)
	require.Zero(t, txCount)
	require.Zero(t, utxoCount)
	require.Zero(t, certCount)
}

// TestLedgerDeltaPoolsResetAndDoNotBleedState seeds stale pooled objects and
// verifies new/released deltas and batches do not retain cross-use state.
func TestLedgerDeltaPoolsResetAndDoNotBleedState(t *testing.T) {
	// Seed the delta and transaction-slice pools with stale data.
	point1 := ocommon.Point{Slot: 400, Hash: deltaTestHash(0x71).Bytes()}
	tx1 := &deltaMockTransaction{hash: deltaTestHash(0x72)}
	staleTransactions := []TransactionRecord{{Tx: tx1, Index: 9}}
	transactionRecordSlicePool.Put(&staleTransactions)
	staleDelta := &LedgerDelta{
		Point:        point1,
		BlockEraId:   99,
		BlockNumber:  99,
		Transactions: staleTransactions,
		Offsets:      &database.BlockIngestionResult{},
		Speculative:  true,
	}
	ledgerDeltaPool.Put(staleDelta)

	// Verify NewLedgerDelta resets stale pooled fields before reuse.
	point2 := ocommon.Point{Slot: 401, Hash: deltaTestHash(0x73).Bytes()}
	delta1 := NewLedgerDelta(point2, uint(conway.EraIdConway), 8)
	require.Equal(t, point2, delta1.Point)
	require.Equal(t, uint(conway.EraIdConway), delta1.BlockEraId)
	require.Equal(t, uint64(8), delta1.BlockNumber)
	require.False(t, delta1.Speculative)
	require.Nil(t, delta1.Offsets)
	require.Empty(t, delta1.Transactions)
	txSlicePtr := delta1.txSlicePtr
	require.NotNil(t, txSlicePtr)
	delta1.addTransaction(tx1, 9)
	require.Len(t, delta1.Transactions, 1)

	// Verify Release clears references before returning objects to their pools.
	delta1.Release()

	require.Nil(t, delta1.Transactions)
	require.Nil(t, delta1.txSlicePtr)
	require.Nil(t, delta1.Offsets)
	require.Empty(t, *txSlicePtr)

	// Verify a second delta does not inherit the previous transaction state.
	point3 := ocommon.Point{Slot: 402, Hash: deltaTestHash(0x75).Bytes()}
	delta2 := NewLedgerDelta(point3, uint(conway.EraIdConway), 10)
	require.Equal(t, point3, delta2.Point)
	require.False(t, delta2.Speculative)
	require.Nil(t, delta2.Offsets)
	require.Empty(t, delta2.Transactions)
	delta2.addTransaction(&deltaMockTransaction{hash: deltaTestHash(0x74)}, 0)
	require.Len(t, delta2.Transactions, 1)
	require.Equal(t, deltaTestHash(0x74), delta2.Transactions[0].Tx.Hash())

	// Verify batch release releases contained deltas and clears retained entries.
	batch := NewLedgerDeltaBatch()
	batch.addDelta(delta2)
	require.Len(t, batch.deltas, 1)
	batch.Release()
	require.Empty(t, batch.deltas)
	require.Nil(t, batch.deltas[:cap(batch.deltas)][0])

	// Verify a reused batch starts empty.
	reusedBatch := NewLedgerDeltaBatch()
	defer reusedBatch.Release()
	require.Empty(t, reusedBatch.deltas)
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
