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

package main

import (
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

type mockGapGovernanceTransaction struct {
	hash               lcommon.Blake2b256
	votingProcedures   lcommon.VotingProcedures
	proposalProcedures []lcommon.ProposalProcedure
	isValid            bool
}

func (m *mockGapGovernanceTransaction) Hash() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockGapGovernanceTransaction) Id() lcommon.Blake2b256 {
	return m.hash
}

func (m *mockGapGovernanceTransaction) Type() int {
	return gledger.TxTypeConway
}

func (m *mockGapGovernanceTransaction) Fee() *big.Int {
	return big.NewInt(0)
}

func (m *mockGapGovernanceTransaction) TTL() uint64 {
	return 0
}

func (m *mockGapGovernanceTransaction) IsValid() bool {
	return m.isValid
}

func (m *mockGapGovernanceTransaction) Metadata() lcommon.TransactionMetadatum {
	return nil
}

func (m *mockGapGovernanceTransaction) AuxiliaryData() lcommon.AuxiliaryData {
	return nil
}

func (m *mockGapGovernanceTransaction) RawAuxiliaryData() []byte {
	return nil
}

func (m *mockGapGovernanceTransaction) CollateralReturn() lcommon.TransactionOutput {
	return nil
}

func (m *mockGapGovernanceTransaction) Produced() []lcommon.Utxo {
	return nil
}

func (m *mockGapGovernanceTransaction) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (m *mockGapGovernanceTransaction) Inputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockGapGovernanceTransaction) Collateral() []lcommon.TransactionInput {
	return nil
}

func (m *mockGapGovernanceTransaction) Certificates() []lcommon.Certificate {
	return nil
}

func (m *mockGapGovernanceTransaction) ProtocolParameterUpdates() (
	uint64,
	map[lcommon.Blake2b224]lcommon.ProtocolParameterUpdate,
) {
	return 0, nil
}

func (m *mockGapGovernanceTransaction) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return nil
}

func (m *mockGapGovernanceTransaction) AuxDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockGapGovernanceTransaction) Cbor() []byte {
	return nil
}

func (m *mockGapGovernanceTransaction) Consumed() []lcommon.TransactionInput {
	return nil
}

func (m *mockGapGovernanceTransaction) Witnesses() lcommon.TransactionWitnessSet {
	return nil
}

func (m *mockGapGovernanceTransaction) ValidityIntervalStart() uint64 {
	return 0
}

func (m *mockGapGovernanceTransaction) ReferenceInputs() []lcommon.TransactionInput {
	return nil
}

func (m *mockGapGovernanceTransaction) TotalCollateral() *big.Int {
	return nil
}

func (m *mockGapGovernanceTransaction) Withdrawals() map[*lcommon.Address]*big.Int {
	return nil
}

func (m *mockGapGovernanceTransaction) RequiredSigners() []lcommon.Blake2b224 {
	return nil
}

func (m *mockGapGovernanceTransaction) ScriptDataHash() *lcommon.Blake2b256 {
	return nil
}

func (m *mockGapGovernanceTransaction) VotingProcedures() lcommon.VotingProcedures {
	return m.votingProcedures
}

func (m *mockGapGovernanceTransaction) ProposalProcedures() []lcommon.ProposalProcedure {
	return m.proposalProcedures
}

func (m *mockGapGovernanceTransaction) CurrentTreasuryValue() *big.Int {
	return nil
}

func (m *mockGapGovernanceTransaction) Donation() *big.Int {
	return nil
}

func (m *mockGapGovernanceTransaction) Utxorpc() (*cardano.Tx, error) {
	return nil, nil
}

func (m *mockGapGovernanceTransaction) LeiosHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func testGapHash32(seed string) []byte {
	hash := make([]byte, 32)
	copy(hash, []byte(seed))
	return hash
}

func testGapHash28(seed string) []byte {
	hash := make([]byte, 28)
	copy(hash, []byte(seed))
	return hash
}

func TestValidateStoredGapBlocks(t *testing.T) {
	immutableTip := models.Block{
		Slot: 10,
		Hash: testGapHash32("immutable-tip"),
	}
	first := models.Block{
		Slot:     11,
		Hash:     testGapHash32("first-gap"),
		PrevHash: immutableTip.Hash,
	}
	second := models.Block{
		Slot:     12,
		Hash:     testGapHash32("second-gap"),
		PrevHash: first.Hash,
	}

	require.NoError(
		t,
		validateStoredGapBlocks(
			[]models.Block{first, second},
			immutableTip,
			second.Hash,
		),
	)

	brokenFirst := first
	brokenFirst.PrevHash = testGapHash32("wrong-prev")
	require.ErrorContains(
		t,
		validateStoredGapBlocks(
			[]models.Block{brokenFirst, second},
			immutableTip,
			second.Hash,
		),
		"does not match immutable tip",
	)

	brokenSecond := second
	brokenSecond.PrevHash = testGapHash32("wrong-link")
	require.ErrorContains(
		t,
		validateStoredGapBlocks(
			[]models.Block{first, brokenSecond},
			immutableTip,
			second.Hash,
		),
		"does not match previous block",
	)

	require.ErrorContains(
		t,
		validateStoredGapBlocks(
			[]models.Block{first, second},
			immutableTip,
			testGapHash32("ledger-hash"),
		),
		"does not match ledger state hash",
	)
}

func testGapConwayProtocolParameters() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		GovActionValidityPeriod: 20,
		DRepInactivityPeriod:    20,
	}
}

func TestProcessGapBlockTransactionsProcessesGovernance(
	t *testing.T,
) {
	tmpDir := t.TempDir()

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xE1}, testGapHash28("proposal-reward")...),
	)
	require.NoError(t, err)
	proposalProcedure := conway.ConwayProposalProcedure{
		PPDeposit:       42,
		PPRewardAccount: rewardAddress,
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeInfo),
			Action: &lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			},
		},
		PPAnchor: lcommon.GovAnchor{
			Url:      "https://example.com/proposal",
			DataHash: [32]byte(testGapHash32("proposal-anchor")),
		},
	}
	proposalBodyCbor, err := cbor.Encode(
		&conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{
				proposalProcedure,
			},
		},
	)
	require.NoError(t, err)
	proposalTxHash := lcommon.Blake2b256Hash(proposalBodyCbor)
	proposalTx := &mockGapGovernanceTransaction{
		hash:               proposalTxHash,
		proposalProcedures: []lcommon.ProposalProcedure{proposalProcedure},
		isValid:            true,
	}

	ccCred := testGapHash28("committee-voter")
	var voterHash [28]byte
	copy(voterHash[:], ccCred)
	var actionTxHash [32]byte
	copy(actionTxHash[:], proposalTxHash.Bytes())
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: voterHash,
	}
	actionID := &lcommon.GovActionId{
		TransactionId: actionTxHash,
		GovActionIdx:  0,
	}
	var voteTxHash lcommon.Blake2b256
	copy(voteTxHash[:], testGapHash32("vote-tx"))
	voteTx := &mockGapGovernanceTransaction{
		hash:    voteTxHash,
		isValid: true,
		votingProcedures: lcommon.VotingProcedures{
			voter: {
				actionID: {Vote: models.VoteYes},
			},
		},
	}

	point := ocommon.Point{
		Slot: 1000,
		Hash: testGapHash32("gap-block"),
	}
	var blockHash [32]byte
	copy(blockHash[:], point.Hash)
	var proposalTxHashArray [32]byte
	copy(proposalTxHashArray[:], proposalTxHash.Bytes())
	var voteTxHashArray [32]byte
	copy(voteTxHashArray[:], voteTxHash.Bytes())
	// The TxOffsets entries below are placeholders, not real offsets
	// into a stored block: the test builds proposalTxHash from the
	// in-memory proposalBodyCbor and never persists that CBOR to the
	// blob store. SetGapBlockTransaction (invoked via
	// processGapBlockTransactions) only requires that the tx hash has
	// an entry in BlockIngestionResult.TxOffsets, and
	// mockGapGovernanceTransaction.Consumed() returns nil so no input
	// recovery / block lookup is triggered. As a result this test does
	// not exercise CBOR blob round-trip or offset validation, and
	// future eager validation in processGapBlockTransactions must not
	// rely on these offsets pointing at usable bytes.
	offsets := &database.BlockIngestionResult{
		TxOffsets: map[[32]byte]database.CborOffset{
			proposalTxHashArray: {
				BlockSlot:  point.Slot,
				BlockHash:  blockHash,
				ByteOffset: 0,
				ByteLength: 1,
			},
			voteTxHashArray: {
				BlockSlot:  point.Slot,
				BlockHash:  blockHash,
				ByteOffset: 1,
				ByteLength: 1,
			},
		},
		UtxoOffsets: make(map[database.UtxoRef]database.CborOffset),
	}

	err = processGapBlockTransactions(
		db,
		point,
		[]lcommon.Transaction{proposalTx, voteTx},
		offsets,
		100,
		testGapConwayProtocolParameters(),
	)
	require.NoError(t, err)

	proposal, err := db.GetGovernanceProposal(
		proposalTxHash.Bytes(),
		0,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	assert.Equal(t, proposalTxHash.Bytes(), proposal.TxHash)
	assert.Equal(t, uint32(0), proposal.ActionIndex)
	assert.Equal(t, uint8(lcommon.GovActionTypeInfo), proposal.ActionType)
	assert.Equal(t, uint64(100), proposal.ProposedEpoch)
	assert.Equal(t, uint64(120), proposal.ExpiresEpoch)
	assert.Equal(t, proposalProcedure.PPAnchor.Url, proposal.AnchorURL)
	assert.Equal(t, proposalProcedure.PPAnchor.DataHash[:], proposal.AnchorHash)

	votes, err := db.GetGovernanceVotes(proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	assert.Equal(t, uint8(models.VoterTypeCC), votes[0].VoterType)
	assert.Equal(t, ccCred, votes[0].VoterCredential)
	assert.Equal(t, uint8(models.VoteYes), votes[0].Vote)
	assert.Equal(t, point.Slot, votes[0].AddedSlot)
}

func TestProcessGapBlocksNoOpWithoutUint64Overflow(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Empty input returns immediately without touching the epoch
	// table; verify no arithmetic is attempted on an empty epochs
	// slice (where len(epochs)-1 would wrap to a huge uint).
	require.NoError(t, processGapBlocks(t.Context(), db, logger, nil))
	require.NoError(
		t,
		processGapBlocks(t.Context(), db, logger, []models.Block{}),
	)

	// With at least one gap block and no epoch records,
	// processGapBlocks must surface an error rather than panic
	// on epochs[len(epochs)-1] or wrap the negative index into a
	// large uint64. Use a real block from immutable/testdata so
	// gledger.NewBlockFromCbor succeeds and execution actually
	// reaches gapBlockEpoch with an empty epochs slice — that is
	// the path the underflow regression would land on.
	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	var block models.Block
	const maxIterations = 1_000
	for i := range maxIterations {
		next, err := iter.Next()
		require.NoError(t, err)
		if next == nil {
			require.Failf(
				t,
				"immutable testdata exhausted",
				"no non-EBB block with transactions found after %d iterations",
				i+1,
			)
		}
		if next.IsEbb {
			continue
		}
		parsed, err := gledger.NewBlockFromCbor(
			next.Type,
			next.Cbor,
			lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		require.NoError(t, err)
		if len(parsed.Transactions()) == 0 {
			continue
		}
		block = models.Block{
			Slot: next.Slot,
			Hash: append([]byte(nil), next.Hash...),
			Cbor: append([]byte(nil), next.Cbor...),
			Type: next.Type,
		}
		break
	}
	require.NotZero(
		t,
		block.Slot,
		"no non-EBB block with transactions found within scan bound",
	)

	var processErr error
	require.NotPanics(t, func() {
		processErr = processGapBlocks(
			t.Context(),
			db,
			logger,
			[]models.Block{block},
		)
	})
	require.Error(t, processErr)
	require.ErrorContains(t, processErr, "no epoch found for slot")
}

func TestDeleteBlobBlocksAboveSlot(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	// Three synthetic blocks at well-separated slots so the deletion
	// boundary is unambiguous. CBOR/type don't need to parse — the
	// blob store treats them as opaque bytes.
	blocks := []models.Block{
		{Slot: 100, Hash: testGapHash32("blk-100"), Cbor: []byte{0x82, 0x01}, Type: 1},
		{Slot: 200, Hash: testGapHash32("blk-200"), Cbor: []byte{0x82, 0x02}, Type: 1},
		{Slot: 300, Hash: testGapHash32("blk-300"), Cbor: []byte{0x82, 0x03}, Type: 1},
	}
	for _, b := range blocks {
		require.NoError(t, db.BlockCreate(b, nil))
	}

	require.NoError(t, deleteBlobBlocksAboveSlot(db, 150))

	remaining, err := loadGapBlocksFromBlob(db, 0, 1000)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	assert.Equal(t, uint64(100), remaining[0].Slot)

	// Idempotent re-run is a no-op.
	require.NoError(t, deleteBlobBlocksAboveSlot(db, 150))
	remaining, err = loadGapBlocksFromBlob(db, 0, 1000)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	assert.Equal(t, uint64(100), remaining[0].Slot)
}

func TestDeleteBlobBlocksAboveSlotKeepsBoundaryTip(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	blocks := []models.Block{
		{Slot: 100, Hash: testGapHash32("blk-100"), Cbor: []byte{0x82, 0x01}, Type: 1},
		{Slot: 200, Hash: testGapHash32("blk-200"), Cbor: []byte{0x82, 0x02}, Type: 1},
		{Slot: 300, Hash: testGapHash32("blk-300"), Cbor: []byte{0x82, 0x03}, Type: 1},
	}
	for _, b := range blocks {
		require.NoError(t, db.BlockCreate(b, nil))
	}

	require.NoError(t, deleteBlobBlocksAboveSlot(db, 200))

	remaining, err := loadGapBlocksFromBlob(db, 0, 1000)
	require.NoError(t, err)
	require.Len(t, remaining, 2)
	assert.Equal(t, uint64(100), remaining[0].Slot)
	assert.Equal(t, uint64(200), remaining[1].Slot)

	recent, err := database.BlocksRecent(db, 1)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	assert.Equal(t, uint64(200), recent[0].Slot)
}

func TestLoadGapBlocksFromBlob(t *testing.T) {
	tmpDir := t.TempDir()

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err)
	defer db.Close()

	immutableDir := filepath.Join(
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
	imm, err := immutable.New(immutableDir)
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer iter.Close()

	var expected models.Block
	const maxIterations = 1_000
	for i := range maxIterations {
		next, err := iter.Next()
		require.NoError(t, err)
		if next == nil {
			require.Failf(
				t,
				"immutable testdata exhausted",
				"iterator returned nil before a non-EBB block was found (after %d iterations)",
				i+1,
			)
		}
		if next.IsEbb {
			continue
		}
		expected = models.Block{
			Slot: next.Slot,
			Hash: append([]byte(nil), next.Hash...),
			Cbor: append([]byte(nil), next.Cbor...),
			Type: next.Type,
		}
		break
	}
	require.NotZero(
		t,
		expected.Slot,
		"no non-EBB block found within the %d-iteration scan bound",
		maxIterations,
	)
	require.NoError(t, db.BlockCreate(expected, nil))

	loaded, err := loadGapBlocksFromBlob(
		db,
		expected.Slot,
		expected.Slot,
	)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, expected.Slot, loaded[0].Slot)
	assert.Equal(t, expected.Hash, loaded[0].Hash)
	assert.Equal(t, expected.Cbor, loaded[0].Cbor)
	assert.Equal(t, expected.Type, loaded[0].Type)
}
