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

package indexer

import (
	"encoding/hex"
	"log/slog"
	"os"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	fxcbor "github.com/fxamacker/cbor/v2"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testPolicyID is a 28-byte (56 hex-char) cNIGHT policy ID.
const testPolicyID = "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa"

// testAssetNameHex is "NIGHT" (hex: 4e49474854).
const testAssetNameHex = "4e49474854"

// testAuthAssetNameHex is "AUTH" (hex: 41555448).
const testAuthAssetNameHex = "41555448"

// testAuthPolicyID is the expected auth-token policy (28 bytes, all 0xCC).
const testAuthPolicyID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// testMappingAddr is used as the mapping validator address in tests.
const testMappingAddr = "addr_test1wplxjzranravtp574s2wz00md7vz9rzpucu252je68u9a8qzjheng"

// testOtherAddr is a distinct address used for non-relevant outputs.
const testOtherAddr = "addr_test1qpe6s9amgfwtu9u6lqj998vke6uncswr4dg88qqft5d7f67kfjf77qy57hqhnefcqyy7hmhsygj9j38rj984hn9r57fswc4wg0"

// testCouncilAddr is a second governance address (Technical Committee uses
// testMappingAddr; council uses this one so the two can be distinguished).
const testCouncilAddr = testOtherAddr

// testGovPolicyID is a 28-byte (56 hex-char) policy used for governance / Ariadne tests.
const testGovPolicyID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// testPermPolicyID is a 28-byte (56 hex-char) policy used for permissioned-candidate tests.
const testPermPolicyID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// pad32 returns a 64-char hex string by appending zero bytes after prefix.
// prefix must be a valid hex string of at most 64 chars.
func pad32(prefix string) string {
	need := 64 - len(prefix)
	return prefix + hex.EncodeToString(make([]byte, need/2))
}

// setupTestStore creates an in-memory SQLite store and runs auto-migration.
func setupTestStore(t *testing.T) *sqlite.MetadataStoreSqlite {
	t.Helper()
	store, err := sqlite.New("", slog.New(slog.NewTextHandler(os.Stderr, nil)), nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	require.NoError(t, store.DB().AutoMigrate(models.MigrateModels...))
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

// setupIndexer creates a test Indexer backed by the given store.
// It uses testAuthPolicyID to enforce policy-scoped auth-token matching.
func setupIndexer(t *testing.T, store *sqlite.MetadataStoreSqlite) *Indexer {
	t.Helper()
	idx, err := New(Config{
		Metadata:                store,
		Logger:                  slog.New(slog.NewTextHandler(os.Stderr, nil)),
		CNightPolicyID:          testPolicyID,
		CNightAssetName:         testAssetNameHex,
		MappingValidatorAddress: testMappingAddr,
		AuthTokenPolicyID:       testAuthPolicyID,
		AuthTokenAssetName:      testAuthAssetNameHex,
	})
	require.NoError(t, err)
	return idx
}

// testBlock creates a minimal models.Block for use in tests.
func testBlock(number uint64, slot uint64, hashByte byte) models.Block {
	hash := make([]byte, 32)
	hash[0] = hashByte
	return models.Block{
		Number: number,
		Slot:   slot,
		Hash:   hash,
	}
}

// buildCNightOutput builds a mock output containing a cNIGHT token.
func buildCNightOutput(t *testing.T, policyHex, assetNameHex string, amount uint64) lcommon.TransactionOutput {
	t.Helper()
	policyBytes, err := hex.DecodeString(policyHex)
	require.NoError(t, err)
	assetName, err := hex.DecodeString(assetNameHex)
	require.NoError(t, err)
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testOtherAddr).
		WithLovelace(2_000_000).
		WithAssets(mockledger.Asset{
			PolicyId:  policyBytes,
			AssetName: assetName,
			Amount:    amount,
		}).
		Build()
	require.NoError(t, err)
	return out
}

// buildAuthOutput builds a mock output at the mapping validator address
// with an inline datum and auth token.
func buildAuthOutput(t *testing.T, policyHex, authAssetNameHex string, datumCbor []byte) lcommon.TransactionOutput {
	t.Helper()
	policyBytes, err := hex.DecodeString(policyHex)
	require.NoError(t, err)
	assetName, err := hex.DecodeString(authAssetNameHex)
	require.NoError(t, err)
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testMappingAddr).
		WithLovelace(2_000_000).
		WithAssets(mockledger.Asset{
			PolicyId:  policyBytes,
			AssetName: assetName,
			Amount:    1,
		}).
		WithDatum(datumCbor).
		Build()
	require.NoError(t, err)
	return out
}

// buildInput builds a mock transaction input from a hex tx hash.
func buildInput(t *testing.T, txHashHex string, index uint32) lcommon.TransactionInput {
	t.Helper()
	txBytes, err := hex.DecodeString(txHashHex)
	require.NoError(t, err)
	inp, err := mockledger.NewSimpleTransactionInput(txBytes, index)
	require.NoError(t, err)
	return inp
}

// buildTx assembles a mock transaction with the given ID, inputs, and outputs.
func buildTx(t *testing.T, txHashHex string, inputs []lcommon.TransactionInput, outputs []lcommon.TransactionOutput) lcommon.Transaction {
	t.Helper()
	txBytes, err := hex.DecodeString(txHashHex)
	require.NoError(t, err)
	builder := mockledger.NewTransactionBuilder().
		WithId(txBytes).
		WithFee(200_000)
	for _, inp := range inputs {
		builder = builder.WithInputs(inp)
	}
	for _, out := range outputs {
		builder = builder.WithOutputs(out)
	}
	tx, err := builder.Build()
	require.NoError(t, err)
	return tx
}

// simpleDatumCbor returns a minimal valid CBOR encoding (integer 1) that
// can be used as an inline datum.
func simpleDatumCbor(t *testing.T) []byte {
	t.Helper()
	encoded, err := cbor.Encode(uint64(1))
	require.NoError(t, err)
	return encoded
}

// anyOutput is a helper that builds a plain lovelace-only output.
func anyOutput(t *testing.T) lcommon.TransactionOutput {
	t.Helper()
	out, err := mockledger.NewSimpleTransactionOutput(testOtherAddr, 1_500_000)
	require.NoError(t, err)
	return out
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

// TestCNightCreate_HappyPath verifies that a transaction output containing a
// cNIGHT token is written to midnight_asset_creates and tracked in memory.
func TestCNightCreate_HappyPath(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	txHash := pad32("aabbccdd")
	cnightOut := buildCNightOutput(t, testPolicyID, testAssetNameHex, 500)
	dummyIn := buildInput(t, pad32("1122334455667788"), 0)
	tx := buildTx(t, txHash, []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{cnightOut})

	block := testBlock(1, 100, 0xAA)
	require.NoError(t, idx.processBlock(block, []lcommon.Transaction{tx}, 1_000_000))

	var creates []models.MidnightAssetCreate
	require.NoError(t, store.DB().Find(&creates).Error)
	require.Len(t, creates, 1)
	assert.Equal(t, uint64(500), creates[0].Quantity)
	assert.Equal(t, uint64(1), creates[0].BlockNumber)
	assert.Equal(t, uint32(0), creates[0].OutputIndex)

	idx.mu.RLock()
	_, ok := idx.cNightUTxOs[utxoKey{TxHash: txHash, Index: 0}]
	idx.mu.RUnlock()
	assert.True(t, ok, "cNIGHT UTxO must be tracked in-memory after create")
}

// TestCNightSpend_HappyPath verifies that spending a tracked cNIGHT UTxO
// writes a midnight_asset_spend row and removes it from the in-memory set.
func TestCNightSpend_HappyPath(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	// Block 1: create the cNIGHT UTxO.
	createTxHash := pad32("cccc0000")
	cnightOut := buildCNightOutput(t, testPolicyID, testAssetNameHex, 250)
	dummyIn := buildInput(t, pad32("dddd0000"), 0)
	createTx := buildTx(t, createTxHash, []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{cnightOut})
	require.NoError(t, idx.processBlock(testBlock(1, 100, 0x01), []lcommon.Transaction{createTx}, 1_000))

	// Block 2: spend the cNIGHT UTxO.
	spendTxHash := pad32("eeee0000")
	spendIn := buildInput(t, createTxHash, 0)
	spendTx := buildTx(t, spendTxHash, []lcommon.TransactionInput{spendIn}, []lcommon.TransactionOutput{anyOutput(t)})
	require.NoError(t, idx.processBlock(testBlock(2, 200, 0x02), []lcommon.Transaction{spendTx}, 2_000))

	var spends []models.MidnightAssetSpend
	require.NoError(t, store.DB().Find(&spends).Error)
	require.Len(t, spends, 1)
	assert.Equal(t, uint64(250), spends[0].Quantity)
	assert.Equal(t, uint64(2), spends[0].BlockNumber)

	idx.mu.RLock()
	_, ok := idx.cNightUTxOs[utxoKey{TxHash: createTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.False(t, ok, "spent cNIGHT UTxO must be removed from in-memory set")
}

// TestRegistration_HappyPath verifies that a transaction output at the mapping
// validator address with the correct auth policy+name and inline datum is
// written to midnight_registrations.
func TestRegistration_HappyPath(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	datumCbor := simpleDatumCbor(t)
	// Use the expected auth policy (testAuthPolicyID).
	regOut := buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datumCbor)
	dummyIn := buildInput(t, pad32("aaaa1111"), 0)
	regTxHash := pad32("bbbb1111")
	regTx := buildTx(t, regTxHash, []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{regOut})

	require.NoError(t, idx.processBlock(testBlock(5, 500, 0x05), []lcommon.Transaction{regTx}, 5_000))

	var regs []models.MidnightRegistration
	require.NoError(t, store.DB().Find(&regs).Error)
	require.Len(t, regs, 1)
	assert.Equal(t, uint64(5), regs[0].BlockNumber)
	assert.NotEmpty(t, regs[0].FullDatum)

	idx.mu.RLock()
	_, ok := idx.regUTxOs[utxoKey{TxHash: regTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.True(t, ok, "registration UTxO must be tracked in-memory")
}

// TestRegistration_WrongPolicy verifies that a spoofed auth token under a
// different policy is NOT indexed when AuthTokenPolicyID is configured.
func TestRegistration_WrongPolicy(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	// Mint the auth asset name under a different (wrong) policy.
	wrongPolicy := "aaaabbbbccccddddeeeeffffaaaabbbbccccddddeeeeffffaaaabbbb"
	datumCbor := simpleDatumCbor(t)
	spoofedOut := buildAuthOutput(t, wrongPolicy, testAuthAssetNameHex, datumCbor)
	dummyIn := buildInput(t, pad32("aa000001"), 0)
	spoofedTx := buildTx(t, pad32("bb000001"),
		[]lcommon.TransactionInput{dummyIn},
		[]lcommon.TransactionOutput{spoofedOut})

	require.NoError(t, idx.processBlock(testBlock(1, 100, 0x01), []lcommon.Transaction{spoofedTx}, 1_000))

	var regs []models.MidnightRegistration
	require.NoError(t, store.DB().Find(&regs).Error)
	assert.Empty(t, regs, "spoofed auth token under wrong policy must not be indexed")
}

// TestDeregistration_HappyPath verifies that spending a tracked registration
// UTxO writes a midnight_deregistrations row and clears the in-memory entry.
func TestDeregistration_HappyPath(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	authPolicy := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	datumCbor := simpleDatumCbor(t)

	// Block 1: register.
	regTxHash := pad32("ee000001")
	regOut := buildAuthOutput(t, authPolicy, testAuthAssetNameHex, datumCbor)
	dummyIn := buildInput(t, pad32("ff000001"), 0)
	regTx := buildTx(t, regTxHash, []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{regOut})
	require.NoError(t, idx.processBlock(testBlock(1, 100, 0x10), []lcommon.Transaction{regTx}, 1_000))

	// Block 2: deregister (spend the registration UTxO).
	deregTxHash := pad32("ee000002")
	deregIn := buildInput(t, regTxHash, 0)
	deregTx := buildTx(t, deregTxHash, []lcommon.TransactionInput{deregIn}, []lcommon.TransactionOutput{anyOutput(t)})
	require.NoError(t, idx.processBlock(testBlock(2, 200, 0x20), []lcommon.Transaction{deregTx}, 2_000))

	var deregs []models.MidnightDeregistration
	require.NoError(t, store.DB().Find(&deregs).Error)
	require.Len(t, deregs, 1)
	assert.Equal(t, uint64(2), deregs[0].BlockNumber)
	assert.NotEmpty(t, deregs[0].FullDatum)

	idx.mu.RLock()
	_, ok := idx.regUTxOs[utxoKey{TxHash: regTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.False(t, ok, "deregistered UTxO must be removed from in-memory set")
}

// TestMixedBlock_RelevantAndNonRelevant verifies that only Midnight-relevant
// outputs are indexed when a block contains a mix of relevant and irrelevant
// transactions.
func TestMixedBlock_RelevantAndNonRelevant(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	// Non-relevant tx 1: plain ADA transfer.
	plainTx := buildTx(t, pad32("ef000001"),
		[]lcommon.TransactionInput{buildInput(t, pad32("ef000000"), 0)},
		[]lcommon.TransactionOutput{anyOutput(t)})

	// Non-relevant tx 2: asset from a different policy.
	wrongPolicyID := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	wrongPolicyBytes, err := hex.DecodeString(wrongPolicyID)
	require.NoError(t, err)
	assetName, err := hex.DecodeString(testAssetNameHex)
	require.NoError(t, err)
	wrongOut, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testOtherAddr).
		WithLovelace(1_000_000).
		WithAssets(mockledger.Asset{
			PolicyId:  wrongPolicyBytes,
			AssetName: assetName,
			Amount:    100,
		}).
		Build()
	require.NoError(t, err)
	wrongTx := buildTx(t, pad32("ff000001"),
		[]lcommon.TransactionInput{buildInput(t, pad32("ff000000"), 0)},
		[]lcommon.TransactionOutput{wrongOut})

	// Relevant tx: real cNIGHT output.
	cnightTxHash := pad32("c1900001")
	cnightTx := buildTx(t, cnightTxHash,
		[]lcommon.TransactionInput{buildInput(t, pad32("c1900000"), 0)},
		[]lcommon.TransactionOutput{buildCNightOutput(t, testPolicyID, testAssetNameHex, 77)})

	block := testBlock(10, 1000, 0xFF)
	require.NoError(t, idx.processBlock(block, []lcommon.Transaction{plainTx, wrongTx, cnightTx}, 10_000))

	var creates []models.MidnightAssetCreate
	require.NoError(t, store.DB().Find(&creates).Error)
	require.Len(t, creates, 1, "only the real cNIGHT output must be indexed")
	assert.Equal(t, uint64(77), creates[0].Quantity)

	var spends []models.MidnightAssetSpend
	require.NoError(t, store.DB().Find(&spends).Error)
	assert.Empty(t, spends)

	var regs []models.MidnightRegistration
	require.NoError(t, store.DB().Find(&regs).Error)
	assert.Empty(t, regs)
}

// TestCNightCreate_Rollback verifies that rolling back a block deletes its
// cNIGHT create rows and removes the UTxO from the in-memory tracked set.
func TestCNightCreate_Rollback(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	txHash := pad32("dd000001")
	cnightOut := buildCNightOutput(t, testPolicyID, testAssetNameHex, 300)
	dummyIn := buildInput(t, pad32("dd000000"), 0)
	tx := buildTx(t, txHash, []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{cnightOut})
	block1 := testBlock(7, 700, 0x07)
	require.NoError(t, idx.processBlock(block1, []lcommon.Transaction{tx}, 7_000))

	var creates []models.MidnightAssetCreate
	require.NoError(t, store.DB().Find(&creates).Error)
	require.Len(t, creates, 1)

	idx.mu.RLock()
	_, inMem := idx.cNightUTxOs[utxoKey{TxHash: txHash, Index: 0}]
	idx.mu.RUnlock()
	require.True(t, inMem)

	idx.rollbackBlock(block1)

	require.NoError(t, store.DB().Find(&creates).Error)
	assert.Empty(t, creates, "create row must be deleted on rollback")

	idx.mu.RLock()
	_, inMem = idx.cNightUTxOs[utxoKey{TxHash: txHash, Index: 0}]
	idx.mu.RUnlock()
	assert.False(t, inMem, "UTxO must be removed from memory on rollback")
}

// TestCNightSpend_Rollback verifies that rolling back a block containing a
// cNIGHT spend deletes the spend row and restores the UTxO to memory.
func TestCNightSpend_Rollback(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	// Block 1: create.
	createTxHash := pad32("ee100001")
	cnightOut := buildCNightOutput(t, testPolicyID, testAssetNameHex, 150)
	block1 := testBlock(1, 100, 0x01)
	require.NoError(t, idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, createTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("ee100000"), 0)},
			[]lcommon.TransactionOutput{cnightOut})},
		1_000))

	// Block 2: spend.
	spendTxHash := pad32("ee200001")
	block2 := testBlock(2, 200, 0x02)
	require.NoError(t, idx.processBlock(block2,
		[]lcommon.Transaction{buildTx(t, spendTxHash,
			[]lcommon.TransactionInput{buildInput(t, createTxHash, 0)},
			[]lcommon.TransactionOutput{anyOutput(t)})},
		2_000))

	var spends []models.MidnightAssetSpend
	require.NoError(t, store.DB().Find(&spends).Error)
	require.Len(t, spends, 1)

	// Rollback block 2.
	idx.rollbackBlock(block2)

	require.NoError(t, store.DB().Find(&spends).Error)
	assert.Empty(t, spends, "spend row must be deleted on rollback")

	idx.mu.RLock()
	utxo, restored := idx.cNightUTxOs[utxoKey{TxHash: createTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.True(t, restored, "UTxO must be restored to memory after spend rollback")
	assert.Equal(t, uint64(150), utxo.Quantity)
}

// TestRegistration_Rollback verifies that rolling back a block deletes its
// registration rows and removes the UTxO from the in-memory tracked set.
func TestRegistration_Rollback(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	datumCbor := simpleDatumCbor(t)
	regTxHash := pad32("ff100001")
	regOut := buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datumCbor)
	block1 := testBlock(3, 300, 0x03)
	require.NoError(t, idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, regTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("ff100000"), 0)},
			[]lcommon.TransactionOutput{regOut})},
		3_000))

	var regs []models.MidnightRegistration
	require.NoError(t, store.DB().Find(&regs).Error)
	require.Len(t, regs, 1)

	idx.rollbackBlock(block1)

	require.NoError(t, store.DB().Find(&regs).Error)
	assert.Empty(t, regs, "registration row must be deleted on rollback")

	idx.mu.RLock()
	_, inMem := idx.regUTxOs[utxoKey{TxHash: regTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.False(t, inMem, "registration UTxO must be removed from memory on rollback")
}

// TestDeregistration_Rollback verifies that rolling back a block containing
// a deregistration deletes the dereg row and restores the reg UTxO to memory.
func TestDeregistration_Rollback(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupIndexer(t, store)

	datumCbor := simpleDatumCbor(t)

	// Block 1: register.
	regTxHash := pad32("cc100001")
	block1 := testBlock(1, 100, 0x01)
	require.NoError(t, idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, regTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("cc100000"), 0)},
			[]lcommon.TransactionOutput{buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datumCbor)})},
		1_000))

	// Block 2: deregister.
	deregTxHash := pad32("cc200001")
	block2 := testBlock(2, 200, 0x02)
	require.NoError(t, idx.processBlock(block2,
		[]lcommon.Transaction{buildTx(t, deregTxHash,
			[]lcommon.TransactionInput{buildInput(t, regTxHash, 0)},
			[]lcommon.TransactionOutput{anyOutput(t)})},
		2_000))

	var deregs []models.MidnightDeregistration
	require.NoError(t, store.DB().Find(&deregs).Error)
	require.Len(t, deregs, 1)

	// Rollback block 2.
	idx.rollbackBlock(block2)

	require.NoError(t, store.DB().Find(&deregs).Error)
	assert.Empty(t, deregs, "deregistration row must be deleted on rollback")

	idx.mu.RLock()
	reg, restored := idx.regUTxOs[utxoKey{TxHash: regTxHash, Index: 0}]
	idx.mu.RUnlock()
	assert.True(t, restored, "registration UTxO must be restored to memory after dereg rollback")
	assert.NotEmpty(t, reg.FullDatum)
}

// TestNew_PolicyIDLengthValidation verifies that New rejects policy IDs that
// are not exactly 28 bytes (56 hex characters).
func TestNew_PolicyIDLengthValidation(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Short cNIGHT policy (27 bytes = 54 hex chars).
	_, err := New(Config{
		Metadata:       store,
		Logger:         logger,
		CNightPolicyID: "0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1",
		CNightAssetName: testAssetNameHex,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cnight_policy_id")

	// Short auth token policy (27 bytes).
	_, err = New(Config{
		Metadata:          store,
		Logger:            logger,
		AuthTokenPolicyID: "cccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		AuthTokenAssetName: testAuthAssetNameHex,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth_token_policy_id")
}

// TestLoadTrackedUTxOs_RestoredOnStartup verifies that the indexer restores
// the in-memory tracked UTxO sets from the database on startup so that a
// spend arriving after restart is matched correctly.
func TestLoadTrackedUTxOs_RestoredOnStartup(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// Simulate a cNIGHT create written in a previous run.
	txHashBytes, err := hex.DecodeString(pad32("abcd0001"))
	require.NoError(t, err)
	require.NoError(t, store.CreateMidnightAssetCreate(nil, &models.MidnightAssetCreate{
		Address:     []byte{0x01},
		Quantity:    100,
		TxHash:      txHashBytes,
		OutputIndex: 0,
		BlockNumber: 1,
		BlockHash:   make([]byte, 32),
		TxIndex:     0,
	}))

	// A fresh indexer must load the create from DB.
	idx, err := New(Config{
		Metadata:                store,
		Logger:                  slog.New(slog.NewTextHandler(os.Stderr, nil)),
		CNightPolicyID:          testPolicyID,
		CNightAssetName:         testAssetNameHex,
		MappingValidatorAddress: testMappingAddr,
		AuthTokenAssetName:      testAuthAssetNameHex,
	})
	require.NoError(t, err)

	txHashHex := hex.EncodeToString(txHashBytes)
	idx.mu.RLock()
	_, ok := idx.cNightUTxOs[utxoKey{TxHash: txHashHex, Index: 0}]
	idx.mu.RUnlock()
	assert.True(t, ok, "create loaded from DB must appear in the in-memory tracked set")

	// Spend it and confirm the spend row is written.
	spendIn := buildInput(t, txHashHex, 0)
	spendTx := buildTx(t, pad32("5e0d0001"),
		[]lcommon.TransactionInput{spendIn},
		[]lcommon.TransactionOutput{anyOutput(t)})
	require.NoError(t, idx.processBlock(testBlock(2, 200, 0xBB), []lcommon.Transaction{spendTx}, 2_000))

	var spends []models.MidnightAssetSpend
	require.NoError(t, store.DB().Find(&spends).Error)
	require.Len(t, spends, 1)
	assert.Equal(t, uint64(100), spends[0].Quantity)
}

// -------------------------------------------------------------------------
// Governance / Ariadne / candidate tests
// -------------------------------------------------------------------------

// buildGovOutput builds an output at the given address carrying an inline datum.
// policyHex may be empty to produce a plain ADA output (used to verify the
// indexer does NOT record datums for unrecognised addresses).
func buildGovOutput(t *testing.T, addr string, datumCbor []byte) lcommon.TransactionOutput {
	t.Helper()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(addr).
		WithLovelace(2_000_000).
		WithDatum(datumCbor).
		Build()
	require.NoError(t, err)
	return out
}

// buildPolicyOutput builds an output that carries a token under policyHex at addr.
func buildPolicyOutput(t *testing.T, addr, policyHex string, datumCbor []byte) lcommon.TransactionOutput {
	t.Helper()
	policyBytes, err := hex.DecodeString(policyHex)
	require.NoError(t, err)
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(addr).
		WithLovelace(2_000_000).
		WithAssets(mockledger.Asset{PolicyId: policyBytes, AssetName: []byte("t"), Amount: 1}).
		WithDatum(datumCbor).
		Build()
	require.NoError(t, err)
	return out
}

// setupGovIndexer creates an Indexer wired for governance / Ariadne / candidate
// scanning, backed by the given SQLite store.
func setupGovIndexer(t *testing.T, store *sqlite.MetadataStoreSqlite) *Indexer {
	t.Helper()
	idx, err := New(Config{
		Metadata:                    store,
		Logger:                      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		TechnicalCommitteeAddress:   testMappingAddr,
		TechnicalCommitteePolicyID:  testGovPolicyID,
		CouncilAddress:              testCouncilAddr,
		CouncilPolicyID:             testGovPolicyID,
		PermissionedCandidatePolicy: testPermPolicyID,
		CommitteeCandidateAddress:   testMappingAddr,
		SlotToEpoch: func(slot uint64) (uint64, error) {
			return slot / 100, nil
		},
	})
	require.NoError(t, err)
	return idx
}

// TestGovernanceTechnicalCommitteeDatum verifies that an output at the
// Technical Committee address carrying the TC policy token and an inline
// datum is stored in midnight_governance_datums with datum_type =
// "technical_committee".
func TestGovernanceTechnicalCommitteeDatum(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	datum := simpleDatumCbor(t)
	// Must carry the TC policy token for the governance scan to trigger.
	govOut := buildPolicyOutput(t, testMappingAddr, testGovPolicyID, datum)
	dummyIn := buildInput(t, pad32("f0000001"), 0)
	tx := buildTx(t, pad32("f0000002"), []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{govOut})

	require.NoError(t, idx.processBlock(testBlock(1, 100, 0x01), []lcommon.Transaction{tx}, 1_000))

	var rows []models.MidnightGovernanceDatum
	require.NoError(t, store.DB().Find(&rows).Error)
	require.Len(t, rows, 1)
	assert.Equal(t, models.MidnightGovernanceDatumTypeTechnicalCommittee, rows[0].DatumType)
	assert.Equal(t, uint64(1), rows[0].BlockNumber)
	assert.Equal(t, datum, rows[0].Datum)
}

// TestGovernanceCouncilDatum verifies that an output at the Council address
// carrying the Council policy token and an inline datum is stored with
// datum_type = "council".
func TestGovernanceCouncilDatum(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	datum := simpleDatumCbor(t)
	// Must carry the Council policy token for the governance scan to trigger.
	govOut := buildPolicyOutput(t, testCouncilAddr, testGovPolicyID, datum)
	dummyIn := buildInput(t, pad32("c0000001"), 0)
	tx := buildTx(t, pad32("c0000002"), []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{govOut})

	require.NoError(t, idx.processBlock(testBlock(2, 200, 0x02), []lcommon.Transaction{tx}, 2_000))

	var rows []models.MidnightGovernanceDatum
	require.NoError(t, store.DB().Where("datum_type = ?", models.MidnightGovernanceDatumTypeCouncil).Find(&rows).Error)
	require.Len(t, rows, 1)
	assert.Equal(t, datum, rows[0].Datum)
}

// TestGovernanceTwoOutputsProduceTwoRows verifies that two governance outputs
// in the same block both produce independent DB rows (INSERT, not upsert).
func TestGovernanceTwoOutputsProduceTwoRows(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	datum1 := simpleDatumCbor(t)
	encoded2, err := cbor.Encode(uint64(2))
	require.NoError(t, err)
	datum2 := encoded2

	// Both outputs carry the TC policy token so the governance scan triggers.
	out1 := buildPolicyOutput(t, testMappingAddr, testGovPolicyID, datum1)
	out2 := buildPolicyOutput(t, testMappingAddr, testGovPolicyID, datum2)
	dummyIn := buildInput(t, pad32("d0000001"), 0)
	tx := buildTx(t, pad32("d0000002"), []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{out1, out2})

	require.NoError(t, idx.processBlock(testBlock(3, 300, 0x03), []lcommon.Transaction{tx}, 3_000))

	var rows []models.MidnightGovernanceDatum
	require.NoError(t, store.DB().Find(&rows).Error)
	require.Len(t, rows, 2, "each governance output must produce its own DB row")
}

// TestAriadneParamsDeduplicated verifies that re-scanning the same Ariadne
// datum does not trigger a second DB write, but a changed datum does get
// persisted (updating the existing row for the current epoch via upsert).
func TestAriadneParamsDeduplicated(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	datumA := simpleDatumCbor(t)
	encoded2, err := cbor.Encode(uint64(99))
	require.NoError(t, err)
	datumB := encoded2

	dummyIn := buildInput(t, pad32("a0000001"), 0)

	// Block 1: first Ariadne output with datumA.
	out1 := buildPolicyOutput(t, testOtherAddr, testPermPolicyID, datumA)
	tx1 := buildTx(t, pad32("a0000002"), []lcommon.TransactionInput{dummyIn}, []lcommon.TransactionOutput{out1})
	require.NoError(t, idx.processBlock(testBlock(1, 100, 0x11), []lcommon.Transaction{tx1}, 1_000))

	var params []models.MidnightAriadneParams
	require.NoError(t, store.DB().Find(&params).Error)
	require.Len(t, params, 1, "first Ariadne datum must be stored")
	assert.Equal(t, datumA, params[0].Datum)

	// Block 2: same datumA again — in-memory dedup must prevent a second write.
	dummyIn2 := buildInput(t, pad32("a0000003"), 0)
	out2 := buildPolicyOutput(t, testOtherAddr, testPermPolicyID, datumA)
	tx2 := buildTx(t, pad32("a0000004"), []lcommon.TransactionInput{dummyIn2}, []lcommon.TransactionOutput{out2})
	require.NoError(t, idx.processBlock(testBlock(2, 200, 0x22), []lcommon.Transaction{tx2}, 2_000))

	require.NoError(t, store.DB().Find(&params).Error)
	require.Len(t, params, 1, "duplicate Ariadne datum must not produce a second row")

	// Block 3: new datumB — changed datum must reach the store (upserts the epoch row).
	dummyIn3 := buildInput(t, pad32("a0000005"), 0)
	out3 := buildPolicyOutput(t, testOtherAddr, testPermPolicyID, datumB)
	tx3 := buildTx(t, pad32("a0000006"), []lcommon.TransactionInput{dummyIn3}, []lcommon.TransactionOutput{out3})
	require.NoError(t, idx.processBlock(testBlock(3, 300, 0x33), []lcommon.Transaction{tx3}, 3_000))

	// The store upserts by epoch — since all blocks share epoch 0 in this
	// direct-call path (processBlock doesn't advance SlotToEpoch), the changed
	// datum updates the same row rather than inserting a new one. The invariant
	// being tested is that lastAriadneDatum tracks the written value and the
	// store row reflects the latest non-duplicate datum.
	require.NoError(t, store.DB().Find(&params).Error)
	require.Len(t, params, 1, "upsert-by-epoch produces one row per epoch in direct-call path")
	assert.Equal(t, datumB, params[0].Datum, "changed Ariadne datum must update the stored row")

	// Verify that lastAriadneDatum was updated to datumB so a subsequent
	// repeat of datumB would be deduplicated.
	idx.mu.RLock()
	lastDatum := idx.lastAriadneDatum
	idx.mu.RUnlock()
	assert.Equal(t, datumB, lastDatum)
}

// TestCandidateAddRemove verifies the in-memory candidate set lifecycle:
// an output at the candidate address is added, a spend removes it, and
// an epoch transition snapshots the remaining state.
func TestCandidateAddRemove(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	datum := simpleDatumCbor(t)

	// Block 1: add two candidates at testMappingAddr (candidate address).
	txHash1 := pad32("ca000001")
	txHash2 := pad32("ca000002")
	dummyIn1 := buildInput(t, pad32("ca000000"), 0)
	dummyIn2 := buildInput(t, pad32("ca000009"), 0)
	out1 := buildGovOutput(t, testMappingAddr, datum)
	out2 := buildGovOutput(t, testMappingAddr, datum)
	tx1 := buildTx(t, txHash1, []lcommon.TransactionInput{dummyIn1}, []lcommon.TransactionOutput{out1})
	tx2 := buildTx(t, txHash2, []lcommon.TransactionInput{dummyIn2}, []lcommon.TransactionOutput{out2})
	require.NoError(t, idx.processBlock(testBlock(1, 100, 0xCA), []lcommon.Transaction{tx1, tx2}, 1_000))

	idx.mu.RLock()
	require.Len(t, idx.candidates, 2, "two candidate UTxOs must be tracked after block 1")
	idx.mu.RUnlock()

	// Block 2: spend candidate 1.
	spendIn := buildInput(t, txHash1, 0)
	spendTx := buildTx(t, pad32("ca000003"), []lcommon.TransactionInput{spendIn}, []lcommon.TransactionOutput{anyOutput(t)})
	require.NoError(t, idx.processBlock(testBlock(2, 200, 0xCB), []lcommon.Transaction{spendTx}, 2_000))

	idx.mu.RLock()
	require.Len(t, idx.candidates, 1, "spent candidate must be removed from the in-memory set")
	idx.mu.RUnlock()

	// Epoch transition: snapshot must contain the one remaining candidate.
	idx.handleEpochTransition(event.Event{
		Data: event.EpochTransitionEvent{PreviousEpoch: 1, NewEpoch: 2},
	})

	var snapshots []models.MidnightEpochCandidates
	require.NoError(t, store.DB().Find(&snapshots).Error)
	require.Len(t, snapshots, 1, "epoch transition must write a candidate snapshot")
	assert.Equal(t, uint64(1), snapshots[0].Epoch)

	var entries []candidateEntry
	require.NoError(t, fxcbor.Unmarshal(snapshots[0].CandidatesCbor, &entries))
	require.Len(t, entries, 1)
}

// TestCandidateEmptySnapshot verifies that an epoch transition with no
// candidates still writes a snapshot row (empty candidate set).
func TestCandidateEmptySnapshot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	idx.handleEpochTransition(event.Event{
		Data: event.EpochTransitionEvent{PreviousEpoch: 0, NewEpoch: 1},
	})

	var snapshots []models.MidnightEpochCandidates
	require.NoError(t, store.DB().Find(&snapshots).Error)
	require.Len(t, snapshots, 1, "epoch transition must write a snapshot even with no candidates")
	assert.Equal(t, uint64(0), snapshots[0].Epoch)
}

// TestEpochTransitionIdempotent verifies that replaying the same epoch
// transition does not produce duplicate snapshot rows.
func TestEpochTransitionIdempotent(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	idx := setupGovIndexer(t, store)

	et := event.Event{
		Data: event.EpochTransitionEvent{PreviousEpoch: 3, NewEpoch: 4},
	}
	idx.handleEpochTransition(et)
	idx.handleEpochTransition(et)

	var snapshots []models.MidnightEpochCandidates
	require.NoError(t, store.DB().Find(&snapshots).Error)
	require.Len(t, snapshots, 1, "duplicate epoch transition must not write a second snapshot row")
}
