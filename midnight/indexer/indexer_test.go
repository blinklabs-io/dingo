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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	idx.processBlock(block, []lcommon.Transaction{tx}, 1_000_000)

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
	idx.processBlock(testBlock(1, 100, 0x01), []lcommon.Transaction{createTx}, 1_000)

	// Block 2: spend the cNIGHT UTxO.
	spendTxHash := pad32("eeee0000")
	spendIn := buildInput(t, createTxHash, 0)
	spendTx := buildTx(t, spendTxHash, []lcommon.TransactionInput{spendIn}, []lcommon.TransactionOutput{anyOutput(t)})
	idx.processBlock(testBlock(2, 200, 0x02), []lcommon.Transaction{spendTx}, 2_000)

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

	idx.processBlock(testBlock(5, 500, 0x05), []lcommon.Transaction{regTx}, 5_000)

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

	idx.processBlock(testBlock(1, 100, 0x01), []lcommon.Transaction{spoofedTx}, 1_000)

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
	idx.processBlock(testBlock(1, 100, 0x10), []lcommon.Transaction{regTx}, 1_000)

	// Block 2: deregister (spend the registration UTxO).
	deregTxHash := pad32("ee000002")
	deregIn := buildInput(t, regTxHash, 0)
	deregTx := buildTx(t, deregTxHash, []lcommon.TransactionInput{deregIn}, []lcommon.TransactionOutput{anyOutput(t)})
	idx.processBlock(testBlock(2, 200, 0x20), []lcommon.Transaction{deregTx}, 2_000)

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
	idx.processBlock(block, []lcommon.Transaction{plainTx, wrongTx, cnightTx}, 10_000)

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
	idx.processBlock(block1, []lcommon.Transaction{tx}, 7_000)

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
	idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, createTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("ee100000"), 0)},
			[]lcommon.TransactionOutput{cnightOut})},
		1_000)

	// Block 2: spend.
	spendTxHash := pad32("ee200001")
	block2 := testBlock(2, 200, 0x02)
	idx.processBlock(block2,
		[]lcommon.Transaction{buildTx(t, spendTxHash,
			[]lcommon.TransactionInput{buildInput(t, createTxHash, 0)},
			[]lcommon.TransactionOutput{anyOutput(t)})},
		2_000)

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
	idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, regTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("ff100000"), 0)},
			[]lcommon.TransactionOutput{regOut})},
		3_000)

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
	idx.processBlock(block1,
		[]lcommon.Transaction{buildTx(t, regTxHash,
			[]lcommon.TransactionInput{buildInput(t, pad32("cc100000"), 0)},
			[]lcommon.TransactionOutput{buildAuthOutput(t, testAuthPolicyID, testAuthAssetNameHex, datumCbor)})},
		1_000)

	// Block 2: deregister.
	deregTxHash := pad32("cc200001")
	block2 := testBlock(2, 200, 0x02)
	idx.processBlock(block2,
		[]lcommon.Transaction{buildTx(t, deregTxHash,
			[]lcommon.TransactionInput{buildInput(t, regTxHash, 0)},
			[]lcommon.TransactionOutput{anyOutput(t)})},
		2_000)

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
	idx.processBlock(testBlock(2, 200, 0xBB), []lcommon.Transaction{spendTx}, 2_000)

	var spends []models.MidnightAssetSpend
	require.NoError(t, store.DB().Find(&spends).Error)
	require.Len(t, spends, 1)
	assert.Equal(t, uint64(100), spends[0].Quantity)
}
