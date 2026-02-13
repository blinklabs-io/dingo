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

package forging

import (
	"encoding/hex"
	"errors"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMempool implements MempoolProvider for testing.
type mockMempool struct {
	transactions []MempoolTransaction
}

func (m *mockMempool) Transactions() []MempoolTransaction {
	return m.transactions
}

// mockPParamsProvider implements ProtocolParamsProvider for testing.
type mockPParamsProvider struct {
	pparams lcommon.ProtocolParameters
}

func (m *mockPParamsProvider) GetCurrentPParams() lcommon.ProtocolParameters {
	return m.pparams
}

// mockChainTip implements ChainTipProvider for testing.
type mockChainTip struct {
	tip ochainsync.Tip
}

func (m *mockChainTip) Tip() ochainsync.Tip {
	return m.tip
}

// mockEpochNonceProvider implements EpochNonceProvider for testing.
type mockEpochNonceProvider struct {
	epoch uint64
	nonce []byte
}

func (m *mockEpochNonceProvider) CurrentEpoch() uint64 {
	return m.epoch
}

func (m *mockEpochNonceProvider) EpochNonce(_ uint64) []byte {
	return m.nonce
}

// setupTestCredentials creates a temporary directory with test key files
// and returns loaded pool credentials for testing.
func setupTestCredentials(t *testing.T) *PoolCredentials {
	t.Helper()
	vrfPath, kesPath, opCertPath := createTestKeys(t)
	creds := NewPoolCredentials()
	require.NoError(t, creds.LoadFromFiles(vrfPath, kesPath, opCertPath))
	return creds
}

func TestNewDefaultBlockBuilder(t *testing.T) {
	creds := setupTestCredentials(t)

	mempool := &mockMempool{}
	pparams := &mockPParamsProvider{}
	chainTip := &mockChainTip{}
	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	// Test missing mempool
	_, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         nil,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mempool provider is required")

	// Test missing pparams provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: nil,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "protocol params provider is required")

	// Test missing chain tip provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        nil,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chain tip provider is required")

	// Test missing epoch nonce provider
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      nil,
		Credentials:     creds,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "epoch nonce provider is required")

	// Test missing credentials
	_, err = NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     nil,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool credentials are required")

	// Test successful creation
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparams,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)
	assert.NotNil(t, builder)
}

func TestBuildBlockEmptyMempool(t *testing.T) {
	creds := setupTestCredentials(t)

	// Setup mocks
	mempool := &mockMempool{transactions: []MempoolTransaction{}}

	// Create Conway protocol parameters
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build a block with empty mempool
	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	assert.NotNil(t, block)
	assert.NotEmpty(t, blockCbor)

	// Verify block properties
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, 0, len(block.Transactions()))
}

func TestBuildBlockMissingVRFKey(t *testing.T) {
	// Create credentials with nil VRF key to test error handling
	creds := &PoolCredentials{
		vrfSKey: nil,
		vrfVKey: nil, // This should cause BuildBlock to fail
		kesSKey: nil,
		kesVKey: make([]byte, 32), // Valid size
		opCert: &OpCert{
			KESVKey:     make([]byte, 32),
			IssueNumber: 0,
			KESPeriod:   0,
			Signature:   make([]byte, 64),
			ColdVKey:    make([]byte, 32),
		},
	}

	mempool := &mockMempool{transactions: []MempoolTransaction{}}

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder := &DefaultBlockBuilder{
		logger:          slog.Default(),
		mempool:         mempool,
		pparamsProvider: pparamsProvider,
		chainTip:        chainTip,
		epochNonce:      epochNonce,
		creds:           creds,
	}

	// Build should fail with missing VRF key
	_, _, err := builder.BuildBlock(1001, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VRF verification key not loaded")
}

func TestBuildBlockInvalidColdVKeySize(t *testing.T) {
	// Create credentials with invalid cold vkey size
	creds := &PoolCredentials{
		vrfSKey: make([]byte, 32),
		vrfVKey: make([]byte, 32), // Valid
		kesSKey: nil,
		kesVKey: make([]byte, 32), // Valid
		opCert: &OpCert{
			KESVKey:     make([]byte, 32),
			IssueNumber: 0,
			KESPeriod:   0,
			Signature:   make([]byte, 64),
			ColdVKey:    make([]byte, 16), // Invalid size - should be 32
		},
	}

	mempool := &mockMempool{transactions: []MempoolTransaction{}}

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder := &DefaultBlockBuilder{
		logger:          slog.Default(),
		mempool:         mempool,
		pparamsProvider: pparamsProvider,
		chainTip:        chainTip,
		epochNonce:      epochNonce,
		creds:           creds,
	}

	// Build should fail with invalid cold vkey size
	_, _, err := builder.BuildBlock(1001, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cold verification key size")
}

func TestBuildBlockInvalidVRFVKeySize(t *testing.T) {
	// Create credentials with invalid VRF vkey size
	creds := &PoolCredentials{
		vrfSKey: make([]byte, 32),
		vrfVKey: make([]byte, 16), // Invalid size - should be 32
		kesSKey: nil,
		kesVKey: make([]byte, 32),
		opCert: &OpCert{
			KESVKey:     make([]byte, 32),
			IssueNumber: 0,
			KESPeriod:   0,
			Signature:   make([]byte, 64),
			ColdVKey:    make([]byte, 32),
		},
	}

	mempool := &mockMempool{transactions: []MempoolTransaction{}}

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder := &DefaultBlockBuilder{
		logger:          slog.Default(),
		mempool:         mempool,
		pparamsProvider: pparamsProvider,
		chainTip:        chainTip,
		epochNonce:      epochNonce,
		creds:           creds,
	}

	// Build should fail with invalid VRF vkey size
	_, _, err := builder.BuildBlock(1001, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid VRF verification key size")
}

func TestBuildBlockTxExceedsMaxSize(t *testing.T) {
	creds := setupTestCredentials(t)

	// Create a transaction that's larger than MaxTxSize
	largeTxCbor := make([]byte, 20000) // 20KB, exceeds 16KB MaxTxSize

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "large_tx",
				Cbor: largeTxCbor,
				Type: conway.TxTypeConway,
			},
		},
	}

	// Set small MaxTxSize to force skipping
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build block - oversized tx should be skipped
	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// Block should have no transactions (oversized tx was skipped)
	assert.Equal(t, 0, len(block.Transactions()))
}

func TestBuildBlockNonConwayParams(t *testing.T) {
	creds := setupTestCredentials(t)

	// Setup mocks with nil pparams (simulating error)
	mempool := &mockMempool{transactions: []MempoolTransaction{}}
	pparamsProvider := &mockPParamsProvider{pparams: nil}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	// Build should fail with nil pparams
	_, _, err = builder.BuildBlock(1001, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get protocol parameters")
}

func TestBuildBlockCborRoundTrip(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	t.Run("empty mempool", func(t *testing.T) {
		mempool := &mockMempool{transactions: []MempoolTransaction{}}

		builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
			Mempool:         mempool,
			PParamsProvider: pparamsProvider,
			ChainTip:        chainTip,
			EpochNonce:      epochNonce,
			Credentials:     creds,
		})
		require.NoError(t, err)

		block, blockCbor, err := builder.BuildBlock(1001, 0)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.NotEmpty(t, blockCbor)

		decodedBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
		require.NoError(t, err)

		assert.Equal(t, block.SlotNumber(), decodedBlock.SlotNumber())
		assert.Equal(t, block.BlockNumber(), decodedBlock.BlockNumber())
		assert.Equal(t, 0, len(decodedBlock.Transactions()))

		reencodedCbor := decodedBlock.Cbor()
		assert.Equal(t, blockCbor, reencodedCbor, "CBOR round-trip should produce identical bytes")
	})

	t.Run("with transactions", func(t *testing.T) {
		txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
		txCbor2 := makeMinimalTxCbor(t, 0x02, 0)

		mempool := &mockMempool{
			transactions: []MempoolTransaction{
				{Hash: "tx1", Cbor: txCbor1, Type: conway.TxTypeConway},
				{Hash: "tx2", Cbor: txCbor2, Type: conway.TxTypeConway},
			},
		}

		builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
			Mempool:         mempool,
			PParamsProvider: pparamsProvider,
			ChainTip:        chainTip,
			EpochNonce:      epochNonce,
			Credentials:     creds,
		})
		require.NoError(t, err)

		block, blockCbor, err := builder.BuildBlock(1001, 0)
		require.NoError(t, err)
		require.NotNil(t, block)
		require.NotEmpty(t, blockCbor)

		assert.Equal(t, 2, len(block.Transactions()))

		decodedBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
		require.NoError(t, err)

		assert.Equal(t, block.SlotNumber(), decodedBlock.SlotNumber())
		assert.Equal(t, block.BlockNumber(), decodedBlock.BlockNumber())
		assert.Equal(t, 2, len(decodedBlock.Transactions()))

		reencodedCbor := decodedBlock.Cbor()
		assert.Equal(t, blockCbor, reencodedCbor, "CBOR round-trip should produce identical bytes")
	})
}

// makeMinimalTxCbor creates a minimal valid Conway transaction CBOR.
// The txID byte distinguishes transactions. The padding parameter adds
// extra bytes to the transaction body via a metadata field, allowing
// size-based tests to control transaction size.
func makeMinimalTxCbor(t *testing.T, txID byte, padding int) []byte {
	t.Helper()

	txHash := make([]byte, 32)
	txHash[0] = txID

	// Conway transaction body: {0: inputs, 2: fee}
	// Inputs are encoded as a tagged set (tag 258)
	input := cbor.NewConstructor(0, cbor.IndefLengthList{})
	_ = input // not used directly; we encode manually below

	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{txHash, uint64(0)}},
		},
		2: uint64(200000),
	}

	// Add padding via an output with a large address if needed
	if padding > 0 {
		addr := make([]byte, padding)
		addr[0] = 0x61 // Shelley enterprise address header byte
		bodyMap[1] = []any{
			[]any{addr, uint64(1000000)},
		}
	}

	// Full Conway tx: [body, witnesses, isValid, auxData]
	txArr := []any{bodyMap, map[uint]any{}, true, nil}
	txCbor, err := cbor.Encode(txArr)
	require.NoError(t, err)

	// Verify it actually decodes
	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err, "generated CBOR must decode as a valid Conway tx")

	return txCbor
}

func TestBuildBlockBlockSizeLimit(t *testing.T) {
	creds := setupTestCredentials(t)

	// Create valid transactions using minimal CBOR
	txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
	txCbor2 := makeMinimalTxCbor(t, 0x02, 0)
	txCbor3 := makeMinimalTxCbor(t, 0x03, 0)
	txSize := len(txCbor1)

	t.Logf(
		"tx CBOR size: %d bytes, hex: %s...",
		txSize,
		hex.EncodeToString(txCbor1[:min(32, len(txCbor1))]),
	)

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{Hash: "tx1", Cbor: txCbor1, Type: conway.TxTypeConway},
			{Hash: "tx2", Cbor: txCbor2, Type: conway.TxTypeConway},
			{Hash: "tx3", Cbor: txCbor3, Type: conway.TxTypeConway},
		},
	}

	// MaxBlockBodySize allows 2 transactions but not 3
	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        uint(txSize * 2),
		MaxBlockBodySize: uint(txSize*2 + txSize/2), // 2.5x tx size
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{epoch: 1, nonce: make([]byte, 32)}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// Block should include exactly 2 transactions (third excluded by size limit)
	assert.Equal(
		t, 2, len(block.Transactions()),
		"block should include 2 txs and exclude the 3rd due to size limit",
	)
}

// mockTxValidator implements TxValidator for testing. It rejects
// transactions whose hashes appear in the rejectHashes set.
type mockTxValidator struct {
	rejectHashes map[string]struct{}
}

func (v *mockTxValidator) ValidateTx(tx ledger.Transaction) error {
	if _, reject := v.rejectHashes[tx.Hash().String()]; reject {
		return errors.New("transaction no longer valid")
	}
	return nil
}

// makeMinimalTxCborWithInput creates a minimal Conway transaction
// CBOR that spends a specific input (inputHash, inputIndex). This
// allows tests to construct double-spend scenarios.
func makeMinimalTxCborWithInput(
	t *testing.T,
	inputHash []byte,
	inputIndex uint64,
) []byte {
	t.Helper()

	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputHash, inputIndex}},
		},
		2: uint64(200000),
	}

	txArr := []any{bodyMap, map[uint]any{}, true, nil}
	txCbor, err := cbor.Encode(txArr)
	require.NoError(t, err)

	_, err = conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(
		t,
		err,
		"generated CBOR must decode as a valid Conway tx",
	)

	return txCbor
}

func TestBuildBlockRevalidation(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	t.Run(
		"rejects invalid transactions",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
			txCbor2 := makeMinimalTxCbor(t, 0x02, 0)
			txCbor3 := makeMinimalTxCbor(t, 0x03, 0)

			// Decode tx2 to get its hash for the reject list
			decodedTx2, err := conway.NewConwayTransactionFromCbor(
				txCbor2,
			)
			require.NoError(t, err)
			tx2Hash := decodedTx2.Hash().String()

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx3",
						Cbor: txCbor3,
						Type: conway.TxTypeConway,
					},
				},
			}

			// Validator rejects tx2
			validator := &mockTxValidator{
				rejectHashes: map[string]struct{}{
					tx2Hash: {},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     validator,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			// tx2 should be excluded; tx1 and tx3 included
			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include 2 txs (tx2 rejected by validator)",
			)
		},
	)

	t.Run(
		"rejects all invalid transactions",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)
			txCbor2 := makeMinimalTxCbor(t, 0x02, 0)

			decodedTx1, err := conway.NewConwayTransactionFromCbor(
				txCbor1,
			)
			require.NoError(t, err)
			decodedTx2, err := conway.NewConwayTransactionFromCbor(
				txCbor2,
			)
			require.NoError(t, err)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			// Reject all
			validator := &mockTxValidator{
				rejectHashes: map[string]struct{}{
					decodedTx1.Hash().String(): {},
					decodedTx2.Hash().String(): {},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     validator,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				0,
				len(block.Transactions()),
				"block should have no txs when all fail re-validation",
			)
		},
	)

	t.Run(
		"nil validator skips re-validation",
		func(t *testing.T) {
			txCbor1 := makeMinimalTxCbor(t, 0x01, 0)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
				},
			}

			// No validator - should include all transactions
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
				TxValidator:     nil,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include tx when no validator is set",
			)
		},
	)
}

func TestBuildBlockDoubleSpendDetection(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	t.Run(
		"detects same input in two transactions",
		func(t *testing.T) {
			// Both transactions spend the same UTxO (same input
			// hash and index). The second should be excluded.
			sharedInputHash := make([]byte, 32)
			sharedInputHash[0] = 0xAA
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include only the first tx; second is "+
					"a double-spend",
			)
		},
	)

	t.Run(
		"allows different inputs",
		func(t *testing.T) {
			// Two transactions spending different UTxOs should
			// both be included.
			input1 := make([]byte, 32)
			input1[0] = 0x01
			input2 := make([]byte, 32)
			input2[0] = 0x02
			txCbor1 := makeMinimalTxCborWithInput(t, input1, 0)
			txCbor2 := makeMinimalTxCborWithInput(t, input2, 0)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include both txs with different inputs",
			)
		},
	)

	t.Run(
		"same hash different index is not a double-spend",
		func(t *testing.T) {
			// Two transactions spending the same tx hash but
			// different output indexes are valid.
			sharedHash := make([]byte, 32)
			sharedHash[0] = 0xBB
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedHash,
				1,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				2,
				len(block.Transactions()),
				"block should include both txs spending different "+
					"outputs of the same tx",
			)
		},
	)

	t.Run(
		"three-way double-spend keeps only first",
		func(t *testing.T) {
			// Three transactions all spending the same UTxO.
			// Only the first should be included.
			sharedInputHash := make([]byte, 32)
			sharedInputHash[0] = 0xCC
			txCbor1 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor2 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)
			txCbor3 := makeMinimalTxCborWithInput(
				t,
				sharedInputHash,
				0,
			)

			mempool := &mockMempool{
				transactions: []MempoolTransaction{
					{
						Hash: "tx1",
						Cbor: txCbor1,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx2",
						Cbor: txCbor2,
						Type: conway.TxTypeConway,
					},
					{
						Hash: "tx3",
						Cbor: txCbor3,
						Type: conway.TxTypeConway,
					},
				},
			}

			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         mempool,
				PParamsProvider: pparamsProvider,
				ChainTip:        chainTip,
				EpochNonce:      epochNonce,
				Credentials:     creds,
			})
			require.NoError(t, err)

			block, _, err := builder.BuildBlock(1001, 0)
			require.NoError(t, err)

			assert.Equal(
				t,
				1,
				len(block.Transactions()),
				"block should include only the first tx; second "+
					"and third are double-spends",
			)
		},
	)
}

func TestBuildBlockRevalidationAndDoubleSpend(t *testing.T) {
	creds := setupTestCredentials(t)

	pparams := &conway.ConwayProtocolParameters{
		MaxTxSize:        16384,
		MaxBlockBodySize: 90112,
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62000000,
			Steps:  20000000000,
		},
	}
	pparamsProvider := &mockPParamsProvider{pparams: pparams}

	chainTip := &mockChainTip{
		tip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 1000,
				Hash: make([]byte, 32),
			},
			BlockNumber: 100,
		},
	}

	epochNonce := &mockEpochNonceProvider{
		epoch: 1,
		nonce: make([]byte, 32),
	}

	// Scenario: tx1 fails re-validation, tx2 and tx3 share an
	// input. Expected result: tx1 excluded by validator, tx2
	// included, tx3 excluded by double-spend detection.
	sharedInputHash := make([]byte, 32)
	sharedInputHash[0] = 0xDD
	differentInputHash := make([]byte, 32)
	differentInputHash[0] = 0xEE

	txCbor1 := makeMinimalTxCborWithInput(t, differentInputHash, 0)
	txCbor2 := makeMinimalTxCborWithInput(t, sharedInputHash, 0)
	txCbor3 := makeMinimalTxCborWithInput(t, sharedInputHash, 0)

	decodedTx1, err := conway.NewConwayTransactionFromCbor(txCbor1)
	require.NoError(t, err)

	mempool := &mockMempool{
		transactions: []MempoolTransaction{
			{
				Hash: "tx1",
				Cbor: txCbor1,
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx2",
				Cbor: txCbor2,
				Type: conway.TxTypeConway,
			},
			{
				Hash: "tx3",
				Cbor: txCbor3,
				Type: conway.TxTypeConway,
			},
		},
	}

	validator := &mockTxValidator{
		rejectHashes: map[string]struct{}{
			decodedTx1.Hash().String(): {},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         mempool,
		PParamsProvider: pparamsProvider,
		ChainTip:        chainTip,
		EpochNonce:      epochNonce,
		Credentials:     creds,
		TxValidator:     validator,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)

	// tx1 rejected by validator, tx3 rejected as double-spend of tx2
	assert.Equal(
		t,
		1,
		len(block.Transactions()),
		"block should include only tx2 (tx1 failed validation, "+
			"tx3 is a double-spend of tx2)",
	)
}
