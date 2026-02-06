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
	"log/slog"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
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
