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
	"bytes"
	"math"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildBlockSupportsAllEras verifies BuildBlock dispatches by era
// across the full era table — TPraos (Shelley/Allegra/Mary/Alonzo)
// and Praos (Babbage/Conway) — and that the block re-decodes through
// the era-correct constructor (issue #2124). Each subtest also asserts
// the concrete block type so a regression in decodeBlockFromCbor that
// returned the wrong era's struct would fail loudly.
func TestBuildBlockSupportsAllEras(t *testing.T) {
	creds := setupTestCredentials(t)

	const (
		maxTxSize        = uint(16384)
		maxBlockBodySize = uint(90112)
	)
	maxBlockExUnits := lcommon.ExUnits{
		Memory: 62000000,
		Steps:  20000000000,
	}
	maxTxExUnits := lcommon.ExUnits{
		Memory: 14000000,
		Steps:  10000000000,
	}

	// wantBlock is the concrete type the era's decoder must return.
	// Storing it as an any sentinel keeps the table compact.
	cases := []struct {
		name      string
		pparams   lcommon.ProtocolParameters
		wantBlock any
	}{
		{
			name: "shelley",
			pparams: &shelley.ShelleyProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    2,
			},
			wantBlock: (*shelley.ShelleyBlock)(nil),
		},
		{
			name: "allegra",
			pparams: &allegra.AllegraProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    3,
			},
			wantBlock: (*allegra.AllegraBlock)(nil),
		},
		{
			name: "mary",
			pparams: &mary.MaryProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    4,
			},
			wantBlock: (*mary.MaryBlock)(nil),
		},
		{
			name: "alonzo",
			pparams: &alonzo.AlonzoProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    5,
				MaxBlockExUnits:  maxBlockExUnits,
				MaxTxExUnits:     maxTxExUnits,
			},
			wantBlock: (*alonzo.AlonzoBlock)(nil),
		},
		{
			name: "babbage",
			pparams: &babbage.BabbageProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolMajor:    7,
				MaxBlockExUnits:  maxBlockExUnits,
				MaxTxExUnits:     maxTxExUnits,
			},
			wantBlock: (*babbage.BabbageBlock)(nil),
		},
		{
			name: "conway",
			pparams: &conway.ConwayProtocolParameters{
				MaxTxSize:        maxTxSize,
				MaxBlockBodySize: maxBlockBodySize,
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 9,
				},
				MaxBlockExUnits: maxBlockExUnits,
				MaxTxExUnits:    maxTxExUnits,
			},
			wantBlock: (*conway.ConwayBlock)(nil),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
				Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
				PParamsProvider: &mockPParamsProvider{pparams: tc.pparams},
				ChainTip: &mockChainTip{
					tip: ochainsync.Tip{
						Point: ocommon.Point{
							Slot: 1000,
							Hash: make([]byte, 32),
						},
						BlockNumber: 100,
					},
				},
				EpochNonce: &mockEpochNonceProvider{
					epoch: 1,
					nonce: make([]byte, 32),
				},
				Credentials: creds,
			})
			require.NoError(t, err)

			block, blockCbor, err := builder.BuildBlock(1001, 0)
			require.NoError(
				t,
				err,
				"BuildBlock must succeed in %s era (issue #2124)",
				tc.name,
			)
			require.NotNil(t, block)
			require.NotEmpty(t, blockCbor)

			assert.IsType(
				t,
				tc.wantBlock,
				block,
				"%s era must round-trip through the matching block constructor",
				tc.name,
			)
			assert.Equal(t, uint64(1001), block.SlotNumber())
			assert.Equal(t, uint64(101), block.BlockNumber())
			assert.Equal(t, 0, len(block.Transactions()))
		})
	}
}

// TestBuildBlockSupportsDijkstraEra verifies BuildBlock forges on the
// Dijkstra (Leios) era: a musashi block producer must build a block from
// *dijkstra.DijkstraProtocolParameters instead of failing the forge with
// "unsupported protocol parameter type". Dijkstra shares Conway's Praos
// block/header layout, so the forged block round-trips through the
// Dijkstra block constructor.
func TestBuildBlockSupportsDijkstraEra(t *testing.T) {
	creds := setupTestCredentials(t)

	// DijkstraProtocolParameters embeds ConwayProtocolParameters, so the
	// shared limits are set via the embedded field.
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err, "BuildBlock must succeed in the Dijkstra era")
	require.NotNil(t, block)
	require.NotEmpty(t, blockCbor)

	assert.IsType(
		t,
		(*dijkstra.DijkstraBlock)(nil),
		block,
		"Dijkstra era must round-trip through the Dijkstra block constructor",
	)
	assert.Equal(t, uint64(1001), block.SlotNumber())
	assert.Equal(t, uint64(101), block.BlockNumber())
	assert.Equal(t, 0, len(block.Transactions()))

	// The forged block's body hash must match the header commitment, i.e.
	// the encoded Dijkstra block_body with null certificate fields. A
	// mismatch means the network would reject the block.
	dblock := block.(*dijkstra.DijkstraBlock)
	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(t, present, "Dijkstra forge must emit the Leios header extension")
	assert.False(t, certified)
	_, _, announced := dblock.BlockHeader.LeiosAnnouncement()
	assert.False(t, announced)
	assert.Equal(
		t,
		dblock.BlockBodyHash(),
		dblock.CalculatedBlockBodyHash(),
		"forged Dijkstra block body hash must match the header commitment",
	)
}

func TestBuildBlockDijkstraAnnouncesLeiosEndorserBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	ebHash := lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size))
	block, _, err := builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: ebHash,
			Size: 1234,
		},
	})
	require.NoError(t, err)
	dblock := block.(*dijkstra.DijkstraBlock)

	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(t, present)
	assert.False(t, certified)
	gotHash, gotSize, ok := dblock.BlockHeader.LeiosAnnouncement()
	require.True(t, ok)
	assert.Equal(t, ebHash, gotHash)
	assert.Equal(t, uint64(1234), gotSize)
	assert.Nil(t, dblock.BlockBody.LeiosCertificate)
	assert.Equal(t, dblock.BlockBodyHash(), dblock.CalculatedBlockBodyHash())
}

func TestBuildBlockDijkstraRejectsOversizeLeiosAnnouncement(t *testing.T) {
	creds := setupTestCredentials(t)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        16384,
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool:         &mockMempool{transactions: []MempoolTransaction{}},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	ebHash := lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size))
	_, _, err = builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: ebHash,
			Size: uint64(math.MaxUint32) + 1,
		},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "leios announcement size exceeds uint32")
}

func TestBuildBlockDijkstraCertifiesAndAnnouncesLeiosEndorserBlocks(t *testing.T) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	signature := make([]byte, lcommon.LeiosBlsSignatureSize)
	for i := range signature {
		signature[i] = byte(i)
	}
	announcedHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x44}, 32))
	block, _, err := builder.BuildBlockWithLeios(1001, 0, LeiosBlockData{
		Announcement: &LeiosEndorserBlockAnnouncement{
			Hash: announcedHash,
			Size: 1234,
		},
		Certificate: &lcommon.LeiosEbCertificate{
			SlotNo:              900,
			EndorserBlockHash:   lcommon.NewBlake2b256(make([]byte, lcommon.Blake2b256Size)),
			Signers:             []byte{0x80},
			AggregatedSignature: signature,
		},
	})
	require.NoError(t, err)
	dblock := block.(*dijkstra.DijkstraBlock)

	certified, present := dblock.BlockHeader.LeiosCertified()
	require.True(t, present)
	assert.True(t, certified)
	gotHash, gotSize, announced := dblock.BlockHeader.LeiosAnnouncement()
	require.True(t, announced)
	assert.Equal(t, announcedHash, gotHash)
	assert.Equal(t, uint64(1234), gotSize)
	require.NotNil(t, dblock.BlockBody.LeiosCertificate)
	assert.Equal(t, []byte{0x80}, dblock.BlockBody.LeiosCertificate.Signers)
	assert.Equal(t, signature, dblock.BlockBody.LeiosCertificate.AggregatedSignature)
	assert.Empty(t, dblock.Transactions())
	assert.Equal(t, dblock.BlockBodyHash(), dblock.CalculatedBlockBodyHash())
}

func TestBuildBlockDijkstraNormalizesAdmittedTxForBlock(t *testing.T) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: 90112,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, blockCbor, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Len(t, block.Transactions(), 1)

	dblock := block.(*dijkstra.DijkstraBlock)
	assert.Equal(
		t,
		dblock.BlockBodyHash(),
		dblock.CalculatedBlockBodyHash(),
		"forged Dijkstra block body hash must match the normalized body",
	)

	var originalFields []cbor.RawMessage
	_, err = cbor.Decode(txCbor, &originalFields)
	require.NoError(t, err)
	require.Len(t, originalFields, 4)

	var rawBlock []cbor.RawMessage
	_, err = cbor.Decode(blockCbor, &rawBlock)
	require.NoError(t, err)
	require.Len(t, rawBlock, 2)

	var rawBody []cbor.RawMessage
	_, err = cbor.Decode(rawBlock[1], &rawBody)
	require.NoError(t, err)
	require.Len(t, rawBody, 4)

	var rawTxs []cbor.RawMessage
	_, err = cbor.Decode(rawBody[1], &rawTxs)
	require.NoError(t, err)
	require.Len(t, rawTxs, 1)

	var blockTxFields []cbor.RawMessage
	_, err = cbor.Decode(rawTxs[0], &blockTxFields)
	require.NoError(t, err)
	require.Len(t, blockTxFields, 3)
	assert.Equal(t, originalFields[0], blockTxFields[0])
	assert.Equal(t, originalFields[1], blockTxFields[1])
	assert.Equal(t, originalFields[3], blockTxFields[2])
}

func TestBuildBlockDijkstraRespectsActualBlockBodySize(t *testing.T) {
	creds := setupTestCredentials(t)
	txCbor := makeMinimalTxCbor(t, 0x01, 0)

	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: uint(len(txCbor)),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}

	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: creds,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.NotNil(t, block)

	require.Empty(t, block.Transactions())
	require.LessOrEqual(
		t,
		block.BlockBodySize(),
		uint64(pparams.MaxBlockBodySize),
	)
}
