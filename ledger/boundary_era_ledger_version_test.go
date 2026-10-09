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
	"io"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const boundaryEraTestEpochLength = 1_000

func boundaryEraBabbagePParams(major uint) *babbage.BabbageProtocolParameters {
	rat := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	return &babbage.BabbageProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155_381,
		MaxBlockBodySize:   90_112,
		MaxTxSize:          16_384,
		MaxBlockHeaderSize: 1_100,
		KeyDeposit:         2_000_000,
		PoolDeposit:        500_000_000,
		MaxEpoch:           18,
		NOpt:               500,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(1, 5),
		ProtocolMajor:      major,
		MinPoolCost:        170_000_000,
		AdaPerUtxoByte:     4_310,
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  rat(577, 10_000),
			StepPrice: rat(721, 10_000_000),
		},
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// boundaryEraConwayBlock builds an empty Conway block in the first slot of
// epoch 1 whose header advertises headerMajor.
func boundaryEraConwayBlock(
	t *testing.T,
	headerMajor uint,
) *conway.ConwayBlock {
	t.Helper()
	block := &conway.ConwayBlock{BlockHeader: &conway.ConwayBlockHeader{}}
	block.BlockHeader.Body.BlockNumber = 1
	block.BlockHeader.Body.Slot = boundaryEraTestEpochLength + 1
	block.BlockHeader.Body.ProtoVersion.Major = uint64(headerMajor)
	encoded, err := cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	bodySize, err := serializedBlockBodySize(block)
	require.NoError(t, err)
	block.BlockHeader.Body.BlockBodySize = bodySize
	encoded, err = cbor.EncodeGeneric(block)
	require.NoError(t, err)
	block.SetCbor(encoded)
	return block
}

// boundaryEraCase describes a Babbage ledger at the end of epoch 0 and the
// Conway-encoded block that opens epoch 1.
type boundaryEraCase struct {
	// pparamsMajor is the protocol major of the epoch 0 parameters.
	pparamsMajor uint
	// headerMajor is the protocol major the boundary block header carries;
	// zero means Conway's.
	headerMajor uint
	// enableDijkstra makes Dijkstra a known era, so a header major of 12
	// names one.
	enableDijkstra bool
	// configure may schedule a hard fork with a TestXHardForkAtEpoch
	// override.
	configure func(cfg *cardano.CardanoNodeConfig)
}

// runBoundaryEraCase runs the boundary block through the pipeline and returns
// the ledger and the pipeline's result.
func runBoundaryEraCase(
	t *testing.T,
	tc boundaryEraCase,
) (*LedgerState, error) {
	t.Helper()
	headerMajor := tc.headerMajor
	if headerMajor == 0 {
		headerMajor = conway.MinProtocolVersionConway
	}
	block := boundaryEraConwayBlock(t, headerMajor)

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(
		context.Background(),
		[]chain.RawBlock{{
			Slot:        block.SlotNumber(),
			Hash:        block.Hash().Bytes(),
			BlockNumber: block.BlockNumber(),
			Type:        uint(gledger.BlockTypeConway),
			PrevHash:    block.PrevHash().Bytes(),
			Cbor:        block.Cbor(),
		}},
	))

	nodeConfig := newTestShelleyGenesisCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	nodeConfig.ShelleyGenesisHash = strings.Repeat("42", 32)
	conwayGenesis, err := os.Open("../config/cardano/preview/conway-genesis.json")
	require.NoError(t, err)
	t.Cleanup(func() { _ = conwayGenesis.Close() })
	require.NoError(t, nodeConfig.LoadConwayGenesisFromReader(conwayGenesis))
	if tc.configure != nil {
		tc.configure(nodeConfig)
	}

	nonce := bytes.Repeat([]byte{0x42}, 32)
	epoch0 := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: boundaryEraTestEpochLength,
		EraId:         eras.BabbageEraDesc.Id,
		Nonce:         nonce,
		EvolvingNonce: nonce,
	}
	require.NoError(t, db.SetEpoch(
		epoch0.StartSlot, epoch0.EpochId,
		nonce, nonce, nil, nil,
		epoch0.EraId, epoch0.SlotLength, epoch0.LengthInSlots,
		nil,
	))
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     nodeConfig,
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		PromRegistry:          prometheus.NewRegistry(),
		ManualBlockProcessing: true,
		ValidateHistorical:    true,
		EnableDijkstra:        tc.enableDijkstra,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	pparams := boundaryEraBabbagePParams(tc.pparamsMajor)
	ls.currentEra = eras.BabbageEraDesc
	ls.currentPParams = pparams
	ls.currentEpoch = epoch0
	ls.epochCache = []models.Epoch{epoch0}
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))
	seedEmptyRewardBasisForRollover(t, db, epoch0, pparams)

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{block}}
	close(results)
	return ls, ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

// TestBoundaryBlockEraMustMatchLedgerProtocolVersion pins that an epoch
// boundary moves the ledger into the era its protocol parameters (or a
// configured hard-fork epoch) name, not the era the first block of the new
// epoch happens to be encoded in.
func TestBoundaryBlockEraMustMatchLedgerProtocolVersion(t *testing.T) {
	t.Parallel()

	conwayAt := func(epoch uint64) func(*cardano.CardanoNodeConfig) {
		return func(cfg *cardano.CardanoNodeConfig) {
			cfg.TestConwayHardForkAtEpoch = &epoch
		}
	}
	requireStaysBabbage := func(t *testing.T, ls *LedgerState, err error) {
		t.Helper()
		require.Error(t, err, "a Conway boundary block the ledger did not authorize must be refused")
		require.ErrorIs(t, err, errRestartLedgerPipeline,
			"the refused block must be rewound, not re-read forever")
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		ver, verErr := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, verErr)
		assert.Equal(t, uint(8), ver.Major)
		assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
		assert.Equal(t, uint64(0), ls.currentTip.Point.Slot)
		assert.Equal(t, uint64(0),
			ls.config.ChainManager.PrimaryChain().Tip().Point.Slot,
			"the refused block must leave the primary chain")
	}
	requireBecomesConway := func(t *testing.T, ls *LedgerState, err error) {
		t.Helper()
		require.NoError(t, err)
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		ver, verErr := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, verErr)
		assert.Equal(t, uint(9), ver.Major)
		assert.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	}

	t.Run("conway block without a ratified major bump is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{pparamsMajor: 8})
		requireStaysBabbage(t, ls, err)
	})
	t.Run("conway block before the configured trigger epoch is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			configure:    conwayAt(5),
		})
		requireStaysBabbage(t, ls, err)
	})
	t.Run("major bump to conway transitions", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{pparamsMajor: 9})
		requireBecomesConway(t, ls, err)
	})
	t.Run("configured trigger epoch transitions", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			configure:    conwayAt(1),
		})
		requireBecomesConway(t, ls, err)
	})
	// A header major of 12 elevates the Conway body to Dijkstra, which the
	// major-9 parameters do not authorize. The body era is authorized, so the
	// block is accepted and the ledger stops at Conway.
	t.Run("header elevation past the authorized era stops at it", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor:   9,
			headerMajor:    12,
			enableDijkstra: true,
		})
		requireBecomesConway(t, ls, err)
	})
}

// TestAuthorizedBoundaryEra covers the era gate directly for the transitions
// the pipeline test cannot reach cheaply: two consecutive hard forks at one
// boundary and Byron.
func TestAuthorizedBoundaryEra(t *testing.T) {
	t.Parallel()

	newLedger := func(t *testing.T) *LedgerState {
		t.Helper()
		return &LedgerState{
			config: LedgerStateConfig{
				CardanoNodeConfig: newTestEraHistoryCfg(t),
			},
		}
	}
	pparams := func(major uint) lcommon.ProtocolParameters {
		return boundaryEraBabbagePParams(major)
	}

	t.Run("two consecutive transitions need the successor major", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t)
		era, err := ls.authorizedBoundaryEra(
			eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id,
			eras.BabbageEraDesc.Id, pparams(7), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, era)
	})
	t.Run("an elevation the major does not reach stops at the body era", func(t *testing.T) {
		t.Parallel()
		era, err := newLedger(t).authorizedBoundaryEra(
			eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id,
			eras.BabbageEraDesc.Id, pparams(5), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.AlonzoEraDesc.Id, era)
	})
	t.Run("a single transition needs its own major", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t)
		era, err := ls.authorizedBoundaryEra(
			eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id,
			eras.AlonzoEraDesc.Id, pparams(5), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.AlonzoEraDesc.Id, era)
		_, err = ls.authorizedBoundaryEra(
			eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id,
			eras.AlonzoEraDesc.Id, pparams(4), 10,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
	})
	t.Run("an elevated body the major does not reach is refused", func(t *testing.T) {
		t.Parallel()
		_, err := newLedger(t).authorizedBoundaryEra(
			eras.MaryEraDesc.Id, eras.AlonzoEraDesc.Id,
			eras.BabbageEraDesc.Id, pparams(4), 10,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
	})
	t.Run("an unchanged era is never refused", func(t *testing.T) {
		t.Parallel()
		era, err := newLedger(t).authorizedBoundaryEra(
			eras.BabbageEraDesc.Id, eras.BabbageEraDesc.Id,
			eras.BabbageEraDesc.Id, pparams(8), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, era)
	})
	t.Run("byron has no version to compare", func(t *testing.T) {
		t.Parallel()
		era, err := newLedger(t).authorizedBoundaryEra(
			eras.ByronEraDesc.Id, eras.ShelleyEraDesc.Id,
			eras.ShelleyEraDesc.Id, nil, 10,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.ShelleyEraDesc.Id, era)
	})
}
