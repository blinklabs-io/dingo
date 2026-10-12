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
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
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

// boundaryEraTestSecurityParam is the security parameter of the test
// genesis: the number of blocks a version-triggered transition needs after
// the voting deadline.
const boundaryEraTestSecurityParam = 432

// boundaryEraBlock builds an empty block whose header advertises headerMajor.
// A Babbage body is encoded as Babbage, any other as Conway.
func boundaryEraBlock(
	t *testing.T,
	babbageBody bool,
	headerMajor uint,
	blockNumber, slot uint64,
	prevHash lcommon.Blake2b256,
) gledger.Block {
	t.Helper()
	if babbageBody {
		block := &babbage.BabbageBlock{
			BlockHeader:            &babbage.BabbageBlockHeader{},
			TransactionBodies:      []babbage.BabbageTransactionBody{},
			TransactionWitnessSets: []babbage.BabbageTransactionWitnessSet{},
			TransactionMetadataSet: lcommon.TransactionMetadataSet{},
			InvalidTransactions:    []uint{},
		}
		block.BlockHeader.Body.BlockNumber = blockNumber
		block.BlockHeader.Body.Slot = slot
		block.BlockHeader.Body.PrevHash = prevHash
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
	block := &conway.ConwayBlock{
		BlockHeader:            &conway.ConwayBlockHeader{},
		TransactionBodies:      []conway.ConwayTransactionBody{},
		TransactionWitnessSets: []conway.ConwayTransactionWitnessSet{},
		TransactionMetadataSet: lcommon.TransactionMetadataSet{},
		InvalidTransactions:    []uint{},
	}
	block.BlockHeader.Body.BlockNumber = blockNumber
	block.BlockHeader.Body.Slot = slot
	block.BlockHeader.Body.PrevHash = prevHash
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

func rawBoundaryEraBlock(block gledger.Block, blockType uint) chain.RawBlock {
	return chain.RawBlock{
		Slot:        block.SlotNumber(),
		Hash:        block.Hash().Bytes(),
		BlockNumber: block.BlockNumber(),
		Type:        blockType,
		PrevHash:    block.PrevHash().Bytes(),
		Cbor:        block.Cbor(),
	}
}

// boundaryEraCase describes a Babbage ledger at the end of epoch 0 and the
// Conway-encoded block that opens epoch 1.
type boundaryEraCase struct {
	// pparamsMajor is the protocol major of the epoch 0 parameters.
	pparamsMajor uint
	// headerMajor is the protocol major the boundary block header carries;
	// zero means Conway's.
	headerMajor uint
	// babbageBody encodes the boundary block as Babbage instead of Conway.
	babbageBody bool
	// conwaySource starts the ledger in Conway with Conway parameters whose
	// major is pparamsMajor, with Dijkstra enabled. The boundary block is
	// Dijkstra when dijkstraBody is set and Conway otherwise. It needs
	// votingBlocks of at least one.
	conwaySource bool
	dijkstraBody bool
	// trusted replays the block without validation, as an unvalidated
	// history import does.
	trusted bool
	// seed may add governance state, such as a ratified HardForkInitiation.
	seed func(t *testing.T, db *database.Database)
	// votingBlocks is the number of epoch 0 blocks at or past the voting
	// deadline: the epoch is shorter than twice the stability window, so
	// that is every block of the epoch.
	votingBlocks uint64
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
	ls, block := newBoundaryEraLedger(t, tc)
	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{block}}
	close(results)
	return ls, ls.ledgerProcessBlocksFromSource(context.Background(), results)
}

// newBoundaryEraLedger builds the ledger at the end of epoch 0 and the block
// that opens epoch 1, which is already on the primary chain.
func newBoundaryEraLedger(
	t *testing.T,
	tc boundaryEraCase,
) (*LedgerState, gledger.Block) {
	t.Helper()
	headerMajor := tc.headerMajor
	if headerMajor == 0 {
		headerMajor = conway.MinProtocolVersionConway
	}
	blockType := uint(gledger.BlockTypeConway)
	if tc.babbageBody {
		blockType = uint(gledger.BlockTypeBabbage)
	}
	tipType := uint(gledger.BlockTypeBabbage)
	// The last block of epoch 0 is what the ledger has applied when the
	// boundary block arrives. It is on the chain so a refused boundary block
	// has a point to rewind to.
	var tipBlock, block gledger.Block
	prevHash := lcommon.Blake2b256{}
	switch {
	case tc.conwaySource:
		require.NotZero(t, tc.votingBlocks)
		tipType = uint(gledger.BlockTypeConway)
		var blocks []gledger.Block
		var err error
		if tc.dijkstraBody {
			blockType = uint(gledger.BlockTypeDijkstra)
			blocks, err = fixtures.GenerateConwayToDijkstraChain(
				tc.votingBlocks, prevHash,
				boundaryEraTestEpochLength-1, 2, 1, 1,
			)
		} else {
			blocks, err = fixtures.GenerateConwayChain(
				tc.votingBlocks, prevHash,
				boundaryEraTestEpochLength-1, 2, 2,
			)
		}
		require.NoError(t, err)
		tipBlock, block = blocks[0], blocks[1]
	case tc.votingBlocks > 0:
		tipBlocks, err := fixtures.GenerateBabbageChainWithProtocolVersion(
			tc.votingBlocks, prevHash, boundaryEraTestEpochLength-1, 1,
			uint64(tc.pparamsMajor), 0, 1,
		)
		require.NoError(t, err)
		tipBlock = tipBlocks[0]
		prevHash = tipBlock.Hash()
		fallthrough
	default:
		block = boundaryEraBlock(
			t, tc.babbageBody, headerMajor,
			tc.votingBlocks+1, boundaryEraTestEpochLength+1, prevHash,
		)
	}

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	var raw []chain.RawBlock
	if tipBlock != nil {
		raw = append(raw, rawBoundaryEraBlock(tipBlock, tipType))
	}
	raw = append(raw, rawBoundaryEraBlock(block, blockType))
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), raw))

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

	sourceEra := eras.BabbageEraDesc
	var pparams lcommon.ProtocolParameters = boundaryEraBabbagePParams(
		tc.pparamsMajor,
	)
	if tc.conwaySource {
		sourceEra = eras.ConwayEraDesc
		conwayPParams := donationTestConwayPParams(tc.pparamsMajor)
		conwayPParams.MaxBlockHeaderSize = 1_100
		conwayPParams.MaxBlockBodySize = 90_112
		conwayPParams.MaxTxSize = 16_384
		pparams = conwayPParams
	}
	nonce := bytes.Repeat([]byte{0x42}, 32)
	epoch0 := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    1_000,
		LengthInSlots: boundaryEraTestEpochLength,
		EraId:         sourceEra.Id,
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
		ValidateHistorical:    !tc.trusted,
		TrustedReplay:         tc.trusted,
		EnableDijkstra:        tc.enableDijkstra || tc.conwaySource,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.currentEra = sourceEra
	ls.currentPParams = pparams
	ls.currentEpoch = epoch0
	ls.epochCache = []models.Epoch{epoch0}
	ls.currentTip = ochainsync.Tip{}
	if tipBlock != nil {
		ls.currentTip = ochainsync.Tip{
			Point: ocommon.NewPoint(
				tipBlock.SlotNumber(), tipBlock.Hash().Bytes(),
			),
			BlockNumber: tipBlock.BlockNumber(),
		}
	}
	ls.currentTipBlockNonce = nonce
	ls.publishSnapshotsLocked()
	require.NoError(t, cm.SetLedger(ls))
	seedEmptyRewardBasisForRollover(t, db, epoch0, pparams)
	if tc.seed != nil {
		tc.seed(t, db)
	}
	return ls, block
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
	requireStaysBabbage := func(
		t *testing.T,
		ls *LedgerState,
		err error,
		major uint,
	) {
		t.Helper()
		require.Error(t, err, "a boundary block the ledger did not authorize must be refused")
		require.ErrorIs(t, err, errRestartLedgerPipeline,
			"the refused block must be rewound, not re-read forever")
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		ver, verErr := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, verErr)
		assert.Equal(t, major, ver.Major)
		assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
		assert.Equal(t, uint64(0), ls.currentEpoch.StartSlot)
		assert.Equal(t, ls.currentTip.Point.Slot, ls.Tip().Point.Slot)
		assert.Equal(t, ls.currentTip.Point.Slot,
			ls.config.ChainManager.PrimaryChain().Tip().Point.Slot,
			"the refused block must leave the primary chain")
	}
	requireBecomes := func(
		t *testing.T,
		ls *LedgerState,
		err error,
		era uint,
		major uint,
	) {
		t.Helper()
		require.NoError(t, err)
		assert.Equal(t, era, ls.currentEra.Id)
		ver, verErr := GetProtocolVersion(ls.currentPParams)
		require.NoError(t, verErr)
		assert.Equal(t, major, ver.Major)
		assert.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	}
	requireBecomesConway := func(t *testing.T, ls *LedgerState, err error) {
		t.Helper()
		requireBecomes(t, ls, err, eras.ConwayEraDesc.Id, 9)
	}
	k := uint64(boundaryEraTestSecurityParam)

	t.Run("conway block without a ratified major bump is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{pparamsMajor: 8})
		requireStaysBabbage(t, ls, err, 8)
	})
	t.Run("conway block before the configured trigger epoch is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			configure:    conwayAt(5),
		})
		requireStaysBabbage(t, ls, err, 8)
	})
	t.Run("major bump to conway transitions", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 9,
			votingBlocks: k,
		})
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
			votingBlocks:   k,
		})
		requireBecomesConway(t, ls, err)
	})
	// The ledger enters Conway at this boundary, so a block still encoded in
	// Babbage names an era the ledger has left.
	t.Run("babbage block after the major reached conway is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 9,
			babbageBody:  true,
			votingBlocks: k,
		})
		requireStaysBabbage(t, ls, err, 9)
	})
	t.Run("babbage block at the configured trigger epoch is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			babbageBody:  true,
			configure:    conwayAt(1),
		})
		requireStaysBabbage(t, ls, err, 8)
	})
	// Fewer than k blocks after the voting deadline leave the transition
	// unknown to consensus: the era does not end at this boundary, so the
	// Conway block is the wrong one and the Babbage block the right one.
	t.Run("conway block with fewer than k blocks after the deadline is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 9,
			votingBlocks: k - 1,
		})
		requireStaysBabbage(t, ls, err, 9)
	})
	t.Run("babbage block with fewer than k blocks after the deadline is accepted", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 9,
			babbageBody:  true,
			votingBlocks: k - 1,
		})
		requireBecomes(t, ls, err, eras.BabbageEraDesc.Id, 9)
	})
	t.Run("babbage block without a major bump is accepted", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			babbageBody:  true,
			votingBlocks: k,
		})
		requireBecomes(t, ls, err, eras.BabbageEraDesc.Id, 8)
	})
}

// TestTrustedReplayBoundaryEraFollowsBlock pins that a boundary the node does
// not validate keeps following the block that opens the epoch: the era
// decision runs only where the block is validated.
func TestTrustedReplayBoundaryEraFollowsBlock(t *testing.T) {
	t.Parallel()

	k := uint64(boundaryEraTestSecurityParam)
	t.Run("a conway block moves the ledger without a major bump", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 8,
			trusted:      true,
		})
		require.NoError(t, err)
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	})
	t.Run("a babbage block keeps the ledger after the major bump", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, boundaryEraCase{
			pparamsMajor: 9,
			babbageBody:  true,
			votingBlocks: k,
			trusted:      true,
		})
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	})
}

// TestRefusedBoundaryReleasesValidationTransition pins that refusing a
// boundary block ends the validation transition the rollover opened. A
// transition left open makes every later transaction validation session
// report itself stale, so the mempool could never commit a transaction.
func TestRefusedBoundaryReleasesValidationTransition(t *testing.T) {
	t.Parallel()
	ls, err := runBoundaryEraCase(t, boundaryEraCase{pparamsMajor: 8})
	require.ErrorIs(t, err, errRestartLedgerPipeline)
	assert.Equal(t, int64(0), ls.txValidationTransitions.Load())

	var current bool
	require.NoError(t, ls.WithTxValidationSession(
		context.Background(),
		func(
			_ func(
				gledger.Transaction,
				map[utxoref.Key]struct{},
				map[utxoref.Key]lcommon.Utxo,
				*utxoref.StateOverlay,
			) error,
			stillCurrent func() bool,
			_ func(func() error) (bool, error),
		) error {
			current = stillCurrent()
			return nil
		},
	))
	assert.True(t, current,
		"a validation session after a refused boundary must stay current")
}

// TestAuthorizeBoundaryEra covers the era decision directly for the
// transitions the pipeline test cannot reach cheaply: two consecutive hard
// forks at one boundary, the lagging direction, a Conway source whose
// HardForkInitiation is enacted at the boundary, and Byron.
func TestAuthorizeBoundaryEra(t *testing.T) {
	t.Parallel()

	k := uint64(boundaryEraTestSecurityParam)
	const boundaryEpoch = 10
	// newLedger returns a ledger with a database and an empty primary chain,
	// so that the voting-deadline block count reads the tip's block number.
	newLedger := func(t *testing.T, dijkstraAt *uint64) *LedgerState {
		t.Helper()
		cfg := newTestEraHistoryCfg(t)
		enabled := true
		cfg.ExperimentalHardForksEnabled = &enabled
		cfg.TestDijkstraHardForkAtEpoch = dijkstraAt
		db := newTestDB(t)
		cm, err := chain.NewManager(context.Background(), db, nil)
		require.NoError(t, err)
		return &LedgerState{
			db:         db,
			chain:      cm.PrimaryChain(),
			activeEras: eras.ErasWithDijkstra,
			config: LedgerStateConfig{
				CardanoNodeConfig: cfg,
				EnableDijkstra:    true,
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
			},
		}
	}
	// decide runs the decision for the boundary into epoch 10 with voting
	// blocks at or past the deadline of the epoch that ends.
	decide := func(
		ls *LedgerState,
		source eras.EraDesc,
		pparams lcommon.ProtocolParameters,
		voting uint64,
		body, target uint,
	) (uint, error) {
		epoch := models.Epoch{
			EpochId:       boundaryEpoch - 1,
			StartSlot:     0,
			LengthInSlots: boundaryEraTestEpochLength,
			EraId:         source.Id,
		}
		return ls.authorizeBoundaryEra(
			context.Background(),
			boundaryEraInputs{
				sourceEra: source,
				epoch:     epoch,
				tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(boundaryEraTestEpochLength-1, nil),
					BlockNumber: voting,
				},
				pparams:    pparams,
				newEpochID: boundaryEpoch,
			},
			body, target,
		)
	}
	babbagePParams := func(major uint) lcommon.ProtocolParameters {
		return boundaryEraBabbagePParams(major)
	}

	t.Run("two consecutive transitions need the successor major", func(t *testing.T) {
		t.Parallel()
		era, err := decide(
			newLedger(t, nil), eras.MaryEraDesc, babbagePParams(7), k,
			eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, era)
	})
	t.Run("an elevation the major does not reach stops at the body era", func(t *testing.T) {
		t.Parallel()
		era, err := decide(
			newLedger(t, nil), eras.MaryEraDesc, babbagePParams(5), k,
			eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.AlonzoEraDesc.Id, era)
	})
	t.Run("a single transition needs its own major", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t, nil)
		era, err := decide(
			ls, eras.MaryEraDesc, babbagePParams(5), k,
			eras.AlonzoEraDesc.Id, eras.AlonzoEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.AlonzoEraDesc.Id, era)
		_, err = decide(
			ls, eras.MaryEraDesc, babbagePParams(4), k,
			eras.AlonzoEraDesc.Id, eras.AlonzoEraDesc.Id,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
	})
	t.Run("an elevated body the major does not reach is refused", func(t *testing.T) {
		t.Parallel()
		_, err := decide(
			newLedger(t, nil), eras.MaryEraDesc, babbagePParams(4), k,
			eras.AlonzoEraDesc.Id, eras.BabbageEraDesc.Id,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
	})
	t.Run("an unchanged era is never refused", func(t *testing.T) {
		t.Parallel()
		era, err := decide(
			newLedger(t, nil), eras.BabbageEraDesc, babbagePParams(8), 0,
			eras.BabbageEraDesc.Id, eras.BabbageEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.BabbageEraDesc.Id, era)
	})
	t.Run("a source-era block after the ledger left the era is refused", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t, nil)
		_, err := decide(
			ls, eras.MaryEraDesc, babbagePParams(5), k,
			eras.MaryEraDesc.Id, eras.MaryEraDesc.Id,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
		era, err := decide(
			ls, eras.MaryEraDesc, babbagePParams(5), k-1,
			eras.MaryEraDesc.Id, eras.MaryEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.MaryEraDesc.Id, era)
	})
	// Majors 6 and 8 are intra-era bumps: they never name the next era.
	t.Run("an intra-era major bump keeps the source era", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t, nil)
		for _, tc := range []struct {
			source eras.EraDesc
			major  uint
			next   eras.EraDesc
		}{
			{eras.AlonzoEraDesc, 6, eras.BabbageEraDesc},
			{eras.BabbageEraDesc, 8, eras.ConwayEraDesc},
		} {
			era, err := decide(
				ls, tc.source, babbagePParams(tc.major), k,
				tc.source.Id, tc.source.Id,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.source.Id, era)
			_, err = decide(
				ls, tc.source, babbagePParams(tc.major), k,
				tc.next.Id, tc.next.Id,
			)
			require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
		}
	})
	t.Run("byron has no version to compare", func(t *testing.T) {
		t.Parallel()
		era, err := decide(
			newLedger(t, nil), eras.ByronEraDesc, nil, 0,
			eras.ShelleyEraDesc.Id, eras.ShelleyEraDesc.Id,
		)
		require.NoError(t, err)
		assert.Equal(t, eras.ShelleyEraDesc.Id, era)
	})

	conwayPParams := func(major uint) lcommon.ProtocolParameters {
		return &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: major,
			},
		}
	}
	conwayLedger := func(t *testing.T, initiation uint) *LedgerState {
		t.Helper()
		ls := newLedger(t, nil)
		if initiation != 0 {
			seedRatifiedHardForkInitiation(initiation)(t, ls.db)
		}
		return ls
	}
	dijkstra, conwayID := eras.DijkstraEraDesc.Id, eras.ConwayEraDesc.Id
	t.Run("a conway source follows its enacted initiation", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name       string
			major      uint
			initiation uint
			voting     uint64
			body       uint
			want       uint
			refused    bool
		}{
			{"next era accepted", 11, 12, k, dijkstra, dijkstra, false},
			{"conway refused", 11, 12, k, conwayID, 0, true},
			{"no initiation keeps conway", 11, 0, k, conwayID, conwayID, false},
			{"no initiation refuses dijkstra", 11, 0, k, dijkstra, 0, true},
			{"intra-era initiation keeps conway", 10, 11, k, conwayID, conwayID, false},
			{"intra-era initiation refuses dijkstra", 10, 11, k, dijkstra, 0, true},
			{"too few voting blocks keep conway", 11, 12, k - 1, conwayID, conwayID, false},
			{"too few voting blocks refuse dijkstra", 11, 12, k - 1, dijkstra, 0, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				era, err := decide(
					conwayLedger(t, tc.initiation), eras.ConwayEraDesc,
					conwayPParams(tc.major), tc.voting, tc.body, tc.body,
				)
				if tc.refused {
					require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tc.want, era)
			})
		}
	})
	t.Run("an initiation ratified but not yet written still counts", func(t *testing.T) {
		t.Parallel()
		ls := newLedger(t, nil)
		pending := &models.GovernanceProposal{
			ActionType: uint8(lcommon.GovActionTypeHardForkInitiation),
		}
		action := &lcommon.HardForkInitiationGovAction{Type: 1}
		action.ProtocolVersion.Major = 12
		var err error
		pending.GovActionCbor, err = cbor.Encode(action)
		require.NoError(t, err)
		pending.ReturnAddress = make([]byte, 29)
		rec := pendingRatificationRecord{Epoch: boundaryEpoch - 1, BoundarySlot: 1, ID: 1}
		raw, err := json.Marshal(rec)
		require.NoError(t, err)
		require.NoError(t, ls.db.SetSyncState(pendingRatificationSyncKey, string(raw), nil))
		job := &ratificationJob{
			record:   rec,
			decided:  make(chan struct{}),
			settled:  make(chan struct{}),
			decision: &governance.RatificationDecision{Ratified: []*models.GovernanceProposal{pending}},
		}
		close(job.decided)
		ls.ratificationJob = job
		era, err := decide(
			ls, eras.ConwayEraDesc, conwayPParams(11), k, dijkstra, dijkstra,
		)
		require.NoError(t, err)
		assert.Equal(t, dijkstra, era)
	})
	// A configured trigger epoch decides from the epoch alone.
	t.Run("a conway source is held to its configured trigger epoch", func(t *testing.T) {
		t.Parallel()
		at := uint64(20)
		_, err := decide(
			newLedger(t, &at), eras.ConwayEraDesc, conwayPParams(11), k,
			dijkstra, dijkstra,
		)
		require.ErrorIs(t, err, errBoundaryEraNotAuthorized)
		at = boundaryEpoch
		era, err := decide(
			newLedger(t, &at), eras.ConwayEraDesc, conwayPParams(11), k,
			dijkstra, dijkstra,
		)
		require.NoError(t, err)
		assert.Equal(t, dijkstra, era)
	})
}

// seedRatifiedHardForkInitiation records a HardForkInitiation to major that
// an earlier boundary ratified and this one enacts.
func seedRatifiedHardForkInitiation(
	major uint,
) func(*testing.T, *database.Database) {
	return func(t *testing.T, db *database.Database) {
		t.Helper()
		action := &lcommon.HardForkInitiationGovAction{Type: 1}
		action.ProtocolVersion.Major = major
		actionCbor, err := cbor.Encode(action)
		require.NoError(t, err)
		ratifiedEpoch, ratifiedSlot := uint64(0), uint64(500)
		require.NoError(t, db.SetGovernanceProposal(
			context.Background(),
			&models.GovernanceProposal{
				TxHash:        bytes.Repeat([]byte{0x49}, 32),
				ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
				ProposedEpoch: 0,
				ExpiresEpoch:  30,
				ReturnAddress: make([]byte, 29),
				AnchorURL:     "https://example.invalid/hfi",
				AnchorHash:    bytes.Repeat([]byte{0x4a}, 32),
				GovActionCbor: actionCbor,
				RatifiedEpoch: &ratifiedEpoch,
				RatifiedSlot:  &ratifiedSlot,
				AddedSlot:     1,
			},
			nil,
		))
	}
}

// TestBoundaryBlockEraFollowsEnactedHardForkInitiation pins that from Conway
// the boundary era is the one the enacted HardForkInitiation names, decided
// before translation, and that a block in any other era is refused.
func TestBoundaryBlockEraFollowsEnactedHardForkInitiation(t *testing.T) {
	t.Parallel()

	k := uint64(boundaryEraTestSecurityParam)
	requireStaysConway := func(t *testing.T, ls *LedgerState, err error) {
		t.Helper()
		require.ErrorIs(t, err, errRestartLedgerPipeline,
			"the refused block must be rewound")
		assert.Equal(t, eras.ConwayEraDesc.Id, ls.currentEra.Id)
		assert.Equal(t, uint64(0), ls.currentEpoch.EpochId)
	}
	requireEra := func(t *testing.T, ls *LedgerState, err error, era uint) {
		t.Helper()
		require.NoError(t, err)
		assert.Equal(t, era, ls.currentEra.Id)
		assert.Equal(t, uint64(1), ls.currentEpoch.EpochId)
	}
	newCase := func(major uint, enact uint, dijkstra bool, voting uint64) boundaryEraCase {
		tc := boundaryEraCase{
			conwaySource: true,
			pparamsMajor: major,
			dijkstraBody: dijkstra,
			votingBlocks: voting,
		}
		if enact != 0 {
			tc.seed = seedRatifiedHardForkInitiation(enact)
		}
		return tc
	}

	t.Run("dijkstra block after an enacted initiation to its major is accepted", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(11, 12, true, k))
		requireEra(t, ls, err, eras.DijkstraEraDesc.Id)
	})
	t.Run("conway block after an enacted initiation to the next era is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(11, 12, false, k))
		requireStaysConway(t, ls, err)
	})
	t.Run("dijkstra block without an initiation is rejected", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(11, 0, true, k))
		requireStaysConway(t, ls, err)
	})
	t.Run("conway block without an initiation is accepted", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(11, 0, false, k))
		requireEra(t, ls, err, eras.ConwayEraDesc.Id)
	})
	t.Run("an intra-era initiation keeps the ledger in conway", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(10, 11, true, k))
		requireStaysConway(t, ls, err)
	})
	t.Run("fewer than k blocks after the deadline keep the ledger in conway", func(t *testing.T) {
		t.Parallel()
		ls, err := runBoundaryEraCase(t, newCase(11, 12, true, k-1))
		requireStaysConway(t, ls, err)
	})
}
