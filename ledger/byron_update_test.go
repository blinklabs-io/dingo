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
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/byronupdate"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// newByronUpdateTestLedger returns a ledger whose Byron genesis makes the
// real mainnet main block's PBFT header valid, with k = 10.
func newByronUpdateTestLedger(
	t *testing.T,
	block gledger.Block,
) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newByronPBFTTestNodeConfig(t, block, 10),
		},
	}
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	return ls
}

// withByronUpdatePayload returns block with its update payload replaced by
// payload. The header, and so its PBFT signature, is unchanged.
func withByronUpdatePayload(
	t *testing.T,
	block gledger.Block,
	payload []byte,
) *byron.ByronMainBlock {
	t.Helper()
	mainBlock, ok := block.(*byron.ByronMainBlock)
	require.True(t, ok)
	var blockParts []cbor.RawMessage
	_, err := cbor.Decode(block.Cbor(), &blockParts)
	require.NoError(t, err)
	var bodyParts []cbor.RawMessage
	_, err = cbor.Decode(blockParts[1], &bodyParts)
	require.NoError(t, err)
	require.Len(t, bodyParts, 4)
	body := []byte{0x84}
	for _, part := range bodyParts[:3] {
		body = append(body, part...)
	}
	body = append(body, payload...)
	ret := &byron.ByronMainBlock{BlockHeader: mainBlock.BlockHeader}
	_, err = cbor.Decode(body, &ret.Body)
	require.NoError(t, err)
	return ret
}

// voteForUnregisteredProposal is an update payload with no proposal and one
// well-formed vote for a proposal nobody registered.
func voteForUnregisteredProposal(t *testing.T) []byte {
	t.Helper()
	vote, err := cbor.Encode([]any{
		make([]byte, 64), make([]byte, 32), true, make([]byte, 64),
	})
	require.NoError(t, err)
	payload := []byte{0x82, 0x80, 0x9f}
	payload = append(payload, vote...)
	return append(payload, 0xff)
}

// TestAdvanceByronPBFTStateAppliesUpdatePayload covers #4378 at the block
// level: the update payload of a Byron main block with a valid PBFT header is
// registered through the update rules, and a failing one rejects the block
// when the update state holds the whole chain history.
func TestAdvanceByronPBFTStateAppliesUpdatePayload(t *testing.T) {
	t.Parallel()
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	ls := newByronUpdateTestLedger(t, block)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	genesisParams, err := ls.byronGenesisProtocolParameters()
	require.NoError(t, err)
	newState := func(complete bool) byronPBFTState {
		state, err := newByronPBFTState(config, genesisParams)
		require.NoError(t, err)
		if complete {
			state.update = state.update.Advance(0, 0)
		}
		return state
	}

	// The real block registers its payload.
	next, err := ls.advanceByronPBFTState(newState(true), block, true)
	require.NoError(t, err)
	require.True(t, next.update.Complete())
	require.True(t, next.update.AdoptedParams().Equal(genesisParams))

	invalid := withByronUpdatePayload(t, block, voteForUnregisteredProposal(t))
	_, err = ls.advanceByronPBFTState(newState(true), invalid, true)
	var notRegistered byronupdate.VoteProposalNotRegisteredError
	require.ErrorAs(t, err, &notRegistered)

	// A trusted block, or a state rebuilt from a trusted start, follows the
	// chain instead.
	_, err = ls.advanceByronPBFTState(newState(true), invalid, false)
	require.NoError(t, err)
	_, err = ls.advanceByronPBFTState(newState(false), invalid, true)
	require.NoError(t, err)
}

// TestValidateByronBlockSizesUsesAdoptedLimits covers #4418: a main block is
// measured against the adopted limits, not the genesis ones.
func TestValidateByronBlockSizesUsesAdoptedLimits(t *testing.T) {
	t.Parallel()
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	blockSize := int64(len(block.Cbor()))
	headerSize := int64(len(block.Header().Cbor()))

	// Genesis limits one byte short of the real block reject it.
	nodeConfig := &cardano.CardanoNodeConfig{}
	require.NoError(t, loadByronGenesisForTest(t, nodeConfig, strings.NewReader(
		`{"blockVersionData": {"slotDuration": "20000", `+
			`"maxBlockSize": "`+big.NewInt(blockSize-1).String()+`", `+
			`"maxHeaderSize": "`+big.NewInt(headerSize).String()+`"}, `+
			`"protocolConsts": {"k": 10, "protocolMagic": 764824073}}`,
	)))
	require.ErrorContains(t,
		validateByronBlockSizes(block, nil, nodeConfig),
		"exceeds maxBlockSize",
	)

	adopted, err := eras.NewByronProtocolParametersFromGenesis(
		nodeConfig.ByronGenesis(),
	)
	require.NoError(t, err)
	for _, test := range []struct {
		name          string
		maxBlock      int64
		maxHeader     int64
		wantSubstring string
	}{
		{"raised block limit", blockSize, headerSize, ""},
		{"lowered block limit", blockSize - 1, headerSize, "exceeds maxBlockSize"},
		{"lowered header limit", blockSize, headerSize - 1, "exceeds maxHeaderSize"},
	} {
		params := adopted.Clone()
		params.MaxBlockSize = big.NewInt(test.maxBlock)
		params.MaxHeaderSize = big.NewInt(test.maxHeader)
		err := validateByronBlockSizes(block, params, nodeConfig)
		if test.wantSubstring == "" {
			require.NoError(t, err, test.name)
			continue
		}
		require.ErrorContains(t, err, test.wantSubstring, test.name)
	}
}

// TestByronShelleyTransitionRequiresAdoptedCandidate covers #4420 on the real
// mainnet boundary: with an update state that holds the whole chain history
// and no stable protocol version 2 candidate, the first Shelley block is
// rejected. newByronShelleyBoundaryLedger's own tests cover the trusted start
// that cannot know the candidate and so follows the chain.
func TestByronShelleyTransitionRequiresAdoptedCandidate(t *testing.T) {
	t.Parallel()
	ls, lastByron, firstShelley := newByronShelleyBoundaryLedger(t)
	genesisParams, err := ls.byronGenesisProtocolParameters()
	require.NoError(t, err)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(config, genesisParams)
	require.NoError(t, err)
	state.update = state.update.Advance(0, 0)
	ls.Lock()
	ls.byronPBFT.state = state
	ls.byronPBFT.tip = ls.currentTip.Point
	ls.byronPBFT.initialized = true
	ls.Unlock()

	done := make(chan struct{})
	results := make(chan readChainResult, 1)
	results <- readChainResult{
		blocks: []gledger.Block{firstShelley},
		done:   done,
	}
	close(results)
	err = ls.ledgerProcessBlocksFromSource(context.Background(), results)
	// The rejection is deterministic, so the boundary block is dropped from
	// the primary chain and the pipeline restarts rather than re-reading it.
	require.ErrorIs(t, err, errRestartLedgerPipeline)
	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"the reader must be signalled when the transition is rejected",
	)
	require.Equal(t, eras.ByronEraDesc.Id, ls.currentEra.Id)
	require.Equal(t, lastByron.SlotNumber(), ls.chain.Tip().Point.Slot)
	require.Equal(t, uint64(208), firstShelley.SlotNumber()/21_600)
}

// TestByronShelleyTransitionErrorIdentifiesBoundaryBlock pins what the
// rejection carries when there is nothing to rewind: the transition error,
// wrapped as a rejected header of the boundary block.
func TestByronShelleyTransitionErrorIdentifiesBoundaryBlock(t *testing.T) {
	t.Parallel()
	ls, _, firstShelley := newByronShelleyBoundaryLedger(t)
	genesisParams, err := ls.byronGenesisProtocolParameters()
	require.NoError(t, err)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	state, err := newByronPBFTState(config, genesisParams)
	require.NoError(t, err)
	state.update = state.update.Advance(0, 0)
	ls.Lock()
	ls.byronPBFT.state = state
	ls.byronPBFT.tip = ls.currentTip.Point
	ls.byronPBFT.initialized = true
	// Without a chain manager the recovery declines to rewind.
	ls.config.ChainManager = nil
	ls.Unlock()

	done := make(chan struct{})
	results := make(chan readChainResult, 1)
	results <- readChainResult{
		blocks: []gledger.Block{firstShelley},
		done:   done,
	}
	close(results)
	err = ls.ledgerProcessBlocksFromSource(context.Background(), results)
	var notAdopted byronupdate.TransitionNotAdoptedError
	require.ErrorAs(t, err, &notAdopted)
	require.Equal(t, uint64(208), notAdopted.Epoch)
	var rejected *headerValidationError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, firstShelley.SlotNumber(), rejected.BlockPoint.Slot)
	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"the reader must be signalled when the transition is rejected",
	)
}

func byronTestEbbHeader(
	t *testing.T,
	epoch uint64,
	genesisTag bool,
) *byron.ByronEpochBoundaryBlock {
	t.Helper()
	attrs := map[uint64]any{}
	if genesisTag {
		attrs[255] = []byte("Genesis")
	}
	raw, err := cbor.Encode([]any{
		uint64(764824073),
		make([]byte, 32),
		make([]byte, 32),
		[]any{epoch, []any{uint64(1)}},
		[]any{attrs},
	})
	require.NoError(t, err)
	var header byron.ByronEpochBoundaryBlockHeader
	_, err = cbor.Decode(raw, &header)
	require.NoError(t, err)
	require.Equal(t, genesisTag, header.HasGenesisTag())
	return &byron.ByronEpochBoundaryBlock{BlockHeader: &header}
}

// TestValidateByronEbbPreviousHashSemantics covers #4400: an EBB whose
// previous hash is a genesis hash, in epoch 0 or through the 255 => "Genesis"
// attribute, cannot continue a chain that already has a block, whatever its
// bytes.
func TestValidateByronEbbPreviousHashSemantics(t *testing.T) {
	t.Parallel()
	ls, _, _ := newByronShelleyBoundaryLedger(t)
	for _, test := range []struct {
		name       string
		epoch      uint64
		genesisTag bool
		rejected   bool
	}{
		{"epoch 0", 0, false, true},
		{"later epoch with the Genesis tag", 207, true, true},
		{"later epoch without the tag", 207, false, false},
	} {
		err := ls.validateByronPBFTHeaderCrypto(
			byronTestEbbHeader(t, test.epoch, test.genesisTag),
		)
		if test.rejected {
			require.ErrorContains(t, err, "genesis hash", test.name)
			continue
		}
		if err != nil {
			require.NotContains(t, err.Error(), "genesis hash", test.name)
		}
	}
}

// TestByronTxAuxRejectsTrailingFields covers #4383: a TxAux must be exactly
// [body, witnesses], so a third field fails decoding wherever a Byron
// transaction or block body is decoded.
func TestByronTxAuxRejectsTrailingFields(t *testing.T) {
	t.Parallel()
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	txs := block.Transactions()
	require.NotEmpty(t, txs)
	var txParts []cbor.RawMessage
	_, err = cbor.Decode(txs[0].Cbor(), &txParts)
	require.NoError(t, err)
	require.Len(t, txParts, 2)
	_, err = byron.NewByronTransactionFromCbor(txs[0].Cbor())
	require.NoError(t, err)

	extended, err := cbor.Encode([]any{txParts[0], txParts[1], uint64(0)})
	require.NoError(t, err)
	_, err = byron.NewByronTransactionFromCbor(extended)
	require.Error(t, err)

	var blockParts []cbor.RawMessage
	_, err = cbor.Decode(block.Cbor(), &blockParts)
	require.NoError(t, err)
	var bodyParts []cbor.RawMessage
	_, err = cbor.Decode(blockParts[1], &bodyParts)
	require.NoError(t, err)
	body := []byte{0x84, 0x9f}
	body = append(body, extended...)
	body = append(body, 0xff)
	for _, part := range bodyParts[1:] {
		body = append(body, part...)
	}
	var decoded byron.ByronMainBlockBody
	_, err = cbor.Decode(body, &decoded)
	require.Error(t, err)
}

// TestByronBlockPParams covers the block-application half of #4418 and
// #4419: a Byron block is validated against its update state's adopted
// parameters, and any other block against the era's own.
func TestByronBlockPParams(t *testing.T) {
	t.Parallel()
	stored := loadRealByronMainBlock(t)
	block, err := stored.Decode()
	require.NoError(t, err)
	ls := newByronUpdateTestLedger(t, block)
	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	genesisParams, err := ls.byronGenesisProtocolParameters()
	require.NoError(t, err)
	state, err := newByronPBFTState(config, genesisParams)
	require.NoError(t, err)
	shelleyParams := &shelley.ShelleyProtocolParameters{}

	adopted, ok := byronBlockPParams(block, state, shelleyParams).(*eras.ByronProtocolParameters)
	require.True(t, ok)
	require.True(t, adopted.Equal(genesisParams))

	untracked, err := newByronPBFTState(config, nil)
	require.NoError(t, err)
	require.Same(
		t,
		shelleyParams,
		byronBlockPParams(block, untracked, shelleyParams),
	)
	_, _, firstShelley := newByronShelleyBoundaryLedger(t)
	require.Same(
		t,
		shelleyParams,
		byronBlockPParams(firstShelley, state, shelleyParams),
	)
}
