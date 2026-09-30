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
	"crypto/ed25519"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// byronParamUpdate is the protocol parameters a test proposal changes; a nil
// field is left unchanged.
type byronParamUpdate struct {
	maxBlockSize, maxHeaderSize *uint64
	// feeSummandNano and feeMultiplierNano are set together.
	feeSummandNano, feeMultiplierNano *uint64
}

// newByronParamUpdateProposal returns a signed proposal for version that
// changes the parameters in update, and the proposal's id.
func newByronParamUpdateProposal(
	t *testing.T,
	protocolMagic uint32,
	proposer byronPBFTTestKey,
	version [3]uint64,
	update byronParamUpdate,
) ([]byte, []byte) {
	t.Helper()
	optional := func(value *uint64) []any {
		if value == nil {
			return []any{}
		}
		return []any{*value}
	}
	feePolicy := []any{}
	if update.feeSummandNano != nil {
		inner, err := cbor.Encode([]any{
			*update.feeSummandNano, *update.feeMultiplierNano,
		})
		require.NoError(t, err)
		feePolicy = []any{[]any{uint8(0), cbor.WrappedCbor(inner)}}
	}
	fields := make([]cbor.RawMessage, 0, 5)
	for _, value := range []any{
		[]any{uint16(version[0]), uint16(version[1]), uint8(version[2])},
		[]any{
			[]any{}, []any{}, optional(update.maxBlockSize),
			optional(update.maxHeaderSize), []any{}, []any{}, []any{},
			[]any{}, []any{}, []any{}, []any{}, []any{}, feePolicy, []any{},
		},
		[]any{"dingo-test", uint32(1)},
		map[string]any{},
		map[uint64]any{},
	} {
		encoded, err := cbor.Encode(value)
		require.NoError(t, err)
		fields = append(fields, encoded)
	}
	signedBody := []byte{0x85}
	for _, field := range fields {
		signedBody = append(signedBody, field...)
	}
	signature := ed25519.Sign(
		proposer.privateKey,
		byronUpdateSigned(
			t,
			byron.SignTagUSProposal,
			protocolMagic,
			signedBody,
		),
	)
	raw, err := cbor.Encode([]any{
		fields[0], fields[1], fields[2], fields[3], fields[4],
		proposer.verificationKey, signature,
	})
	require.NoError(t, err)
	var proposal byron.ByronUpdateProposal
	_, err = cbor.Decode(raw, &proposal)
	require.NoError(t, err)
	return proposal.Cbor(), lcommon.Blake2b256Hash(proposal.Cbor()).Bytes()
}

// byronAdoptionChain is a Byron chain, k = 10 so an epoch is 100 slots, that
// adopts two updates in turn: update A at the first block of epoch 1 and
// update B at the first block of epoch 2. Each changes the block and header
// limits and the fee policy from what came before.
type byronAdoptionChain struct {
	magic    uint32
	issuer   byronPBFTTestKey
	delegate byronPBFTTestKey
	cert     []any
	blocks   []*byron.ByronMainBlock
	raw      []chain.RawBlock
	// probe is an empty main block; probeBlock and probeHeader are its sizes.
	probe       *byron.ByronMainBlock
	probeBlock  uint64
	probeHeader uint64
	genesis     wantByronParams
	adoptedA    wantByronParams
	adoptedB    wantByronParams
}

// Indexes into byronAdoptionChain.blocks.
const (
	blockGenesisOnlyLast = 3 // last block before update A is adopted
	blockFirstUnderA     = 4 // first block of epoch 1
	blockLastUnderA      = 7 // last block before update B is adopted
	blockFirstUnderB     = 8 // first block of epoch 2
	blockTip             = 9
)

// wantByronParams are the adopted parameters a test asserts.
type wantByronParams struct {
	maxBlockSize, maxHeaderSize uint64
	feeSummand                  uint64
	feeMultiplierNano           int64
}

func newByronAdoptionChain(t *testing.T) *byronAdoptionChain {
	t.Helper()
	c := &byronAdoptionChain{
		magic:    uint32(44),
		issuer:   newByronPBFTTestKey(0x91),
		delegate: newByronPBFTTestKey(0x92),
	}
	c.cert = newSignedByronPBFTDelegationCertificate(
		t, c.magic, 0, c.issuer, c.delegate,
	)
	template := loadRealByronMainBlock(t)
	newBlock := func(
		epoch, slot, number uint64,
		prev lcommon.Blake2b256,
		payload []byte,
		version byron.ByronBlockVersion,
	) *byron.ByronMainBlock {
		return newSignedByronPBFTBlockWithBody(
			t, template, c.magic, epoch, slot, number, prev,
			c.issuer, c.delegate, c.cert, nil,
			&byronPBFTBodyOverride{
				emptyTransactions: true,
				updatePayload:     payload,
				blockVersion:      &version,
			},
		)
	}
	c.probe = newBlock(
		2, 10, 99, lcommon.Blake2b256{}, byronUpdatePayload(nil),
		byron.ByronBlockVersion{Minor: 2},
	)
	c.probeBlock = uint64(len(c.probe.Cbor()))
	c.probeHeader = uint64(len(c.probe.Header().Cbor()))

	c.genesis = wantByronParams{
		maxBlockSize: 2_000_000, maxHeaderSize: 2_000_000,
		feeSummand: 155_381, feeMultiplierNano: 43_946_000_000,
	}
	c.adoptedA = wantByronParams{
		maxBlockSize: c.probeBlock, maxHeaderSize: c.probeHeader,
		feeSummand: 200_000, feeMultiplierNano: 100_000_000_000,
	}
	c.adoptedB = wantByronParams{
		maxBlockSize: 2 * c.probeBlock, maxHeaderSize: c.probeHeader - 1,
		feeSummand: 120_000, feeMultiplierNano: 20_000_000_000,
	}
	update := func(want wantByronParams) byronParamUpdate {
		summand := want.feeSummand * 1_000_000_000
		multiplier := uint64(want.feeMultiplierNano)
		return byronParamUpdate{
			maxBlockSize:      &want.maxBlockSize,
			maxHeaderSize:     &want.maxHeaderSize,
			feeSummandNano:    &summand,
			feeMultiplierNano: &multiplier,
		}
	}
	proposalA, idA := newByronParamUpdateProposal(
		t, c.magic, c.delegate, [3]uint64{0, 1, 0}, update(c.adoptedA),
	)
	proposalB, idB := newByronParamUpdateProposal(
		t, c.magic, c.delegate, [3]uint64{0, 2, 0}, update(c.adoptedB),
	)
	voteA := newByronUpdateVote(t, c.magic, c.delegate, idA, false)
	voteB := newByronUpdateVote(t, c.magic, c.delegate, idB, false)
	v0 := byron.ByronBlockVersion{}
	vA := byron.ByronBlockVersion{Minor: 1}
	vB := byron.ByronBlockVersion{Minor: 2}
	for i, spec := range []struct {
		epoch, slot uint64
		payload     []byte
		version     byron.ByronBlockVersion
	}{
		{0, 0, byronUpdatePayload(nil), v0},
		{0, 6, byronUpdatePayload(proposalA), v0},
		{0, 7, byronUpdatePayload(nil, voteA), v0},
		{0, 30, byronUpdatePayload(nil), vA},
		{1, 0, byronUpdatePayload(nil), vA},
		{1, 6, byronUpdatePayload(proposalB), vA},
		{1, 7, byronUpdatePayload(nil, voteB), vA},
		{1, 30, byronUpdatePayload(nil), vB},
		{2, 0, byronUpdatePayload(nil), vB},
		{2, 10, byronUpdatePayload(nil), vB},
	} {
		var prev lcommon.Blake2b256
		if i > 0 {
			prev = c.blocks[i-1].Hash()
		}
		c.blocks = append(c.blocks, newBlock(
			spec.epoch, spec.slot, uint64(i), prev, spec.payload, spec.version,
		))
		c.raw = append(c.raw, rawByronPBFTBlock(t, c.blocks[i]))
	}
	return c
}

// nodeConfig returns a Byron genesis whose limits and fee policy are the
// chain's genesis ones, except for the block and header limits, which are
// blockLimit when it is not zero.
func (c *byronAdoptionChain) nodeConfig(
	t *testing.T,
	blockLimit int,
) *cardano.CardanoNodeConfig {
	t.Helper()
	nodeConfig := newGeneratedByronPBFTTestNodeConfig(
		t, c.magic, 10, c.issuer, c.delegate, c.cert,
	)
	data := &nodeConfig.ByronGenesis().BlockVersionData
	data.MaxBlockSize = int(c.genesis.maxBlockSize)
	data.MaxHeaderSize = int(c.genesis.maxHeaderSize)
	if blockLimit != 0 {
		data.MaxBlockSize = blockLimit
		data.MaxHeaderSize = blockLimit
	}
	data.MaxTxSize = 100
	data.MaxProposalSize = 4_000
	data.TxFeePolicy.Summand = int64(c.genesis.feeSummand) * 1_000_000_000
	data.TxFeePolicy.Multiplier = c.genesis.feeMultiplierNano
	return nodeConfig
}

// newLedger returns a ledger with no cached Byron state, as after a restart,
// over a chain holding raw.
func (c *byronAdoptionChain) newLedger(
	t *testing.T,
	nodeConfig *cardano.CardanoNodeConfig,
	raw []chain.RawBlock,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	cm, err := chain.NewManager(newTestDB(t), nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 10}))
	primary := cm.PrimaryChain()
	require.NoError(t, primary.AddRawBlocks(raw))
	return &LedgerState{
		chain:  primary,
		config: LedgerStateConfig{CardanoNodeConfig: nodeConfig},
	}, primary
}

func rawTip(raw chain.RawBlock) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(raw.Slot, raw.Hash),
		BlockNumber: raw.BlockNumber,
	}
}

// requireByronParams asserts the adopted limits and fee policy of params.
func requireByronParams(
	t *testing.T,
	params *eras.ByronProtocolParameters,
	want wantByronParams,
	msg string,
) {
	t.Helper()
	require.NotNil(t, params, msg)
	require.False(t, params.AdoptionUnknown, msg)
	require.Zero(
		t,
		params.MaxBlockSize.Cmp(new(big.Int).SetUint64(want.maxBlockSize)),
		"%s: maxBlockSize %s",
		msg,
		params.MaxBlockSize,
	)
	require.Zero(
		t,
		params.MaxHeaderSize.Cmp(new(big.Int).SetUint64(want.maxHeaderSize)),
		"%s: maxHeaderSize %s",
		msg,
		params.MaxHeaderSize,
	)
	require.Equal(
		t,
		want.feeSummand,
		params.TxFeeSummand,
		"%s: fee summand",
		msg,
	)
	require.Zero(
		t,
		params.TxFeeMultiplierNano.Cmp(big.NewInt(want.feeMultiplierNano)),
		"%s: fee multiplier %s",
		msg,
		params.TxFeeMultiplierNano,
	)
}

// stateAt rebuilds the update state at raw[index] and returns the parameters
// block application would validate the block at index with.
func stateAt(
	t *testing.T,
	ls *LedgerState,
	raw chain.RawBlock,
) (byronPBFTState, *eras.ByronProtocolParameters) {
	t.Helper()
	state, err := ls.byronPBFTStateAtTip(context.Background(), rawTip(raw))
	require.NoError(t, err)
	decoded, err := byron.NewByronMainBlockFromCbor(raw.Cbor)
	require.NoError(t, err)
	params, ok := byronBlockPParams(decoded, state, nil).(*eras.ByronProtocolParameters)
	require.True(t, ok)
	return state, params
}

// TestByronAdoptedParamsRestoredFromStoredChain covers #4418 and #4419 for a
// node that synced from genesis: a ledger with no cached state, as after a
// restart, replays the stored chain to the adopted limits and fee policy at
// every tip, and each of two successive adoptions replaces the previous one.
func TestByronAdoptedParamsRestoredFromStoredChain(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	for _, test := range []struct {
		name string
		tip  int
		want wantByronParams
	}{
		{"genesis before any adoption", 0, c.genesis},
		{"proposal registered but not adopted", 2, c.genesis},
		{"last block before update A", blockGenesisOnlyLast, c.genesis},
		{"first block under update A", blockFirstUnderA, c.adoptedA},
		{"update B registered, A still adopted", blockLastUnderA, c.adoptedA},
		{"first block under update B", blockFirstUnderB, c.adoptedB},
		{"tip under update B", blockTip, c.adoptedB},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ls, _ := c.newLedger(t, nodeConfig, c.raw)
			state, params := stateAt(t, ls, c.raw[test.tip])
			require.True(t, state.update.Complete())
			requireByronParams(t, params, test.want, test.name)
		})
	}

	// After a restart at the tip the restored limits govern the next block.
	ls, _ := c.newLedger(t, nodeConfig, c.raw)
	_, params := stateAt(t, ls, c.raw[blockTip])
	err := validateByronBlockSizes(c.probe, params, nodeConfig)
	require.ErrorContains(t, err, "exceeds maxHeaderSize")
	fee, err := params.MinFee(200)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(124_000), fee)
}

// TestByronAdoptedParamsRollbackAcrossAdoptions covers #4418 and #4419: a
// rollback to before an adoption restores the parameters adopted before it,
// whether the ledger cached the state at the abandoned tip or not, and
// replaying forward adopts them again.
func TestByronAdoptedParamsRollbackAcrossAdoptions(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	ls, primary := c.newLedger(t, nodeConfig, c.raw)

	cache := func(state byronPBFTState, raw chain.RawBlock) {
		ls.Lock()
		ls.byronPBFT = byronPBFTCache{
			state:       state,
			tip:         rawTip(raw).Point,
			initialized: true,
		}
		ls.Unlock()
	}
	state, params := stateAt(t, ls, c.raw[blockTip])
	requireByronParams(t, params, c.adoptedB, "tip")
	cache(state, c.raw[blockTip])

	// Roll back across update B's adoption.
	require.NoError(
		t,
		primary.RollbackUnbounded(rawTip(c.raw[blockLastUnderA]).Point),
	)
	state, params = stateAt(t, ls, c.raw[blockLastUnderA])
	requireByronParams(t, params, c.adoptedA, "rolled back across B")
	cache(state, c.raw[blockLastUnderA])

	// Roll back across update A's adoption as well.
	require.NoError(
		t,
		primary.RollbackUnbounded(rawTip(c.raw[blockGenesisOnlyLast]).Point),
	)
	state, params = stateAt(t, ls, c.raw[blockGenesisOnlyLast])
	requireByronParams(t, params, c.genesis, "rolled back across A")
	cache(state, c.raw[blockGenesisOnlyLast])

	// Replaying forward from the cached ancestor adopts both again.
	require.NoError(t, primary.AddRawBlocks(c.raw[blockGenesisOnlyLast+1:]))
	_, params = stateAt(t, ls, c.raw[blockTip])
	requireByronParams(t, params, c.adoptedB, "replayed forward")
}

// TestByronAdoptedParamsForkDoesNotInheritAbandonedAdoption covers #4418 and
// #4419: when the chain switches to a fork that never endorsed update A, a
// cached state from the abandoned fork's adoption is not reused, though the
// new tip is later than the cached one.
func TestByronAdoptedParamsForkDoesNotInheritAbandonedAdoption(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	nodeConfig := c.nodeConfig(t, 0)
	ls, primary := c.newLedger(t, nodeConfig, c.raw[:blockFirstUnderA+1])
	state, params := stateAt(t, ls, c.raw[blockFirstUnderA])
	requireByronParams(t, params, c.adoptedA, "abandoned fork")
	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         rawTip(c.raw[blockFirstUnderA]).Point,
		initialized: true,
	}
	ls.Unlock()

	// The fork keeps the confirmed proposal but never endorses it, and its
	// first epoch-1 block is one slot later than the abandoned one.
	require.NoError(t, primary.RollbackUnbounded(rawTip(c.raw[2]).Point))
	template := loadRealByronMainBlock(t)
	unendorsed := newSignedByronPBFTBlockWithBody(
		t, template, c.magic, 0, 30, 3, c.blocks[2].Hash(),
		c.issuer, c.delegate, c.cert, nil,
		&byronPBFTBodyOverride{
			emptyTransactions: true,
			updatePayload:     byronUpdatePayload(nil),
			blockVersion:      &byron.ByronBlockVersion{},
		},
	)
	nextEpoch := newSignedByronPBFTBlockWithBody(
		t, template, c.magic, 1, 1, 4, unendorsed.Hash(),
		c.issuer, c.delegate, c.cert, nil,
		&byronPBFTBodyOverride{
			emptyTransactions: true,
			updatePayload:     byronUpdatePayload(nil),
			blockVersion:      &byron.ByronBlockVersion{},
		},
	)
	forkRaw := []chain.RawBlock{
		rawByronPBFTBlock(t, unendorsed), rawByronPBFTBlock(t, nextEpoch),
	}
	require.NoError(t, primary.AddRawBlocks(forkRaw))
	_, params = stateAt(t, ls, forkRaw[1])
	requireByronParams(t, params, c.genesis, "fork without an adoption")
}

// TestByronTrustedMidByronStartHasUnknownAdoption covers the contract for a
// node whose stored chain starts inside Byron: the updates adopted before its
// first block are unknowable, so the state is marked incomplete and the size
// and fee limits, both those of block application and those of the tip the
// mempool validates against, are not enforced from genesis values.
func TestByronTrustedMidByronStartHasUnknownAdoption(t *testing.T) {
	t.Parallel()
	c := newByronAdoptionChain(t)
	// A genesis that would reject the probe block on either limit.
	nodeConfig := c.nodeConfig(t, 1)
	ls, _ := c.newLedger(t, nodeConfig, c.raw[blockFirstUnderA:])

	state, params := stateAt(t, ls, c.raw[blockTip])
	require.False(t, state.update.Complete())
	require.True(t, params.AdoptionUnknown)
	require.NoError(t, validateByronBlockSizes(c.probe, params, nodeConfig))

	// The same parameters, were they treated as known, reject the block.
	known := params.Clone()
	known.AdoptionUnknown = false
	require.ErrorContains(
		t,
		validateByronBlockSizes(c.probe, known, nodeConfig),
		"exceeds maxHeaderSize",
	)

	ls.Lock()
	ls.byronPBFT = byronPBFTCache{
		state:       state,
		tip:         rawTip(c.raw[blockTip]).Point,
		initialized: true,
	}
	ls.Unlock()
	atTip, err := ls.ByronProtocolParameters()
	require.NoError(t, err)
	require.True(t, atTip.AdoptionUnknown)

	// A chain that starts at genesis enforces the same limits.
	full, _ := c.newLedger(t, nodeConfig, c.raw[:1])
	fullState, fullParams := stateAt(t, full, c.raw[0])
	require.True(t, fullState.update.Complete())
	require.False(t, fullParams.AdoptionUnknown)
	require.ErrorContains(
		t,
		validateByronBlockSizes(c.probe, fullParams, nodeConfig),
		"exceeds maxHeaderSize",
	)
}
