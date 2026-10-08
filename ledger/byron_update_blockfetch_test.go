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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/byronupdate"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// byronUpdateSigned prefixes body with the signing tag and protocol magic, as
// the Byron update system signs its proposals and votes.
func byronUpdateSigned(
	t *testing.T,
	tag byte,
	protocolMagic uint32,
	body []byte,
) []byte {
	t.Helper()
	magic, err := cbor.Encode(protocolMagic)
	require.NoError(t, err)
	return append(append([]byte{tag}, magic...), body...)
}

// newByronSoftwareUpdateProposal returns a signed proposal that changes only
// the software version, so it registers without protocol parameter changes,
// and the proposal's id.
func newByronSoftwareUpdateProposal(
	t *testing.T,
	protocolMagic uint32,
	proposer byronPBFTTestKey,
) ([]byte, []byte) {
	t.Helper()
	return newByronUpdateProposal(
		t, protocolMagic, proposer, [3]uint64{}, nil, nil,
	)
}

// newByronUpdateProposal returns a signed proposal for the protocol version
// that sets the block and header size limits it is given, and the proposal's
// id. A nil limit leaves that parameter unchanged.
func newByronUpdateProposal(
	t *testing.T,
	protocolMagic uint32,
	proposer byronPBFTTestKey,
	version [3]uint64,
	maxBlockSize *uint64,
	maxHeaderSize *uint64,
) ([]byte, []byte) {
	t.Helper()
	optional := func(value *uint64) []any {
		if value == nil {
			return []any{}
		}
		return []any{*value}
	}
	fields := make([]cbor.RawMessage, 0, 5)
	for _, value := range []any{
		[]any{uint16(version[0]), uint16(version[1]), uint8(version[2])},
		[]any{
			[]any{}, []any{}, optional(maxBlockSize), optional(maxHeaderSize),
			[]any{}, []any{}, []any{}, []any{}, []any{}, []any{}, []any{},
			[]any{}, []any{}, []any{},
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
		byronUpdateSigned(t, byron.SignTagUSProposal, protocolMagic, signedBody),
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

// newByronUpdateVote returns a vote for the proposal id. A corrupt vote has
// its signature altered and is otherwise well formed.
func newByronUpdateVote(
	t *testing.T,
	protocolMagic uint32,
	voter byronPBFTTestKey,
	proposalId []byte,
	corrupt bool,
) []byte {
	t.Helper()
	idCbor, err := cbor.Encode(proposalId)
	require.NoError(t, err)
	inner := append(append([]byte{0x82}, idCbor...), 0xf5)
	signature := ed25519.Sign(
		voter.privateKey,
		byronUpdateSigned(t, byron.SignTagUSVote, protocolMagic, inner),
	)
	if corrupt {
		signature[0] ^= 0xff
	}
	raw, err := cbor.Encode([]any{
		voter.verificationKey, cbor.RawMessage(idCbor), true, signature,
	})
	require.NoError(t, err)
	return raw
}

// byronUpdatePayload encodes an update payload of at most one proposal and
// any votes.
func byronUpdatePayload(proposal []byte, votes ...[]byte) []byte {
	payload := []byte{0x82}
	if proposal == nil {
		payload = append(payload, 0x80)
	} else {
		payload = append(payload, 0x81)
		payload = append(payload, proposal...)
	}
	payload = append(payload, 0x9f)
	for _, vote := range votes {
		payload = append(payload, vote...)
	}
	return append(payload, 0xff)
}

// byronUpdateBlockfetchChain is a Byron chain, k = 10, whose only stored block
// is an epoch boundary parent, with the update state a from-genesis replay
// leaves and a ledger that receives blocks through the blockfetch handler.
type byronUpdateBlockfetchChain struct {
	ls       *LedgerState
	connId   ouroboros.ConnectionId
	errors   chan LedgerErrorEvent
	proposer byronPBFTTestKey
	magic    uint32
	parent   chain.RawBlock
}

func newByronUpdateBlockfetchChain(t *testing.T) *byronUpdateBlockfetchChain {
	t.Helper()
	const (
		protocolMagic = uint32(44)
		securityParam = 10
	)
	issuer := newByronPBFTTestKey(0x91)
	delegate := newByronPBFTTestKey(0x92)
	genesisCertificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	nodeConfig := newGeneratedByronPBFTTestNodeConfig(
		t, protocolMagic, securityParam, issuer, delegate, genesisCertificate,
	)
	limits := &nodeConfig.ByronGenesis().BlockVersionData
	limits.MaxBlockSize = 2_000_000
	limits.MaxHeaderSize = 2_000_000
	limits.MaxTxSize = 4_096
	limits.MaxProposalSize = 700

	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{
		securityParam: securityParam,
	}))
	parent := chain.RawBlock{
		Slot:        5,
		Hash:        testHashBytes("byron-update-parent"),
		BlockNumber: 0,
		Type:        uint(gledger.BlockTypeByronEbb),
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().
			AddRawBlocks(context.Background(), []chain.RawBlock{parent}),
	)
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.ByronEraDesc.Id, 20_000, 100, nil,
	))
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:           db,
		ChainManager:       cm,
		CardanoNodeConfig:  nodeConfig,
		EventBus:           bus,
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ValidateHistorical: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	ls.slotClock = NewSlotClock(
		newMockSlotTimeProvider(time.Unix(0, 0), time.Second, 100),
		DefaultSlotClockConfig(),
	)
	parentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(parent.Slot, parent.Hash),
		BlockNumber: parent.BlockNumber,
	}
	require.NoError(t, db.SetTip(parentTip, nil))
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		SlotLength:    20_000,
		LengthInSlots: 100,
		EraId:         eras.ByronEraDesc.Id,
	}
	ls.Lock()
	ls.currentTip = parentTip
	ls.currentEra = eras.ByronEraDesc
	ls.currentEpoch = epoch
	ls.epochCache = []models.Epoch{epoch}
	ls.currentPParams = nil
	ls.Unlock()

	config, err := ls.byronPBFTConfig()
	require.NoError(t, err)
	genesisParams, err := ls.byronGenesisProtocolParameters()
	require.NoError(t, err)
	state, err := newByronPBFTState(config, genesisParams)
	require.NoError(t, err)
	// The chain's first block is stored without its body, so seed the update
	// state a replay from block 0 at slot 0 would have produced.
	state.update = state.update.Advance(0, 0)
	ls.Lock()
	ls.byronPBFT.state = state
	ls.byronPBFT.tip = parentTip.Point
	ls.byronPBFT.initialized = true
	ls.publishSnapshotsLocked()
	ls.Unlock()

	errs := make(chan LedgerErrorEvent, 8)
	bus.SubscribeFunc(LedgerErrorEventType, func(evt event.Event) {
		if e, ok := evt.Data.(LedgerErrorEvent); ok {
			errs <- e
		}
	})
	connId := ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = connId
	return &byronUpdateBlockfetchChain{
		ls:       ls,
		connId:   connId,
		errors:   errs,
		proposer: delegate,
		magic:    protocolMagic,
		parent:   parent,
	}
}

// signedBlock returns a Byron main block in epoch 0 whose header carries a
// correct body proof over updatePayload and a valid PBFT signature.
func (c *byronUpdateBlockfetchChain) signedBlock(
	t *testing.T,
	slot uint64,
	blockNumber uint64,
	prevHash lcommon.Blake2b256,
	updatePayload []byte,
) *byron.ByronMainBlock {
	t.Helper()
	issuer := newByronPBFTTestKey(0x91)
	certificate := newSignedByronPBFTDelegationCertificate(
		t, c.magic, 0, issuer, c.proposer,
	)
	block := newSignedByronPBFTBlockWithBody(
		t,
		loadRealByronMainBlock(t),
		c.magic,
		0,
		slot,
		blockNumber,
		prevHash,
		issuer,
		c.proposer,
		certificate,
		nil,
		&byronPBFTBodyOverride{
			emptyTransactions: true,
			updatePayload:     updatePayload,
		},
	)
	require.NoError(t, block.ValidateBodyProof())
	return block
}

// deliver hands blocks to the blockfetch handler in order and flushes what
// the handler still holds onto the chain. The handler flushes earlier blocks
// itself while it has no cached epoch nonce for a slot.
func (c *byronUpdateBlockfetchChain) deliver(
	t *testing.T,
	blocks ...*byron.ByronMainBlock,
) {
	t.Helper()
	for _, block := range blocks {
		c.ls.handleEventBlockfetch(event.NewEvent(
			BlockfetchEventType,
			BlockfetchEvent{
				ConnectionId: c.connId,
				Point: ocommon.NewPoint(
					block.SlotNumber(),
					block.Hash().Bytes(),
				),
				Type:  uint(gledger.BlockTypeByronMain),
				Block: block,
			},
		))
	}
	select {
	case e := <-c.errors:
		t.Fatalf("blockfetch rejected a block: %v", e.Error)
	default:
	}
	c.ls.chainsyncBlockfetchMutex.Lock()
	err := c.ls.flushPendingBlockfetchBlocksDeferred(nil)
	c.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)
	require.Equal(t, blocks[len(blocks)-1].Hash().Bytes(),
		c.ls.chain.Tip().Point.Hash,
		"blockfetch must have put every block on the chain")
}

// startApply runs one attempt of the ledger's chain reader and block pipeline
// over the blocks blockfetch stored and returns the channel it reports on.
// Cancelling the returned function stops an attempt that has caught up.
func (c *byronUpdateBlockfetchChain) startApply(
	t *testing.T,
) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	done := make(chan error, 1)
	go func() {
		done <- c.ls.runLedgerReadChainAttempt(
			ctx,
			c.ls.ledgerReadChain,
			c.ls.ledgerProcessBlocksFromSource,
		)
	}()
	return done, cancel
}

// TestByronBlockfetchRejectsBodyProofedBlockWithInvalidUpdateVote covers
// update-vote validation end to end: blocks reach the ledger through the
// blockfetch handler, whose header checks pass for a re-signed header carrying
// a correct upd_proof, and the block whose update payload votes with a bad
// signature for a registered proposal is refused when the chain is applied.
func TestByronBlockfetchRejectsBodyProofedBlockWithInvalidUpdateVote(
	t *testing.T,
) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		corruptVote bool
	}{
		{"valid vote is applied", false},
		{"invalid vote signature is rejected", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := newByronUpdateBlockfetchChain(t)
			proposal, proposalId := newByronSoftwareUpdateProposal(
				t, c.magic, c.proposer,
			)
			registering := c.signedBlock(
				t, 6, 1,
				lcommon.NewBlake2b256(c.parent.Hash),
				byronUpdatePayload(proposal),
			)
			voting := c.signedBlock(
				t, 7, 2, registering.Hash(),
				byronUpdatePayload(nil, newByronUpdateVote(
					t, c.magic, c.proposer, proposalId, test.corruptVote,
				)),
			)
			c.deliver(t, registering, voting)

			done, cancel := c.startApply(t)
			defer cancel()
			if !test.corruptVote {
				testutil.WaitForCondition(t, func() bool {
					return string(c.ls.Tip().Point.Hash) ==
						string(voting.Hash().Bytes())
				}, 20*time.Second, "ledger tip must reach the voting block")
				cancel()
				<-done
				return
			}
			var err error
			finished := false
			testutil.WaitForCondition(t, func() bool {
				select {
				case err = <-done:
					finished = true
					return true
				default:
				}
				return string(c.ls.Tip().Point.Hash) ==
					string(voting.Hash().Bytes())
			}, 20*time.Second, "the ledger must reject or apply the block")
			require.True(
				t, finished,
				"the ledger applied a block whose update vote has an "+
					"invalid signature",
			)
			// A header-stage rejection drops the block from the primary chain,
			// rolls the ledger back to its last good tip and restarts the
			// pipeline; the verdict is what recovery recorded.
			require.ErrorIs(t, err, errRestartLedgerPipeline)
			failure := c.ls.lastHeaderValidationFailure
			require.NotNil(t, failure, "the block must be rejected, not skipped")
			require.Equal(t, voting.Hash().Bytes(), failure.BlockPoint.Hash)
			var invalidSignature byronupdate.VoteInvalidSignatureError
			require.ErrorAs(t, failure.Cause, &invalidSignature)
			require.Equal(
				t, c.parent.Hash, c.ls.chain.Tip().Point.Hash,
				"the rejected block must be dropped from the primary chain",
			)
			require.Equal(t, c.parent.Hash, c.ls.Tip().Point.Hash)
		})
	}
}
