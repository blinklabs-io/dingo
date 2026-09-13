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

package chain_test

import (
	"bytes"
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/chain"
)

// originContinuityCase describes one candidate offered as the first block of a
// chain that a rollback has emptied back to origin.
type originContinuityCase struct {
	name        string
	blockNumber uint64
	prevHash    string
	wantReject  bool
}

// originContinuityCases covers both directions: a chain emptied back to origin
// must still accept the network's genuine first block, and must reject a block
// from further along the chain -- which is what a peer whose chainsync cursor
// survived the rollback offers next (issue #4202).
//
// The chain package does not know the network's genesis hash, so the anchor
// available at origin is the block number. The first block of a Cardano chain
// carries block number 0 (Ouroboros numbers the first block after genesis 0 --
// the Byron epoch-boundary block on a Byron network, the first block of the
// starting era on a post-Byron genesis network); 1 is tolerated because some
// networks and chain indexes number their first block 1.
var originContinuityCases = []originContinuityCase{
	{
		name:        "genuine first block number 0 accepted",
		blockNumber: 0,
		prevHash:    "",
		wantReject:  false,
	},
	{
		name:        "genuine first block number 1 accepted",
		blockNumber: 1,
		prevHash:    "",
		wantReject:  false,
	},
	{
		name:        "block from further along the chain rejected",
		blockNumber: 160,
		prevHash:    testHashPrefix + "00a0",
		wantReject:  true,
	},
	{
		name:        "second block of the chain rejected",
		blockNumber: 2,
		prevHash:    testHashPrefix + "0001",
		wantReject:  true,
	},
}

func originCandidate(c originContinuityCase) *MockBlock {
	return &MockBlock{
		MockBlockNumber: c.blockNumber,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
		MockPrevHash:    c.prevHash,
	}
}

func originCandidateRawBlock(c originContinuityCase) chain.RawBlock {
	candidate := originCandidate(c)
	return chain.RawBlock{
		Slot:        candidate.MockSlot,
		Hash:        candidate.Hash().Bytes(),
		BlockNumber: candidate.MockBlockNumber,
		Type:        uint(candidate.Type()),
		PrevHash:    decodeHex(candidate.MockPrevHash),
		Cbor:        []byte{0x80},
	}
}

// chainEmptiedToOrigin returns a chain that held blocks and was then rolled
// back to origin -- the state that makes the continuity checks inapplicable
// and that a peer with a stale chainsync cursor rolls forward into.
func chainEmptiedToOrigin(t *testing.T) *chain.Chain {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 10)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	if err := c.Rollback(ocommon.NewPointOrigin()); err != nil {
		t.Fatalf("unexpected error rolling back to origin: %s", err)
	}
	if tip := c.Tip(); len(tip.Point.Hash) != 0 {
		t.Fatalf(
			"expected an empty chain after rollback to origin, got tip %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
		)
	}
	if c.HeaderCount() != 0 {
		t.Fatalf(
			"expected no queued headers after rollback to origin, got %d",
			c.HeaderCount(),
		)
	}
	return c
}

func assertOriginResult(
	t *testing.T,
	tc originContinuityCase,
	op string,
	err error,
) {
	t.Helper()
	if !tc.wantReject {
		if err != nil {
			t.Fatalf(
				"%s: must accept the network's first block (number %d): %s",
				op,
				tc.blockNumber,
				err,
			)
		}
		return
	}
	if err == nil {
		t.Fatalf(
			"%s: accepted block number %d as the chain's first block; "+
				"the chain would grow with a missing prefix",
			op,
			tc.blockNumber,
		)
	}
	var notFitErr chain.BlockNotFitChainTipError
	if !errors.As(err, &notFitErr) {
		t.Fatalf(
			"%s: expected BlockNotFitChainTipError, got %T: %s",
			op,
			err,
			err,
		)
	}
}

// TestAddBlockHeaderAfterRollbackToOriginRequiresFirstBlock is the reported
// defect: a rollback to origin empties the chain mid-run, and the next roll
// forward from a peer whose chainsync cursor is still ahead must not be
// accepted as the chain's first block (issue #4202).
func TestAddBlockHeaderAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddBlockHeader after rollback to origin",
				c.AddBlockHeader(originCandidate(tc)),
			)
		})
	}
}

// TestAddBlockAfterRollbackToOriginRequiresFirstBlock covers the block-apply
// path (Chain.AddBlock -> addBlockLocked), which skipped the same check.
func TestAddBlockAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddBlock after rollback to origin",
				c.AddBlock(originCandidate(tc), nil),
			)
		})
	}
}

// TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock covers the raw-block
// path (Chain.AddRawBlocks -> addRawBlockLocked), which skipped the same check.
func TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := chainEmptiedToOrigin(t)
			assertOriginResult(
				t,
				tc,
				"AddRawBlocks after rollback to origin",
				c.AddRawBlocks(
					[]chain.RawBlock{originCandidateRawBlock(tc)},
				),
			)
		})
	}
}

// TestFreshChainAcceptsFirstBlockFromAnySource pins the initial-sync side: a
// chain that has never been mutated has no anchor to check against -- it is
// the state the bulk block importer fills from a local immutable database --
// so it keeps accepting whatever it is given as its first block. Only a chain
// emptied back to origin is anchored.
func TestFreshChainAcceptsFirstBlockFromAnySource(t *testing.T) {
	t.Parallel()

	for _, tc := range originContinuityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			if err != nil {
				t.Fatalf("unexpected error creating chain manager: %s", err)
			}
			if err := cm.PrimaryChain().AddBlockHeader(
				originCandidate(tc),
			); err != nil {
				t.Fatalf(
					"fresh chain must accept its first header (number %d): %s",
					tc.blockNumber,
					err,
				)
			}
			if err := cm.PrimaryChain().AddRawBlocks(
				[]chain.RawBlock{originCandidateRawBlock(tc)},
			); err != nil {
				t.Fatalf(
					"fresh chain must accept its first raw block (number %d): %s",
					tc.blockNumber,
					err,
				)
			}
		})
	}
}

// queuedFirstHeader is the chain's genuine first header (block number 1), the
// one a chain emptied back to origin legitimately accepts into its queue.
func queuedFirstHeader() *MockBlock {
	return &MockBlock{
		MockBlockNumber: 1,
		MockSlot:        3360,
		MockHash:        testHashPrefix + "beef",
	}
}

// rawBlockForHeader builds a raw block carrying the queued header's hash and
// slot but the caller's block number and prev hash, so a test can offer a
// block that matches the header by hash yet belongs further along the chain.
func rawBlockForHeader(
	header *MockBlock,
	blockNumber uint64,
	prevHash string,
) chain.RawBlock {
	return chain.RawBlock{
		Slot:        header.MockSlot,
		Hash:        header.Hash().Bytes(),
		BlockNumber: blockNumber,
		Type:        uint(header.Type()),
		PrevHash:    decodeHex(prevHash),
		Cbor:        []byte{0x80},
	}
}

func assertStillAtOriginWithQueuedHeader(t *testing.T, c *chain.Chain) {
	t.Helper()
	if tip := c.Tip(); len(tip.Point.Hash) != 0 {
		t.Fatalf(
			"chain advanced past origin on a rejected block: tip %d.%x",
			tip.Point.Slot,
			tip.Point.Hash,
		)
	}
	if got := c.HeaderCount(); got != 1 {
		t.Fatalf(
			"rejected block consumed the queued header: header count %d, want 1",
			got,
		)
	}
}

// TestAddRawBlocksAfterRollbackToOriginWithQueuedHeader pins the origin check
// to the chain's exported API rather than to the shape of the header queue.
// A queued header binds the next block's hash, not its block number, so the
// sequence rollback-to-origin -> AddBlockHeader(first header) ->
// AddRawBlocks(same hash, block number 2) must still be rejected: accepting it
// would delete the queued header and persist block number 2 as the chain's
// first block, leaving the missing prefix of issue #4202. The variant with no
// header queued is covered by
// TestAddRawBlockAfterRollbackToOriginRequiresFirstBlock.
func TestAddRawBlocksAfterRollbackToOriginWithQueuedHeader(t *testing.T) {
	t.Parallel()

	c := chainEmptiedToOrigin(t)
	header := queuedFirstHeader()
	if err := c.AddBlockHeader(header); err != nil {
		t.Fatalf("chain must accept its first header after rollback: %s", err)
	}
	if got := c.HeaderCount(); got != 1 {
		t.Fatalf("expected 1 queued header, got %d", got)
	}
	// Same hash as the queued header, but a block number from further along
	// the chain.
	err := c.AddRawBlocks(
		[]chain.RawBlock{
			rawBlockForHeader(header, 2, testHashPrefix+"0001"),
		},
	)
	if err == nil {
		t.Fatal(
			"AddRawBlocks accepted block number 2 as the chain's first block " +
				"because a header was queued; the chain would grow with a " +
				"missing prefix",
		)
	}
	var notFitErr chain.BlockNotFitChainTipError
	if !errors.As(err, &notFitErr) {
		t.Fatalf("expected BlockNotFitChainTipError, got %T: %s", err, err)
	}
	assertStillAtOriginWithQueuedHeader(t, c)
	// The consistent raw block for that same header is still accepted and
	// clears the queue.
	if err := c.AddRawBlocks(
		[]chain.RawBlock{rawBlockForHeader(header, 1, "")},
	); err != nil {
		t.Fatalf(
			"chain must accept the raw block matching its queued first header: %s",
			err,
		)
	}
	if got := c.HeaderCount(); got != 0 {
		t.Fatalf("accepted block left %d queued headers, want 0", got)
	}
	tip := c.Tip()
	if !bytes.Equal(tip.Point.Hash, header.Hash().Bytes()) {
		t.Fatalf(
			"tip hash %x after accepted block, want %x",
			tip.Point.Hash,
			header.Hash().Bytes(),
		)
	}
	if tip.BlockNumber != 1 {
		t.Fatalf("tip block number %d after accepted block, want 1", tip.BlockNumber)
	}
}

// TestAddBlockAfterRollbackToOriginUsesQueuedHeaderBlockNumber pins why the
// decoded-block path needs no separate guard for the same sequence: when a
// header is queued, addBlockLocked takes the block number (and prev hash) from
// that header, which the origin check above already anchored at queue time. So
// even a block whose own body claims a mid-chain number enters the chain as
// the header's block number 1, not as a truncated prefix. RawBlock carries a
// caller-supplied BlockNumber that is not derived from the queued header,
// which is why AddRawBlocks is checked directly.
func TestAddBlockAfterRollbackToOriginUsesQueuedHeaderBlockNumber(t *testing.T) {
	t.Parallel()

	c := chainEmptiedToOrigin(t)
	header := queuedFirstHeader()
	if err := c.AddBlockHeader(header); err != nil {
		t.Fatalf("chain must accept its first header after rollback: %s", err)
	}
	midChainBlock := &MockBlock{
		MockBlockNumber: 2,
		MockSlot:        header.MockSlot,
		MockHash:        header.MockHash,
		MockPrevHash:    testHashPrefix + "0001",
	}
	if err := c.AddBlock(midChainBlock, nil); err != nil {
		t.Fatalf(
			"block matching the queued first header must be accepted: %s",
			err,
		)
	}
	if got := c.HeaderCount(); got != 0 {
		t.Fatalf("accepted block left %d queued headers, want 0", got)
	}
	if tip := c.Tip(); tip.BlockNumber != 1 {
		t.Fatalf(
			"chain recorded block number %d, want the queued header's 1",
			tip.BlockNumber,
		)
	}
}
