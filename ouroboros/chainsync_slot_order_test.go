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

package ouroboros

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// babbageTestHeader is a post-Byron header at slot extending parentHash.
func babbageTestHeader(
	slot, blockNumber uint64,
	hashByte byte,
	parentHash gledger.Blake2b256,
) gledger.BlockHeader {
	header := newTestBlockHeader(slot, blockNumber, hashByte).(*testBlockHeader)
	header.prevHash = parentHash
	return nonByronTestHeader{header}
}

func headerPoint(header gledger.BlockHeader) ocommon.Point {
	return ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
}

func (f *checkpointRollForwardFixture) rollBackward(point ocommon.Point) {
	// The fixture has no ledger to roll back; only the session's record of
	// the rollback point matters here.
	_ = f.o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: f.connId},
		point,
		ochainsync.Tip{Point: point},
	)
}

func (f *checkpointRollForwardFixture) requireSlotOrderRejection(
	t *testing.T,
	err error,
	slot uint64,
) {
	t.Helper()
	var slotErr chain.SlotNotAfterParentError
	require.ErrorAs(t, err, &slotErr)
	require.Equal(t, slot, slotErr.Slot())
	testutil.RequireReceive(
		t, f.recycleCh, time.Second,
		"a header not after its parent's slot must recycle the connection",
	)
}

// TestChainsyncRollForwardChecksSlotOrderAfterRollback covers the header that
// follows a rollback. Chain selection keeps a peer's candidate length across a
// rollback to a header the peer delivered, the current one included, so a
// header whose slot does not follow the rollback point would extend that
// candidate. A peer could otherwise roll back to the point it stands at before
// every header and evade the check.
func TestChainsyncRollForwardChecksSlotOrderAfterRollback(t *testing.T) {
	t.Parallel()

	var origin gledger.Blake2b256
	parent := babbageTestHeader(100, 7, 0xaa, origin)
	cases := []struct {
		name string
		// deliver is sent before the rollback to parent.
		deliver []gledger.BlockHeader
		slot    uint64
		reject  bool
	}{
		{
			name:    "rollback to the current header, same slot",
			deliver: []gledger.BlockHeader{parent},
			slot:    100,
			reject:  true,
		},
		{
			name: "rollback to an earlier header, earlier slot",
			deliver: []gledger.BlockHeader{
				parent,
				babbageTestHeader(110, 8, 0xbb, parent.Hash()),
			},
			slot:   99,
			reject: true,
		},
		{
			name: "rollback to an earlier header, later slot",
			deliver: []gledger.BlockHeader{
				parent,
				babbageTestHeader(110, 8, 0xbb, parent.Hash()),
			},
			slot: 101,
		},
		{
			name:   "rollback to an undelivered point, same slot",
			slot:   100,
			reject: true,
		},
		{
			name: "rollback to an undelivered point, later slot",
			slot: 101,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newCheckpointRollForwardFixture(t, nil)
			for _, header := range tc.deliver {
				require.ErrorIs(
					t, f.rollForward(header, 0), errChainsyncCandidateRejected,
				)
			}
			f.rollBackward(headerPoint(parent))
			err := f.rollForward(
				babbageTestHeader(tc.slot, 8, 0xcc, parent.Hash()),
				0,
			)
			if tc.reject {
				f.requireSlotOrderRejection(t, err, tc.slot)
				return
			}
			require.ErrorIs(t, err, errChainsyncCandidateRejected)
			testutil.RequireNoReceive(
				t, f.recycleCh, 50*time.Millisecond,
				"an ordered header must not recycle the connection",
			)
		})
	}
}

// TestChainsyncRollForwardAcceptsByronMainBlockAtBoundarySlot is the honest
// control for the Byron exception at the session check: the first main block
// of an epoch shares the slot of the epoch boundary block before it, whether
// that boundary block was just delivered, is where a rollback returned to, or
// is a rollback point whose kind the session does not know.
func TestChainsyncRollForwardAcceptsByronMainBlockAtBoundarySlot(t *testing.T) {
	t.Parallel()

	ebb := &byron.ByronEpochBoundaryBlockHeader{}
	ebb.ConsensusData.Epoch = 1
	ebb.ConsensusData.Difficulty.Value = 7
	main := &byron.ByronMainBlockHeader{PrevBlock: ebb.Hash()}
	main.ConsensusData.SlotId.Epoch = 1
	main.ConsensusData.Difficulty.Value = 8
	require.Equal(t, ebb.SlotNumber(), main.SlotNumber())

	for _, name := range []string{"delivered", "rolled back to", "unknown"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newCheckpointRollForwardFixture(t, nil)
			if name != "unknown" {
				require.ErrorIs(
					t,
					f.rollForward(ebb, gledger.BlockTypeByronEbb),
					errChainsyncCandidateRejected,
				)
			}
			if name != "delivered" {
				f.rollBackward(headerPoint(ebb))
			}
			require.ErrorIs(
				t,
				f.rollForward(main, gledger.BlockTypeByronMain),
				errChainsyncCandidateRejected,
			)
			testutil.RequireNoReceive(
				t, f.recycleCh, 50*time.Millisecond,
				"a main block at its boundary parent's slot must not recycle",
			)
		})
	}
}

// TestChainsyncRollForwardRejectsByronMainBlockReusingSlotAfterRollback is
// the negative half: a rollback to a main block the session delivered keeps
// its kind, so the next main block may not share its slot. A rollback point of
// unknown kind could be a boundary block and would allow it.
func TestChainsyncRollForwardRejectsByronMainBlockReusingSlotAfterRollback(
	t *testing.T,
) {
	t.Parallel()

	f := newCheckpointRollForwardFixture(t, nil)
	// newTestBlockHeader reports the Byron era and is not a boundary block.
	first := newTestBlockHeader(100, 7, 0xaa)
	require.ErrorIs(t, f.rollForward(first, 0), errChainsyncCandidateRejected)
	require.ErrorIs(
		t,
		f.rollForward(linkedTestBlockHeader(first, 110, 0xbb), 0),
		errChainsyncCandidateRejected,
	)
	f.rollBackward(headerPoint(first))
	f.requireSlotOrderRejection(
		t,
		f.rollForward(linkedTestBlockHeader(first, 100, 0xcc), 0),
		100,
	)
}

func linkedTestBlockHeader(
	parent gledger.BlockHeader,
	slot uint64,
	hashByte byte,
) gledger.BlockHeader {
	header := newTestBlockHeader(slot, parent.BlockNumber()+1, hashByte).(*testBlockHeader)
	header.prevHash = parent.Hash()
	return header
}

// TestChainsyncRollForwardRejectsHeaderNotAfterParentSlot covers a session
// that delivers a header continuing its previous one without a later slot.
// Chain selection ranks candidates by the block numbers their delivered
// headers claim, so the header must be refused before it reaches selection.
func TestChainsyncRollForwardRejectsHeaderNotAfterParentSlot(t *testing.T) {
	t.Parallel()

	for _, slot := range []uint64{99, 100} {
		f := newCheckpointRollForwardFixture(t, nil)
		parent := newTestBlockHeader(100, 7, 0xaa)
		require.ErrorIs(
			t,
			f.rollForward(parent, 0),
			errChainsyncCandidateRejected,
		)

		err := f.rollForward(linkedTestBlockHeader(parent, slot, 0xbb), 0)

		var slotErr chain.SlotNotAfterParentError
		require.ErrorAs(t, err, &slotErr)
		require.Equal(t, slot, slotErr.Slot())
		evt := testutil.RequireReceive(
			t, f.recycleCh, time.Second,
			"a header not after its parent's slot must recycle the connection",
		)
		recycle := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
		require.Equal(t, f.connId, recycle.ConnectionId)
		require.Equal(t, "header_verification_failure", recycle.Reason)
		testutil.RequireNoReceive(
			t, f.observedCh, 50*time.Millisecond,
			"the header must not reach chain selection",
		)
	}
}

// TestChainsyncRollForwardAcceptsHeaderAfterParentSlot is the honest control.
func TestChainsyncRollForwardAcceptsHeaderAfterParentSlot(t *testing.T) {
	t.Parallel()

	f := newCheckpointRollForwardFixture(t, nil)
	parent := newTestBlockHeader(100, 7, 0xaa)
	require.ErrorIs(t, f.rollForward(parent, 0), errChainsyncCandidateRejected)
	require.ErrorIs(
		t,
		f.rollForward(linkedTestBlockHeader(parent, 101, 0xbb), 0),
		errChainsyncCandidateRejected,
	)
	testutil.RequireNoReceive(
		t, f.recycleCh, 50*time.Millisecond,
		"an ordered header must not recycle the connection",
	)
}
