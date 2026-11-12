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
	"github.com/stretchr/testify/require"
)

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
