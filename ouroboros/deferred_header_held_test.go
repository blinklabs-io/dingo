package ouroboros

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestDeferredHeaderRecoveryKeepsPeerWhenChainAlreadyHoldsHeader is the relay
// reconnect storm. A faster peer's block H is already stored as the chain's
// block and header tip, and the ledger is mid-way through applying it, when a
// second peer delivers H's header. Verification defers on the transitioning
// ledger context, and H no longer extends a chain that already ends at H, so
// the header is withheld. The chain update for H was published when H was
// stored, before this header arrived, so no later update can resolve it: the
// idle path re-intersects the peer at once, and otherwise the next block does.
// The chain already holds the block, and the ledger validates it on the body
// path and re-syncs every peer itself if that fails, so the peer must be kept
// and its cursor aligned with H instead.
func TestDeferredHeaderRecoveryKeepsPeerWhenChainAlreadyHoldsHeader(
	t *testing.T,
) {
	t.Parallel()

	var parentHash, heldHash gledger.Blake2b256
	parentHash[0], heldHash[0] = 0x01, 0x02
	f := newDeferredHeaderFixture(t, heldHash)

	held := nonByronTestHeader{&testBlockHeader{
		hash:        heldHash,
		prevHash:    parentHash,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	f.rollForward(t, held)

	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"a deferred header the chain already holds must not reconnect its peer",
	)
	f.o.deferredHeaderRecoveryMu.Lock()
	_, flagged := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.False(t, flagged)
	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.Equal(
		t,
		ocommon.NewPoint(held.SlotNumber(), held.Hash().Bytes()),
		tracked.Cursor,
	)
}

// TestDeferredHeaderRecoveryStillFlagsHeaderTheChainLacks is the control: a
// withheld fork header whose block the chain does not hold still waits for
// recovery, as before.
func TestDeferredHeaderRecoveryStillFlagsHeaderTheChainLacks(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0] = 0x01, 0x03, 0x04
	f := newDeferredHeaderFixture(t, localTip)

	fork := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	f.rollForward(t, fork)

	f.o.deferredHeaderRecoveryMu.Lock()
	_, flagged := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, flagged)
}

// TestDeferredHeaderRecoveryHeldTipKeepsNewerWithheldHeader covers a late
// ledger rejection: a peer's newer fork header is withheld, then the ledger
// reports its older header, whose block the chain holds. Resolving the older
// header must not discard recovery for the newer one, which the chain lacks.
func TestDeferredHeaderRecoveryHeldTipKeepsNewerWithheldHeader(t *testing.T) {
	t.Parallel()

	var localTip, heldHash, forkHash gledger.Blake2b256
	localTip[0], heldHash[0], forkHash[0] = 0x01, 0x02, 0x05
	f := newDeferredHeaderFixture(t, localTip)

	fork := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    heldHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}
	f.rollForward(t, fork)
	f.o.deferredHeaderRecoveryMu.Lock()
	_, flagged := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, flagged)

	// The older header's block is now the chain's tip when its late
	// rejection arrives.
	f.o.chainsyncLocalChainTips = func() ([]byte, []byte) {
		return heldHash.Bytes(), heldHash.Bytes()
	}
	heldPoint := ocommon.NewPoint(86400, heldHash.Bytes())
	f.o.markDeferredHeaderRecovery(
		f.conn,
		f.o.ledgerState.Tip(),
		heldPoint,
		ochainsync.Tip{Point: heldPoint, BlockNumber: 10},
		true,
	)

	f.o.deferredHeaderRecoveryMu.Lock()
	state, stillFlagged := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(
		t,
		stillFlagged,
		"resolving an older held header must keep recovery for a newer withheld one",
	)
	require.Equal(t, uint64(86420), state.point.Slot)
}
